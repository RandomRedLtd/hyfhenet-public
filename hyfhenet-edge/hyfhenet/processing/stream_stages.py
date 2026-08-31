from __future__ import annotations

import json
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Empty, Queue
from typing import Any, Sequence

from ..ai.models import (
    EdgeModelRunner,
    MODEL_INPUT_CONTRACT_VERSION,
    MODEL_OUTPUT_CONTRACT_VERSION,
    build_default_edge_models,
    build_model_input,
)
from ..ai.event_gates import (
    EdgeEventGateRunner,
    build_default_event_gates,
)
from ..fhe.features import (
    FheCohortFeaturePreparer,
    FheForecastFeaturePreparer,
    FheNilmFeaturePreparer,
)
from .preprocessing import DataQualityValidator, RollingFeatureEngine
from ..ai.services import EdgeServiceRunner, build_default_edge_services
from ..core.interfaces import (
    PipelineContext,
    StreamingObserver,
    StreamingPipelineStage,
    StreamingSink,
)
from ..core.models import (
    CloudCohortFeatureRecord,
    CloudForecastFeatureRecord,
    CloudNilmFeatureRecord,
    EdgeForecastEvaluationRecord,
    ModelInferenceResult,
    RawTelemetryEvent,
    StreamingPipelineRuntime,
)
from ..fhe import FheInferenceRunner, build_default_fhe_inference_tasks
from ..ingestion.normalization import apply_normalized_event_to_state, build_snapshot, normalize_raw_row


class StreamingPreprocessingStage(StreamingPipelineStage):
    LATENCY_STAGE_NAME = "preprocessing"

    def __init__(self, validator: DataQualityValidator | None = None) -> None:
        self.validator = validator

    def _get_validator(self, context: PipelineContext) -> DataQualityValidator:
        if self.validator is None:
            self.validator = DataQualityValidator(context.config)
        return self.validator

    def on_event_group(
            self,
            events: list[RawTelemetryEvent],
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        validator = self._get_validator(context)
        observer.on_pipeline_step(
            "preprocessing",
            f"normalize_validate_update_state raw_events={len(events)}",
            context,
        )
        for raw_event in events:
            normalized = normalize_raw_row(raw_event, context.config["devices"])
            apply_normalized_event_to_state(normalized, runtime.state, runtime.last_update)
            runtime.devices[normalized.device_id] += 1
            runtime.normalized_event_count += 1
            sink.append_normalized_event(raw_event, normalized, context)
            observer.on_normalized_event(raw_event, normalized, context)
            for alert in validator.validate_event(normalized):
                sink.append_quality_alert(alert, context)
                observer.on_quality_alert(alert, context)
                runtime.quality_alert_count += 1

    def on_tick(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        validator = self._get_validator(context)
        observer.on_pipeline_step(
            "preprocessing",
            f"build_snapshot ts={tick_timestamp.isoformat()}",
            context,
        )
        snapshot = build_snapshot(
            tick_timestamp,
            runtime.tick_interval_seconds,
            runtime.state,
            runtime.last_update,
            context.config["devices"],
        )
        runtime.latest_snapshot = snapshot
        runtime.latest_feature_window = None
        runtime.latest_cloud_forecast_feature = None
        runtime.latest_cloud_nilm_feature = None
        runtime.latest_cloud_cohort_feature = None
        runtime.latest_cloud_forecast_training_examples = []
        runtime.latest_load_event_markers = []
        runtime.latest_service_results = []
        runtime.latest_model_input = None
        runtime.latest_model_results = []
        sink.append_snapshot(snapshot, context)
        observer.on_snapshot(snapshot, context)
        runtime.snapshot_count += 1
        for alert in validator.validate_snapshot(snapshot):
            sink.append_quality_alert(alert, context)
            observer.on_quality_alert(alert, context)
            runtime.quality_alert_count += 1


class StreamingFeatureStage(StreamingPipelineStage):
    LATENCY_STAGE_NAME = "feature_engineering"

    def __init__(self, feature_engine: RollingFeatureEngine | None = None) -> None:
        self.feature_engine = feature_engine

    def _get_feature_engine(self, context: PipelineContext) -> RollingFeatureEngine:
        if self.feature_engine is None:
            self.feature_engine = RollingFeatureEngine(context.config)
        return self.feature_engine

    def on_tick(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if runtime.latest_snapshot is None:
            return
        feature_engine = self._get_feature_engine(context)
        observer.on_pipeline_step(
            "feature_engineering",
            (
                f"build_feature_window ts={tick_timestamp.isoformat()} "
                f"windows={','.join(_feature_window_labels(context))}"
            ),
            context,
        )
        feature_window = feature_engine.build_feature_window(runtime.latest_snapshot)
        runtime.latest_feature_window = feature_window
        sink.append_feature_window(feature_window, context)
        observer.on_feature_window(feature_window, context)
        runtime.feature_window_count += 1


class StreamingEventGateStage(StreamingPipelineStage):
    LATENCY_STAGE_NAME = "event_gating"

    def __init__(self, event_gate_runner: EdgeEventGateRunner | None = None) -> None:
        self.event_gate_runner = event_gate_runner

    def _get_event_gate_runner(self, context: PipelineContext) -> EdgeEventGateRunner:
        if self.event_gate_runner is None:
            self.event_gate_runner = EdgeEventGateRunner(
                build_default_event_gates(context.config)
            )
        return self.event_gate_runner

    def on_tick(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if runtime.latest_feature_window is None:
            return
        event_gate_runner = self._get_event_gate_runner(context)
        detector_ids = ",".join(event_gate_runner.detector_ids()) or "none"
        observer.on_pipeline_step(
            "event_gating",
            (
                f"evaluate {detector_ids} "
                f"ts={runtime.latest_feature_window.timestamp.isoformat()} "
                f"mode=threshold_hysteresis"
            ),
            context,
        )
        runtime.latest_load_event_markers = event_gate_runner.evaluate_feature_window(
            runtime.latest_feature_window,
            context,
        )
        for marker in runtime.latest_load_event_markers:
            sink.append_load_event_marker(marker, context)
            observer.on_load_event_marker(marker, context)
            runtime.load_event_marker_count += 1


class StreamingCloudForecastPrepStage(StreamingPipelineStage):
    LATENCY_STAGE_NAME = "cloud_forecast_prep"

    def __init__(
            self,
            forecast_preparer: FheForecastFeaturePreparer | None = None,
            nilm_preparer: FheNilmFeaturePreparer | None = None,
            cohort_preparer: FheCohortFeaturePreparer | None = None,
    ) -> None:
        self.preparer = forecast_preparer
        self.nilm_preparer = nilm_preparer
        self.cohort_preparer = cohort_preparer

    def _get_preparer(self, context: PipelineContext) -> FheForecastFeaturePreparer:
        if self.preparer is None:
            self.preparer = FheForecastFeaturePreparer(context.config)
        return self.preparer

    def _get_nilm_preparer(self, context: PipelineContext) -> FheNilmFeaturePreparer:
        if self.nilm_preparer is None:
            self.nilm_preparer = FheNilmFeaturePreparer(context.config)
        return self.nilm_preparer

    def _get_cohort_preparer(self, context: PipelineContext) -> FheCohortFeaturePreparer:
        if self.cohort_preparer is None:
            self.cohort_preparer = FheCohortFeaturePreparer(context.config)
        return self.cohort_preparer

    def on_start(
            self,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if not context.config.get("cloud_forecast", {}).get("enabled", True):
            return
        preparer = self._get_preparer(context)
        nilm = self._get_nilm_preparer(context)
        cohort = self._get_cohort_preparer(context)
        horizons = ",".join(str(value) for value in preparer.horizons_minutes)
        observer.on_pipeline_step(
            "cloud_forecast_prep",
            (
                f"configured feature_set={preparer.feature_set_id} "
                f"horizons_minutes={horizons} "
                f"nilm_feature_set={nilm.feature_set_id} "
                f"cohort_feature_set={cohort.feature_set_id}"
            ),
            context,
        )

    def on_tick(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if not context.config.get("cloud_forecast", {}).get("enabled", True):
            return
        if runtime.latest_snapshot is None or runtime.latest_feature_window is None:
            return
        preparer = self._get_preparer(context)
        nilm_preparer = self._get_nilm_preparer(context)
        cohort_preparer = self._get_cohort_preparer(context)
        observer.on_pipeline_step(
            "cloud_forecast_prep",
            (
                f"build_cloud_features ts={runtime.latest_feature_window.timestamp.isoformat()} "
                f"feature_set={preparer.feature_set_id}"
            ),
            context,
        )
        forecast_feature, training_examples = preparer.observe(
            runtime.latest_snapshot,
            runtime.latest_feature_window,
            runtime.latest_load_event_markers,
        )
        runtime.latest_cloud_forecast_feature = forecast_feature
        runtime.cloud_forecast_feature_count += 1
        sink.append_cloud_forecast_feature(forecast_feature, context)
        observer.on_cloud_forecast_feature(forecast_feature, context)

        nilm_feature = nilm_preparer.build_feature_record(
            runtime.latest_snapshot,
            runtime.latest_feature_window,
            runtime.latest_load_event_markers,
        )
        runtime.latest_cloud_nilm_feature = nilm_feature
        sink.append_cloud_nilm_feature(nilm_feature, context)
        observer.on_cloud_nilm_feature(nilm_feature, context)

        cohort_feature = cohort_preparer.build_feature_record(
            runtime.latest_snapshot,
            runtime.latest_feature_window,
            runtime.latest_load_event_markers,
        )
        runtime.latest_cloud_cohort_feature = cohort_feature
        sink.append_cloud_cohort_feature(cohort_feature, context)
        observer.on_cloud_cohort_feature(cohort_feature, context)

        runtime.latest_cloud_forecast_training_examples = training_examples
        for training_example in training_examples:
            sink.append_cloud_forecast_training_example(training_example, context)
            observer.on_cloud_forecast_training_example(training_example, context)
            runtime.cloud_forecast_training_example_count += 1

    def on_complete(
            self,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if self.preparer is not None:
            context.config.setdefault("_runtime_cloud_forecast_metadata", self.preparer.metadata())
        context.config.setdefault(
            "_runtime_cloud_model_contracts",
            {
                "forecast": self._get_preparer(context).metadata(),
                "nilm": self._get_nilm_preparer(context).metadata(),
                "cohort": self._get_cohort_preparer(context).metadata(),
            },
        )


class StreamingFheCloudInferenceStage(StreamingPipelineStage):
    LATENCY_STAGE_NAME = "fhe_cloud_inference"

    def __init__(self, inference_runner: FheInferenceRunner | None = None) -> None:
        self.inference_runner = inference_runner
        self.forecast_tracker = ForecastEvaluationTracker()
        self._next_inference_tick: datetime | None = None
        self._async_dispatcher: AsyncFheDispatcher | None = None

    def _get_inference_runner(self, context: PipelineContext) -> FheInferenceRunner:
        if self.inference_runner is None:
            self.inference_runner = FheInferenceRunner(
                build_default_fhe_inference_tasks(context.config)
            )
        return self.inference_runner

    def on_start(
            self,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        runner = self._get_inference_runner(context)
        task_ids = ",".join(runner.task_ids()) or "none"
        mode = "async" if _fhe_async_enabled(context.config) else "sync"
        observer.on_pipeline_step(
            "fhe_cloud_inference",
            f"configured tasks={task_ids} mode={mode}",
            context,
        )

    def on_tick(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        self.forecast_tracker.resolve_due(tick_timestamp, runtime, context, sink)
        self._drain_async_results(runtime, context, sink, observer, record_latency=True)
        if runtime.latest_feature_window is None or runtime.latest_cloud_forecast_feature is None:
            return
        runner = self._get_inference_runner(context)
        task_ids = ",".join(runner.task_ids()) or "none"
        if task_ids == "none":
            return
        sample_interval_seconds = _fhe_sample_interval_seconds(context.config)
        if not self._should_run_fhe(tick_timestamp, sample_interval_seconds):
            observer.on_pipeline_step(
                "fhe_cloud_inference",
                (
                    f"skip remote FHE ts={tick_timestamp.isoformat()} "
                    f"next={self._next_inference_tick.isoformat() if self._next_inference_tick else 'n/a'}"
                ),
                context,
            )
            return
        async_enabled = _fhe_async_enabled(context.config)
        if async_enabled:
            dispatcher = self._get_async_dispatcher(context, runner)
            queued = dispatcher.submit(
                runtime.latest_cloud_forecast_feature,
                runtime.latest_cloud_nilm_feature,
                runtime.latest_cloud_cohort_feature,
                context,
            )
            if not queued:
                observer.on_pipeline_step(
                    "fhe_cloud_inference",
                    (
                        f"skip remote FHE ts={tick_timestamp.isoformat()} "
                        f"pending={dispatcher.pending_count}"
                    ),
                    context,
                )
                return
        model_input = build_model_input(
            runtime.latest_feature_window,
            runtime.latest_cloud_forecast_feature,
        )
        runtime.latest_model_input = model_input
        runtime.model_input_count += 1
        sink.append_model_input(model_input, context)
        observer.on_pipeline_step(
            "fhe_cloud_inference",
            (
                f"{'queue' if async_enabled else 'infer'} {task_ids} "
                f"ts={runtime.latest_cloud_forecast_feature.timestamp.isoformat()} "
                f"feature_set={runtime.latest_cloud_forecast_feature.feature_set_id}"
            ),
            context,
        )
        if async_enabled:
            return
        runtime.mark_fhe_cloud_sampled()
        cloud_started_at = time.monotonic()
        results = runner.infer_cloud_features(
            runtime.latest_cloud_forecast_feature,
            runtime.latest_cloud_nilm_feature,
            runtime.latest_cloud_cohort_feature,
            context,
        )
        cloud_elapsed_ms = _cloud_fhe_operations_ms_from_results(results)
        runtime.record_tick_stage_duration(
            "cloud_fhe_operations",
            cloud_elapsed_ms
            if cloud_elapsed_ms is not None
            else max(time.monotonic() - cloud_started_at, 0.0) * 1000.0,
        )
        self._record_model_results(results, runtime, context, sink, observer)

    def on_complete(
            self,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if self._async_dispatcher is None:
            return
        wait_seconds = _fhe_async_drain_timeout_seconds(context.config)
        deadline = time.monotonic() + wait_seconds
        while self._async_dispatcher.pending_count > 0 and time.monotonic() < deadline:
            self._drain_async_results(runtime, context, sink, observer, record_latency=False)
            time.sleep(0.01)
        self._drain_async_results(runtime, context, sink, observer, record_latency=False)
        self._async_dispatcher.stop()

    def _get_async_dispatcher(
            self,
            context: PipelineContext,
            runner: FheInferenceRunner,
    ) -> "AsyncFheDispatcher":
        if self._async_dispatcher is None:
            self._async_dispatcher = AsyncFheDispatcher(
                runner,
                max_pending_requests=_fhe_max_pending_requests(context.config),
            )
        return self._async_dispatcher

    def _drain_async_results(
            self,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
            record_latency: bool,
    ) -> None:
        if self._async_dispatcher is None:
            return
        completions = self._async_dispatcher.poll_completed()
        if not completions:
            return
        total_cloud_ms = round(sum(item.cloud_operations_ms for item in completions), 3)
        if record_latency:
            runtime.mark_fhe_cloud_sampled()
            runtime.record_tick_stage_duration("cloud_fhe_operations", total_cloud_ms)
        for completion in completions:
            if completion.error:
                observer.on_pipeline_step(
                    "fhe_cloud_inference",
                    (
                        f"async remote FHE failed ts={completion.request_timestamp.isoformat()} "
                        f"error={completion.error}"
                    ),
                    context,
                )
            self._record_model_results(
                completion.results,
                runtime,
                context,
                sink,
                observer,
            )

    def _record_model_results(
            self,
            results: list[ModelInferenceResult],
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        runtime.latest_model_results.extend(results)
        for model_result in results:
            sink.append_model_result(model_result, context)
            observer.on_model_result(model_result, context)
            runtime.record_model_result(model_result)
            runtime.model_result_count += 1
        self.forecast_tracker.remember(
            results,
            {
                "fhe_long_term_load_forecast": _fhe_forecast_task_config(context.config)
            },
        )

    def _should_run_fhe(
            self,
            tick_timestamp: datetime,
            sample_interval_seconds: int | None,
    ) -> bool:
        if sample_interval_seconds is None:
            return True
        if self._next_inference_tick is None:
            self._next_inference_tick = tick_timestamp + timedelta(seconds=sample_interval_seconds)
            return True
        if tick_timestamp < self._next_inference_tick:
            return False
        while self._next_inference_tick <= tick_timestamp:
            self._next_inference_tick += timedelta(seconds=sample_interval_seconds)
        return True


_ASYNC_FHE_STOP = object()


@dataclass(frozen=True)
class AsyncFheRequest:
    request_timestamp: datetime
    forecast_feature: CloudForecastFeatureRecord
    nilm_feature: CloudNilmFeatureRecord | None
    cohort_feature: CloudCohortFeatureRecord | None
    context: PipelineContext


@dataclass(frozen=True)
class AsyncFheCompletion:
    request_timestamp: datetime
    results: list[ModelInferenceResult]
    cloud_operations_ms: float
    error: str | None = None


class AsyncFheDispatcher:
    def __init__(
            self,
            runner: FheInferenceRunner,
            max_pending_requests: int = 1,
            monotonic_fn=None,
    ) -> None:
        self.runner = runner
        self.max_pending_requests = max(1, int(max_pending_requests))
        self.monotonic_fn = monotonic_fn or time.monotonic
        self._requests: Queue[Any] = Queue()
        self._completions: Queue[AsyncFheCompletion] = Queue()
        self._pending_count = 0
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stopped = False

    @property
    def pending_count(self) -> int:
        with self._lock:
            return self._pending_count

    def submit(
            self,
            forecast_feature: CloudForecastFeatureRecord,
            nilm_feature: CloudNilmFeatureRecord | None,
            cohort_feature: CloudCohortFeatureRecord | None,
            context: PipelineContext,
    ) -> bool:
        with self._lock:
            if self._stopped or self._pending_count >= self.max_pending_requests:
                return False
            self._pending_count += 1
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._run,
                    name="hyfhenet-fhe-dispatcher",
                    daemon=True,
                )
                self._thread.start()
        self._requests.put(
            AsyncFheRequest(
                request_timestamp=forecast_feature.timestamp,
                forecast_feature=forecast_feature,
                nilm_feature=nilm_feature,
                cohort_feature=cohort_feature,
                context=context,
            )
        )
        return True

    def poll_completed(self) -> list[AsyncFheCompletion]:
        completions: list[AsyncFheCompletion] = []
        while True:
            try:
                completions.append(self._completions.get_nowait())
            except Empty:
                return completions

    def stop(self) -> None:
        with self._lock:
            self._stopped = True
        self._requests.put(_ASYNC_FHE_STOP)

    def _run(self) -> None:
        while True:
            request = self._requests.get()
            if request is _ASYNC_FHE_STOP:
                return
            started_at = self.monotonic_fn()
            results: list[ModelInferenceResult] = []
            error = None
            try:
                results = self.runner.infer_cloud_features(
                    request.forecast_feature,
                    request.nilm_feature,
                    request.cohort_feature,
                    request.context,
                )
            except Exception as exc:
                error = f"{exc.__class__.__name__}: {exc}"
            elapsed_ms = round(max(self.monotonic_fn() - started_at, 0.0) * 1000.0, 3)
            cloud_operations_ms = _cloud_fhe_operations_ms_from_results(results)
            self._completions.put(
                AsyncFheCompletion(
                    request_timestamp=request.request_timestamp,
                    results=results,
                    cloud_operations_ms=cloud_operations_ms or elapsed_ms,
                    error=error,
                )
            )
            with self._lock:
                self._pending_count = max(self._pending_count - 1, 0)


class StreamingServiceStage(StreamingPipelineStage):
    LATENCY_STAGE_NAME = "service_inference"

    def __init__(self, service_runner: EdgeServiceRunner | None = None) -> None:
        self.service_runner = service_runner

    def _get_service_runner(self, context: PipelineContext) -> EdgeServiceRunner:
        if self.service_runner is None:
            self.service_runner = EdgeServiceRunner(
                build_default_edge_services(context.config)
            )
        return self.service_runner

    def on_tick(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if runtime.latest_feature_window is None:
            return
        service_runner = self._get_service_runner(context)
        service_ids = ",".join(service_runner.service_ids()) or "none"
        observer.on_pipeline_step(
            "service_inference",
            (
                f"evaluate {service_ids} "
                f"ts={runtime.latest_feature_window.timestamp.isoformat()} "
                f"mode=deterministic_rule_based"
            ),
            context,
        )
        runtime.latest_service_results = service_runner.evaluate_feature_window(
            runtime.latest_feature_window,
            context,
        )
        for service_result in runtime.latest_service_results:
            sink.append_service_result(service_result, context)
            observer.on_service_result(service_result, context)
            runtime.service_result_count += 1


@dataclass(frozen=True)
class PendingForecastPrediction:
    timestamp: datetime
    target_timestamp: datetime
    horizon_minutes: int
    model_id: str
    model_version: str
    feature_set_id: str | None
    predicted_household_power_w: float


class ForecastEvaluationTracker:
    def __init__(self) -> None:
        self.pending_forecast_predictions: deque[PendingForecastPrediction] = deque()

    def resolve_due(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
    ) -> None:
        if runtime.latest_snapshot is None:
            return
        target_power = _as_float(runtime.latest_snapshot.get("household_power_w"))
        while (
                self.pending_forecast_predictions
                and self.pending_forecast_predictions[0].target_timestamp <= tick_timestamp
        ):
            pending = self.pending_forecast_predictions.popleft()
            if target_power is None:
                continue
            error = pending.predicted_household_power_w - target_power
            evaluation = EdgeForecastEvaluationRecord(
                timestamp=pending.timestamp,
                target_timestamp=pending.target_timestamp,
                horizon_minutes=pending.horizon_minutes,
                model_id=pending.model_id,
                model_version=pending.model_version,
                feature_set_id=pending.feature_set_id,
                predicted_household_power_w=round(pending.predicted_household_power_w, 4),
                target_household_power_w=round(target_power, 4),
                forecast_error_w=round(error, 4),
                absolute_error_w=round(abs(error), 4),
            )
            sink.append_edge_forecast_evaluation(evaluation, context)
            runtime.record_forecast_evaluation(evaluation)
            runtime.edge_forecast_evaluation_count += 1

    def remember(
            self,
            model_results,
            default_config_by_model_id: dict[str, dict[str, Any]],
    ) -> None:
        for result in model_results:
            default_config = default_config_by_model_id.get(result.model_id)
            if default_config is None:
                continue
            if result.inference_status != "ok":
                continue
            predicted = _as_float(result.prediction_score)
            if predicted is None:
                continue
            horizon_minutes = int(default_config.get("horizon_minutes", 1))
            feature_set_id = None
            target_timestamp = result.timestamp
            try:
                details = json.loads(result.details)
                horizon_minutes = int(details.get("horizon_minutes", horizon_minutes))
                feature_set_id = details.get("feature_set_id")
                if details.get("target_timestamp"):
                    target_timestamp = datetime.fromisoformat(details["target_timestamp"])
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
            self.pending_forecast_predictions.append(
                PendingForecastPrediction(
                    timestamp=result.timestamp,
                    target_timestamp=target_timestamp,
                    horizon_minutes=horizon_minutes,
                    model_id=result.model_id,
                    model_version=result.model_version,
                    feature_set_id=feature_set_id,
                    predicted_household_power_w=predicted,
                )
            )


class StreamingAiModelStage(StreamingPipelineStage):
    LATENCY_STAGE_NAME = "ai_modeling"

    def __init__(self, model_runner: EdgeModelRunner | None = None) -> None:
        self.model_runner = model_runner
        self.forecast_tracker = ForecastEvaluationTracker()

    def _get_model_runner(self, context: PipelineContext) -> EdgeModelRunner:
        if self.model_runner is None:
            self.model_runner = EdgeModelRunner(build_default_edge_models(context.config))
        return self.model_runner

    def on_start(
            self,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        model_runner = self._get_model_runner(context)
        model_ids = ",".join(model_runner.model_ids()) or "none"
        observer.on_pipeline_step(
            "ai_modeling",
            (
                f"configured models={model_ids} "
                f"input_contract={MODEL_INPUT_CONTRACT_VERSION} "
                f"output_contract={MODEL_OUTPUT_CONTRACT_VERSION}"
            ),
            context,
        )

    def on_tick(
            self,
            tick_timestamp: datetime,
            runtime: StreamingPipelineRuntime,
            context: PipelineContext,
            sink: StreamingSink,
            observer: StreamingObserver,
    ) -> None:
        if runtime.latest_feature_window is None:
            return
        self.forecast_tracker.resolve_due(tick_timestamp, runtime, context, sink)
        model_runner = self._get_model_runner(context)
        model_input = build_model_input(
            runtime.latest_feature_window,
            runtime.latest_cloud_forecast_feature,
        )
        runtime.latest_model_input = model_input
        runtime.model_input_count += 1
        sink.append_model_input(model_input, context)
        model_ids = ",".join(model_runner.model_ids()) or "none"
        observer.on_pipeline_step(
            "ai_modeling",
            (
                f"infer {model_ids} "
                f"ts={model_input.timestamp.isoformat()} "
                f"input_contract={model_input.contract_version}"
            ),
            context,
        )
        results = model_runner.infer(model_input, context)
        runtime.latest_model_results.extend(results)
        for model_result in results:
            sink.append_model_result(model_result, context)
            observer.on_model_result(model_result, context)
            runtime.record_model_result(model_result)
            runtime.model_result_count += 1
        self.forecast_tracker.remember(
            results,
            {
                "edge_short_term_load_forecast": (
                    context.config.get("edge_models", {})
                    .get("short_term_load_forecast", {})
                )
            },
        )


StreamingStageFactory = Callable[[dict[str, Any]], StreamingPipelineStage]

DEFAULT_STREAMING_STAGE_ORDER = [
    "preprocessing",
    "feature_engineering",
    "event_gating",
    "cloud_forecast_prep",
    "fhe_cloud_inference",
    "service_inference",
    "ai_modeling",
]


def get_streaming_stage_factories() -> dict[str, StreamingStageFactory]:
    return {
        "preprocessing": lambda config: StreamingPreprocessingStage(
            DataQualityValidator(config)
        ),
        "feature_engineering": lambda config: StreamingFeatureStage(
            RollingFeatureEngine(config)
        ),
        "event_gating": lambda config: StreamingEventGateStage(
            EdgeEventGateRunner(build_default_event_gates(config))
        ),
        "cloud_forecast_prep": lambda config: StreamingCloudForecastPrepStage(
            FheForecastFeaturePreparer(config)
        ),
        "fhe_cloud_inference": lambda config: StreamingFheCloudInferenceStage(
            FheInferenceRunner(build_default_fhe_inference_tasks(config))
        ),
        "service_inference": lambda config: StreamingServiceStage(
            EdgeServiceRunner(build_default_edge_services(config))
        ),
        "ai_modeling": lambda config: StreamingAiModelStage(
            EdgeModelRunner(build_default_edge_models(config))
        ),
    }


def build_streaming_stages(
        stage_names: Sequence[str],
        config: dict[str, Any],
) -> list[StreamingPipelineStage]:
    factories = get_streaming_stage_factories()
    stages: list[StreamingPipelineStage] = []
    for stage_name in stage_names:
        factory = factories.get(stage_name)
        if factory is None:
            supported = ", ".join(sorted(factories))
            raise ValueError(
                f"Unsupported streaming stage '{stage_name}'. Supported stages: {supported}"
            )
        stages.append(factory(config))
    return stages


def build_default_streaming_stages(
        config: dict[str, Any],
) -> list[StreamingPipelineStage]:
    configured_order = config.get("gateway_stream", {}).get("stage_order")
    stage_order = configured_order or DEFAULT_STREAMING_STAGE_ORDER
    return build_streaming_stages(stage_order, config)


def describe_streaming_stages(
        stages: Sequence[StreamingPipelineStage],
        config: dict[str, Any],
) -> str:
    parts = []
    for stage in stages:
        if isinstance(stage, StreamingPreprocessingStage):
            parts.append("preprocessing=enabled")
        elif isinstance(stage, StreamingFeatureStage):
            parts.append(
                f"feature_engineering=feature_windows[{','.join(_feature_window_labels_from_config(config))}]"
            )
        elif isinstance(stage, StreamingEventGateStage):
            event_gate_runner = stage.event_gate_runner or EdgeEventGateRunner(
                build_default_event_gates(config)
            )
            parts.append(
                "event_gating="
                + ",".join(event_gate_runner.detector_ids() or ["none"])
            )
        elif isinstance(stage, StreamingCloudForecastPrepStage):
            preparer = stage.preparer or FheForecastFeaturePreparer(config)
            nilm = stage.nilm_preparer or FheNilmFeaturePreparer(config)
            cohort = stage.cohort_preparer or FheCohortFeaturePreparer(config)
            parts.append(
                "cloud_forecast_prep="
                + f"{preparer.feature_set_id}[{','.join(str(value) for value in preparer.horizons_minutes)}m]"
                + f",nilm={nilm.feature_set_id},cohort={cohort.feature_set_id}"
            )
        elif isinstance(stage, StreamingFheCloudInferenceStage):
            runner = stage.inference_runner or FheInferenceRunner(
                build_default_fhe_inference_tasks(config)
            )
            sample_interval_seconds = _fhe_sample_interval_seconds(config)
            interval_detail = (
                f" every_{sample_interval_seconds}s"
                if sample_interval_seconds is not None
                else ""
            )
            mode_detail = " async" if _fhe_async_enabled(config) else ""
            parts.append(
                "fhe_cloud_inference="
                + ",".join(runner.task_ids() or ["none"])
                + mode_detail
                + interval_detail
            )
        elif isinstance(stage, StreamingServiceStage):
            service_runner = stage.service_runner or EdgeServiceRunner(
                build_default_edge_services(config)
            )
            parts.append(
                "service_inference="
                + ",".join(service_runner.service_ids() or ["none"])
            )
        elif isinstance(stage, StreamingAiModelStage):
            model_runner = stage.model_runner or EdgeModelRunner(
                build_default_edge_models(config)
            )
            parts.append(
                "ai_modeling="
                + ",".join(model_runner.model_ids() or ["none"])
            )
        else:
            parts.append(f"{stage.__class__.__name__}=enabled")
    return " ".join(parts)


def _feature_window_labels(context: PipelineContext) -> list[str]:
    return _feature_window_labels_from_config(context.config)


def _feature_window_labels_from_config(config: dict[str, Any]) -> list[str]:
    labels = []
    for window_seconds in config["edge_processing"]["feature_windows_seconds"]:
        window_seconds = int(window_seconds)
        if window_seconds % 60 == 0:
            labels.append(f"{window_seconds // 60}m")
        else:
            labels.append(f"{window_seconds}s")
    return labels


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _cloud_fhe_operations_ms_from_results(model_results) -> float | None:
    cloud_keys = (
        "cloud_fhe_client_files_download_ms",
        "cloud_fhe_key_upload_ms",
        "cloud_fhe_inference_wait_ms",
    )
    values: list[float] = []
    for result in model_results:
        try:
            details = json.loads(result.details)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(details, dict):
            continue
        timing = details.get("timing_ms")
        if not isinstance(timing, dict):
            continue
        for key in cloud_keys:
            value = _as_float(timing.get(key))
            if value is not None:
                values.append(value)
    if not values:
        return None
    return round(sum(values), 3)


def _fhe_forecast_task_config(config: dict[str, Any]) -> dict[str, Any]:
    tasks = config.get("fhe_cloud", {}).get("tasks", {})
    return tasks.get("forecast") or tasks.get("long_term_load_forecast", {})


def _fhe_sample_interval_seconds(config: dict[str, Any]) -> int | None:
    value = config.get("fhe_cloud", {}).get("sample_interval_seconds")
    if value is None:
        value = config.get("gateway_stream", {}).get("fhe_cloud_sample_interval_seconds")
    if value in (None, "", 0, "0"):
        return None
    interval_seconds = int(value)
    if interval_seconds <= 0:
        return None
    return interval_seconds


def _fhe_async_enabled(config: dict[str, Any]) -> bool:
    value = config.get("fhe_cloud", {}).get("async_enabled", False)
    return _as_bool(value)


def _fhe_max_pending_requests(config: dict[str, Any]) -> int:
    value = config.get("fhe_cloud", {}).get("max_pending_requests", 1)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1


def _fhe_async_drain_timeout_seconds(config: dict[str, Any]) -> float:
    value = config.get("fhe_cloud", {}).get("async_drain_timeout_seconds", 0.0)
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 0.0


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)




