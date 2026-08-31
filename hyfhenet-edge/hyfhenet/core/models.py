from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class RawTelemetryEvent:
    timestamp: datetime
    device: str
    field: str
    value: str
    source: str = "zigbee"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, str]:
        return {
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "device": self.device,
            "field": self.field,
            "value": self.value,
            "source": self.source,
        }


@dataclass(frozen=True)
class NormalizedEvent:
    timestamp: datetime
    device_id: str
    device_role: str
    device_name: str
    field: str
    value: str | float | int
    unit: str | None
    source: str = "zigbee"

    def to_record(self) -> dict[str, str | float | int | None]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat()
        return record


@dataclass(frozen=True)
class EdgeServiceResult:
    timestamp: datetime
    service_id: str
    service_version: str
    service_status: str
    profile_state: str
    activity_state: str
    household_context: str | None
    data_quality: str
    dominant_load_flag: int
    appliance_share_pct: float | None
    plug_power_mean_1m_w: float | None
    plug_power_mean_5m_w: float | None
    household_power_mean_1m_w: float | None
    household_power_mean_5m_w: float | None
    reasons: str

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat()
        return record


@dataclass(frozen=True)
class LoadEventMarker:
    timestamp: datetime
    detector_id: str
    detector_version: str
    detector_status: str
    event_type: str
    event_state: str
    signal_name: str
    direction: str
    magnitude_w: float | None
    baseline_w: float | None
    current_w: float | None
    threshold_w: float | None
    appliance_share_pct: float | None
    confidence: float
    should_forward: int
    reasons: str

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat()
        return record


@dataclass(frozen=True)
class CloudForecastFeatureRecord:
    timestamp: datetime
    schema_version: str
    feature_set_id: str
    values: dict[str, Any]

    def to_record(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "schema_version": self.schema_version,
            "feature_set_id": self.feature_set_id,
            **self.values,
        }


CloudNilmFeatureRecord = CloudForecastFeatureRecord
CloudCohortFeatureRecord = CloudForecastFeatureRecord


@dataclass(frozen=True)
class CloudForecastTrainingExample:
    timestamp: datetime
    schema_version: str
    feature_set_id: str
    horizon_minutes: int
    target_timestamp: datetime
    target_household_power_w: float
    values: dict[str, Any]

    def to_record(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "schema_version": self.schema_version,
            "feature_set_id": self.feature_set_id,
            "horizon_minutes": self.horizon_minutes,
            "target_timestamp": self.target_timestamp.isoformat(),
            "target_household_power_w": self.target_household_power_w,
            **self.values,
        }


@dataclass(frozen=True)
class FeatureWindow:
    timestamp: datetime
    schema_version: str
    values: dict[str, Any]

    def to_record(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "schema_version": self.schema_version,
            **self.values,
        }

    def get(self, key: str, default: Any = None) -> Any:
        if key == "timestamp":
            return self.timestamp.isoformat()
        if key == "schema_version":
            return self.schema_version
        return self.values.get(key, default)


@dataclass(frozen=True)
class ModelInput:
    timestamp: datetime
    contract_version: str
    feature_window: FeatureWindow
    forecast_feature_record: CloudForecastFeatureRecord | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "contract_version": self.contract_version,
            "feature_schema_version": self.feature_window.schema_version,
            **self.feature_window.values,
        }


@dataclass(frozen=True)
class ModelInferenceResult:
    timestamp: datetime
    model_id: str
    model_version: str
    backend: str
    input_contract_version: str
    output_contract_version: str
    inference_status: str
    prediction_label: str
    prediction_score: float | None
    anomaly_score: float | None
    load_score: float | None
    details: str

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat()
        return record


@dataclass(frozen=True)
class EdgeForecastEvaluationRecord:
    timestamp: datetime
    target_timestamp: datetime
    horizon_minutes: int
    model_id: str
    model_version: str
    feature_set_id: str | None
    predicted_household_power_w: float
    target_household_power_w: float
    forecast_error_w: float
    absolute_error_w: float

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat()
        record["target_timestamp"] = self.target_timestamp.isoformat()
        return record


@dataclass(frozen=True)
class LatencySample:
    tick_timestamp: datetime
    latest_event_timestamp: datetime | None
    raw_event_count: int
    normalized_event_count: int
    snapshot_count: int
    cloud_forecast_feature_count: int
    cloud_forecast_training_example_count: int
    load_event_marker_count: int
    edge_forecast_evaluation_count: int
    service_result_count: int
    model_result_count: int
    group_processing_ms: float | None
    snapshot_stage_ms: float | None
    feature_stage_ms: float | None
    event_gate_stage_ms: float | None
    forecast_prep_stage_ms: float | None
    fhe_cloud_sampled: int
    fhe_cloud_stage_ms: float | None
    service_stage_ms: float | None
    model_stage_ms: float | None
    edge_local_operations_ms: float | None = None
    cloud_fhe_operations_ms: float | None = None
    total_tick_ms: float | None = None
    event_to_model_ms: float | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "tick_timestamp": self.tick_timestamp.isoformat(),
            "latest_event_timestamp": (
                self.latest_event_timestamp.isoformat()
                if self.latest_event_timestamp is not None
                else None
            ),
            "raw_event_count": self.raw_event_count,
            "normalized_event_count": self.normalized_event_count,
            "snapshot_count": self.snapshot_count,
            "cloud_forecast_feature_count": self.cloud_forecast_feature_count,
            "cloud_forecast_training_example_count": self.cloud_forecast_training_example_count,
            "load_event_marker_count": self.load_event_marker_count,
            "edge_forecast_evaluation_count": self.edge_forecast_evaluation_count,
            "service_result_count": self.service_result_count,
            "model_result_count": self.model_result_count,
            "group_processing_ms": self.group_processing_ms,
            "snapshot_stage_ms": self.snapshot_stage_ms,
            "feature_stage_ms": self.feature_stage_ms,
            "event_gate_stage_ms": self.event_gate_stage_ms,
            "forecast_prep_stage_ms": self.forecast_prep_stage_ms,
            "fhe_cloud_sampled": self.fhe_cloud_sampled,
            "fhe_cloud_stage_ms": self.fhe_cloud_stage_ms,
            "service_stage_ms": self.service_stage_ms,
            "model_stage_ms": self.model_stage_ms,
            "edge_local_operations_ms": self.edge_local_operations_ms,
            "cloud_fhe_operations_ms": self.cloud_fhe_operations_ms,
            "total_tick_ms": self.total_tick_ms,
            "event_to_model_ms": self.event_to_model_ms,
        }


@dataclass(frozen=True)
class GatewayStreamRunSummary:
    input_path: str
    output_dir: str
    raw_event_count: int
    normalized_event_count: int
    snapshot_count: int
    quality_alert_count: int
    feature_window_count: int
    cloud_forecast_feature_count: int
    cloud_forecast_training_example_count: int
    load_event_marker_count: int
    edge_forecast_evaluation_count: int
    service_result_count: int
    model_input_count: int
    model_result_count: int
    household_power_source: str
    time_range: dict[str, str | None]
    devices: dict[str, int]
    latency_summary: dict[str, Any] | None = None
    performance_summary: dict[str, Any] | None = None
    forecast_quality_summary: dict[str, Any] | None = None
    model_result_summary: dict[str, Any] | None = None
    stream_mode: str | None = None
    mqtt_listen_seconds: int | None = None
    stop_reason: str | None = None

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class StreamingPipelineRuntime:
    tick_interval_seconds: int
    tick_clock_scale: float
    idle_poll_seconds: float
    group_flush_seconds: float
    state: dict[str, Any] = field(default_factory=dict)
    last_update: dict[str, datetime] = field(default_factory=dict)
    devices: Counter = field(default_factory=Counter)
    raw_event_count: int = 0
    normalized_event_count: int = 0
    snapshot_count: int = 0
    quality_alert_count: int = 0
    feature_window_count: int = 0
    cloud_forecast_feature_count: int = 0
    cloud_forecast_training_example_count: int = 0
    load_event_marker_count: int = 0
    edge_forecast_evaluation_count: int = 0
    service_result_count: int = 0
    model_input_count: int = 0
    model_result_count: int = 0
    profiling_enabled: bool = False
    first_event_timestamp: datetime | None = None
    last_event_timestamp: datetime | None = None
    stream_clock_timestamp: datetime | None = None
    stream_clock_started_at: float | None = None
    latest_group_started_at: float | None = None
    latest_group_completed_at: float | None = None
    latest_group_processing_ms: float | None = None
    next_snapshot_time: datetime | None = None
    latest_snapshot: dict[str, Any] | None = None
    latest_feature_window: FeatureWindow | None = None
    latest_cloud_forecast_feature: CloudForecastFeatureRecord | None = None
    latest_cloud_nilm_feature: CloudNilmFeatureRecord | None = None
    latest_cloud_cohort_feature: CloudCohortFeatureRecord | None = None
    latest_cloud_forecast_training_examples: list[CloudForecastTrainingExample] = field(default_factory=list)
    latest_load_event_markers: list[LoadEventMarker] = field(default_factory=list)
    latest_service_results: list[EdgeServiceResult] = field(default_factory=list)
    latest_model_input: ModelInput | None = None
    latest_model_results: list[ModelInferenceResult] = field(default_factory=list)
    model_results: list[ModelInferenceResult] = field(default_factory=list)
    forecast_evaluations: list[EdgeForecastEvaluationRecord] = field(default_factory=list)
    current_tick_started_at: float | None = None
    current_tick_fhe_cloud_sampled: bool = False
    current_tick_stage_durations_ms: dict[str, float] = field(default_factory=dict)
    latency_samples: list[LatencySample] = field(default_factory=list)

    def observe_raw_event(self, event: RawTelemetryEvent) -> None:
        self.raw_event_count += 1
        if self.first_event_timestamp is None:
            self.first_event_timestamp = event.timestamp
        self.last_event_timestamp = event.timestamp

    def mark_group_processed(self, timestamp: datetime, monotonic_now: float) -> None:
        self.stream_clock_timestamp = timestamp
        self.stream_clock_started_at = monotonic_now
        if self.next_snapshot_time is None:
            self.next_snapshot_time = timestamp

    def virtual_now(self, monotonic_now: float) -> datetime | None:
        if self.stream_clock_timestamp is None or self.stream_clock_started_at is None:
            return None
        if self.tick_clock_scale <= 0:
            return self.stream_clock_timestamp
        elapsed = max(monotonic_now - self.stream_clock_started_at, 0.0)
        return self.stream_clock_timestamp + timedelta(
            seconds=elapsed * self.tick_clock_scale
        )

    def start_group_processing(self, monotonic_now: float) -> None:
        self.latest_group_started_at = monotonic_now

    def finish_group_processing(self, monotonic_now: float) -> None:
        self.latest_group_completed_at = monotonic_now
        if self.latest_group_started_at is None:
            self.latest_group_processing_ms = None
            return
        self.latest_group_processing_ms = round(
            max(monotonic_now - self.latest_group_started_at, 0.0) * 1000.0,
            3,
            )

    def start_tick(self, monotonic_now: float) -> None:
        self.current_tick_started_at = monotonic_now
        self.current_tick_fhe_cloud_sampled = False
        self.current_tick_stage_durations_ms = {}

    def record_tick_stage_duration(self, stage_name: str, duration_ms: float) -> None:
        self.current_tick_stage_durations_ms[stage_name] = round(duration_ms, 3)

    def mark_fhe_cloud_sampled(self) -> None:
        self.current_tick_fhe_cloud_sampled = True

    def finish_tick(self, tick_timestamp: datetime, monotonic_now: float) -> LatencySample | None:
        if not self.profiling_enabled:
            return None
        total_tick_ms = None
        if self.current_tick_started_at is not None:
            total_tick_ms = round(
                max(monotonic_now - self.current_tick_started_at, 0.0) * 1000.0,
                3,
                )
        event_to_model_ms = None
        if self.latest_group_completed_at is not None:
            event_to_model_ms = round(
                max(monotonic_now - self.latest_group_completed_at, 0.0) * 1000.0,
                3,
                )
        fhe_cloud_stage_ms = None
        if self.current_tick_fhe_cloud_sampled:
            fhe_cloud_stage_ms = (
                    self.current_tick_stage_durations_ms.get("cloud_fhe_operations")
                    or self.current_tick_stage_durations_ms.get("fhe_cloud_inference")
            )
        edge_local_operations_ms = self._edge_local_operations_ms(fhe_cloud_stage_ms)
        sample = LatencySample(
            tick_timestamp=tick_timestamp,
            latest_event_timestamp=self.last_event_timestamp,
            raw_event_count=self.raw_event_count,
            normalized_event_count=self.normalized_event_count,
            snapshot_count=self.snapshot_count,
            cloud_forecast_feature_count=self.cloud_forecast_feature_count,
            cloud_forecast_training_example_count=self.cloud_forecast_training_example_count,
            load_event_marker_count=self.load_event_marker_count,
            edge_forecast_evaluation_count=self.edge_forecast_evaluation_count,
            service_result_count=self.service_result_count,
            model_result_count=self.model_result_count,
            group_processing_ms=self.latest_group_processing_ms,
            snapshot_stage_ms=self.current_tick_stage_durations_ms.get("preprocessing"),
            feature_stage_ms=self.current_tick_stage_durations_ms.get("feature_engineering"),
            event_gate_stage_ms=self.current_tick_stage_durations_ms.get("event_gating"),
            forecast_prep_stage_ms=self.current_tick_stage_durations_ms.get("cloud_forecast_prep"),
            fhe_cloud_sampled=1 if self.current_tick_fhe_cloud_sampled else 0,
            fhe_cloud_stage_ms=fhe_cloud_stage_ms,
            service_stage_ms=self.current_tick_stage_durations_ms.get("service_inference"),
            model_stage_ms=self.current_tick_stage_durations_ms.get("ai_modeling"),
            edge_local_operations_ms=edge_local_operations_ms,
            cloud_fhe_operations_ms=fhe_cloud_stage_ms,
            total_tick_ms=total_tick_ms,
            event_to_model_ms=event_to_model_ms,
        )
        self.latency_samples.append(sample)
        return sample

    def _edge_local_operations_ms(self, fhe_cloud_stage_ms: float | None) -> float | None:
        local_stage_names = (
            "preprocessing",
            "feature_engineering",
            "event_gating",
            "cloud_forecast_prep",
            "service_inference",
            "ai_modeling",
        )
        values = [
            float(value)
            for stage_name in local_stage_names
            if (value := self.current_tick_stage_durations_ms.get(stage_name)) is not None
        ]
        if self.current_tick_fhe_cloud_sampled:
            full_fhe_stage_ms = self.current_tick_stage_durations_ms.get(
                "fhe_cloud_inference"
            )
            if full_fhe_stage_ms is not None:
                fhe_edge_overhead_ms = max(
                    float(full_fhe_stage_ms) - float(fhe_cloud_stage_ms or 0.0),
                    0.0,
                    )
                if fhe_edge_overhead_ms > 0:
                    values.append(fhe_edge_overhead_ms)
        elif (
                skipped_fhe_stage_ms := self.current_tick_stage_durations_ms.get(
                    "fhe_cloud_inference"
                )
        ) is not None:
            values.append(float(skipped_fhe_stage_ms))
        if not values:
            return None
        return round(sum(values), 3)

    def record_model_result(self, result: ModelInferenceResult) -> None:
        self.model_results.append(result)

    def record_forecast_evaluation(
            self,
            evaluation: EdgeForecastEvaluationRecord,
    ) -> None:
        self.forecast_evaluations.append(evaluation)

