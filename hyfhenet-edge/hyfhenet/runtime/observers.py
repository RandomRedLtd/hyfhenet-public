from __future__ import annotations

import sys
from typing import Any

from ..core.interfaces import PipelineContext, StreamingObserver
from ..core.models import (
    CloudForecastFeatureRecord,
    CloudForecastTrainingExample,
    FeatureWindow,
    GatewayStreamRunSummary,
    LatencySample,
    LoadEventMarker,
    ModelInferenceResult,
    RawTelemetryEvent,
)


class NullStreamingObserver(StreamingObserver):
    pass


class ConsoleStreamingObserver(NullStreamingObserver):
    def __init__(self, log_level: str = "progress", stream=None) -> None:
        self.log_level = log_level
        self.stream = stream or sys.stdout

    def on_stream_start(self, context: PipelineContext) -> None:
        self._write(f"[stream] start input={context.input_path} output={context.output_dir}")

    def on_stream_wait(
        self,
        delay_seconds: float,
        previous_event: RawTelemetryEvent,
        next_event: RawTelemetryEvent,
        context: PipelineContext,
    ) -> None:
        if delay_seconds >= 0.1:
            self._write(
                "[wait] "
                f"sleep={delay_seconds:.2f}s "
                f"from={previous_event.timestamp.isoformat()} "
                f"to={next_event.timestamp.isoformat()}"
            )

    def on_event_group(self, events: list[RawTelemetryEvent], context: PipelineContext) -> None:
        devices = ",".join(sorted({event.device for event in events}))
        self._write(
            f"[group] ts={events[0].timestamp.isoformat()} raw_events={len(events)} "
            f"devices={devices}"
        )
        if self.log_level == "verbose":
            for event in events:
                self._write(
                    "[raw] "
                    f"ts={event.timestamp.isoformat()} device={event.device} "
                    f"field={event.field} value={event.value} source={event.source}"
                )

    def on_pipeline_step(self, step: str, detail: str, context: PipelineContext) -> None:
        self._write(f"[step] stage={step} detail={detail}")

    def on_normalized_event(
        self,
        raw_event: RawTelemetryEvent,
        normalized_event,
        context: PipelineContext,
    ) -> None:
        if self.log_level == "verbose":
            self._write(
                "[normalized] "
                f"ts={normalized_event.timestamp.isoformat()} "
                f"device={normalized_event.device_id} role={normalized_event.device_role} "
                f"field={normalized_event.field} value={normalized_event.value}"
            )

    def on_quality_alert(self, alert: dict[str, Any], context: PipelineContext) -> None:
        self._write(
            "[alert] "
            f"ts={alert['timestamp']} code={alert['code']} "
            f"device={alert['device_id']} field={alert['field']} value={alert['value']}"
        )

    def on_snapshot(self, snapshot: dict[str, Any], context: PipelineContext) -> None:
        self._write(
            "[snapshot] "
            f"ts={snapshot['timestamp']} plug_power_w={snapshot.get('plug_power_w')} "
            f"household_power_w={snapshot.get('household_power_w')} "
            f"temperature_c={snapshot.get('indoor_temperature_c')}"
        )

    def on_feature_window(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> None:
        if self.log_level != "verbose":
            return
        record = feature_window.to_record()
        self._write(
            "[feature] "
            f"ts={record['timestamp']} schema={record['schema_version']} "
            f"household_mean_1m={record.get('household_power_mean_1m_w')}"
        )

    def on_service_result(self, service_result, context: PipelineContext) -> None:
        self._write(
            "[service] "
            f"ts={service_result.timestamp.isoformat()} "
            f"id={service_result.service_id} status={service_result.service_status} "
            f"profile={service_result.profile_state}"
        )

    def on_cloud_forecast_feature(
        self,
        forecast_feature: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> None:
        if self.log_level == "verbose":
            self._write(
                "[forecast-feature] "
                f"ts={forecast_feature.timestamp.isoformat()} "
                f"feature_set={forecast_feature.feature_set_id}"
            )

    def on_cloud_forecast_training_example(
        self,
        training_example: CloudForecastTrainingExample,
        context: PipelineContext,
    ) -> None:
        self._write(
            "[forecast-training] "
            f"ts={training_example.timestamp.isoformat()} "
            f"horizon_min={training_example.horizon_minutes} "
            f"target_w={training_example.target_household_power_w}"
        )

    def on_load_event_marker(self, marker: LoadEventMarker, context: PipelineContext) -> None:
        self._write(
            "[event] "
            f"ts={marker.timestamp.isoformat()} type={marker.event_type} "
            f"direction={marker.direction} magnitude_w={marker.magnitude_w} "
            f"forward={marker.should_forward}"
        )

    def on_model_result(
        self,
        model_result: ModelInferenceResult,
        context: PipelineContext,
    ) -> None:
        self._write(
            "[model] "
            f"ts={model_result.timestamp.isoformat()} id={model_result.model_id} "
            f"status={model_result.inference_status} "
            f"label={model_result.prediction_label} score={model_result.prediction_score}"
        )

    def on_latency_sample(
        self,
        latency_sample: LatencySample,
        context: PipelineContext,
    ) -> None:
        self._write(
            "[timing] "
            f"ts={latency_sample.tick_timestamp.isoformat()} "
            f"edge_local_ms={latency_sample.edge_local_operations_ms} "
            f"cloud_fhe_ms={latency_sample.cloud_fhe_operations_ms} "
            f"total_ms={latency_sample.total_tick_ms}"
        )

    def on_stream_complete(
        self,
        summary: GatewayStreamRunSummary,
        context: PipelineContext,
    ) -> None:
        self._write(
            "[stream] complete "
            f"raw={summary.raw_event_count} normalized={summary.normalized_event_count} "
            f"snapshots={summary.snapshot_count} alerts={summary.quality_alert_count} "
            f"features={summary.feature_window_count} events={summary.load_event_marker_count} "
            f"services={summary.service_result_count} models={summary.model_result_count}"
        )

    def _write(self, line: str) -> None:
        self.stream.write(line + "\n")
        self.stream.flush()
