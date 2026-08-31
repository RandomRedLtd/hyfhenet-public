from __future__ import annotations

import threading
import time
from datetime import timedelta
from queue import Empty, Queue
from typing import Any, Sequence

from ..core.interfaces import (
    PipelineContext,
    StreamingObserver,
    StreamingPipelineStage,
    StreamingSink,
)
from ..core.models import GatewayStreamRunSummary, RawTelemetryEvent, StreamingPipelineRuntime
from ..processing.stream_stages import build_default_streaming_stages, describe_streaming_stages
from .profiling import (
    build_forecast_quality_summary,
    build_latency_summary,
    build_model_result_summary,
    build_performance_summary,
)
from .observers import NullStreamingObserver

_STREAM_DONE = object()


class GatewayStreamingPipeline:
    def __init__(
        self,
        source,
        sink: StreamingSink,
        stages: Sequence[StreamingPipelineStage] | None = None,
        observer: StreamingObserver | None = None,
        monotonic_fn=None,
    ) -> None:
        self.source = source
        self.sink = sink
        self.stages = list(stages) if stages is not None else []
        self.observer = observer or NullStreamingObserver()
        self.monotonic_fn = monotonic_fn or time.monotonic

    def run(self, context: PipelineContext) -> GatewayStreamRunSummary:
        run_started_at = self.monotonic_fn()
        self.sink.open(context)
        self.observer.on_stream_start(context)
        stages = self.stages or build_default_streaming_stages(context.config)
        runtime = StreamingPipelineRuntime(
            tick_interval_seconds=int(context.config["pipeline"]["interval_seconds"]),
            tick_clock_scale=self._tick_clock_scale(context),
            idle_poll_seconds=float(
                context.config["gateway_stream"].get("idle_poll_seconds", 0.1)
            ),
            group_flush_seconds=float(
                context.config["gateway_stream"].get("group_flush_seconds", 0.2)
            ),
            profiling_enabled=bool(
                context.config["gateway_stream"].get("profiling_enabled", True)
            ),
        )
        self.observer.on_pipeline_step(
            "pipeline_ready",
            describe_streaming_stages(stages, context.config),
            context,
        )
        for stage in stages:
            stage.on_start(runtime, context, self.sink, self.observer)

        event_queue: Queue[Any] = Queue()
        producer = threading.Thread(
            target=self._pump_source_events,
            args=(context, event_queue),
            daemon=True,
        )
        producer.start()

        try:
            grouped_events: list[RawTelemetryEvent] = []
            group_last_seen_at: float | None = None
            source_done = False

            while True:
                now = self.monotonic_fn()
                if (
                    grouped_events
                    and group_last_seen_at is not None
                    and (now - group_last_seen_at) >= runtime.group_flush_seconds
                ):
                    self._process_group(grouped_events, runtime, stages, context)
                    grouped_events = []
                    group_last_seen_at = None
                    self._maybe_sleep(context)
                    now = self.monotonic_fn()

                if not grouped_events:
                    self._emit_due_ticks(runtime, stages, context, now)

                if source_done and not grouped_events:
                    break

                try:
                    item = event_queue.get(timeout=runtime.idle_poll_seconds)
                except Empty:
                    continue

                if item is _STREAM_DONE:
                    source_done = True
                    continue

                if isinstance(item, Exception):
                    raise item

                raw_event = item
                runtime.observe_raw_event(raw_event)
                if grouped_events and raw_event.timestamp != grouped_events[-1].timestamp:
                    self._process_group(grouped_events, runtime, stages, context)
                    grouped_events = [raw_event]
                else:
                    grouped_events.append(raw_event)
                group_last_seen_at = self.monotonic_fn()
        finally:
            for stage in stages:
                stage.on_complete(runtime, context, self.sink, self.observer)
            stream_runtime_summary = self._stream_runtime_summary(context)
            wall_clock_elapsed_seconds = max(self.monotonic_fn() - run_started_at, 0.0)
            summary = GatewayStreamRunSummary(
                input_path=str(context.input_path),
                output_dir=str(context.output_dir.resolve()),
                raw_event_count=runtime.raw_event_count,
                normalized_event_count=runtime.normalized_event_count,
                snapshot_count=runtime.snapshot_count,
                quality_alert_count=runtime.quality_alert_count,
                feature_window_count=runtime.feature_window_count,
                cloud_forecast_feature_count=runtime.cloud_forecast_feature_count,
                cloud_forecast_training_example_count=runtime.cloud_forecast_training_example_count,
                load_event_marker_count=runtime.load_event_marker_count,
                edge_forecast_evaluation_count=runtime.edge_forecast_evaluation_count,
                service_result_count=runtime.service_result_count,
                model_input_count=runtime.model_input_count,
                model_result_count=runtime.model_result_count,
                household_power_source=(
                    (runtime.latest_snapshot or {}).get("household_power_source")
                    or "sum_of_configured_smart_plugs"
                ),
                time_range={
                    "start": runtime.first_event_timestamp.isoformat() if runtime.first_event_timestamp else None,
                    "end": runtime.last_event_timestamp.isoformat() if runtime.last_event_timestamp else None,
                },
                devices=dict(runtime.devices),
                latency_summary=build_latency_summary(runtime.latency_samples),
                performance_summary=build_performance_summary(
                    runtime,
                    wall_clock_elapsed_seconds,
                ),
                forecast_quality_summary=build_forecast_quality_summary(
                    runtime.forecast_evaluations,
                ),
                model_result_summary=build_model_result_summary(runtime.model_results),
                stream_mode=stream_runtime_summary.get("stream_mode"),
                mqtt_listen_seconds=stream_runtime_summary.get("mqtt_listen_seconds"),
                stop_reason=stream_runtime_summary.get("stop_reason"),
            )
            self.sink.close(summary, context)
            self.observer.on_stream_complete(summary, context)

        return summary

    def _process_group(
        self,
        grouped_events: list[RawTelemetryEvent],
        runtime: StreamingPipelineRuntime,
        stages: Sequence[StreamingPipelineStage],
        context: PipelineContext,
    ) -> None:
        if not grouped_events:
            return
        self.observer.on_pipeline_step(
            "group_processing",
            f"events={len(grouped_events)}",
            context,
        )
        self.observer.on_event_group(grouped_events, context)
        runtime.start_group_processing(self.monotonic_fn())
        group_timestamp = grouped_events[-1].timestamp
        for stage in stages:
            stage.on_event_group(grouped_events, runtime, context, self.sink, self.observer)
        processed_at = self.monotonic_fn()
        runtime.finish_group_processing(processed_at)
        runtime.mark_group_processed(group_timestamp, processed_at)
        self._emit_due_ticks(runtime, stages, context, processed_at)

    def _emit_due_ticks(
        self,
        runtime: StreamingPipelineRuntime,
        stages: Sequence[StreamingPipelineStage],
        context: PipelineContext,
        monotonic_now: float,
    ) -> None:
        virtual_now = runtime.virtual_now(monotonic_now)
        if virtual_now is None or runtime.next_snapshot_time is None:
            return
        while runtime.next_snapshot_time <= virtual_now:
            tick_timestamp = runtime.next_snapshot_time
            runtime.start_tick(self.monotonic_fn())
            for stage in stages:
                stage_started_at = self.monotonic_fn()
                stage.on_tick(tick_timestamp, runtime, context, self.sink, self.observer)
                stage_finished_at = self.monotonic_fn()
                stage_name = getattr(stage, "LATENCY_STAGE_NAME", stage.__class__.__name__)
                runtime.record_tick_stage_duration(
                    stage_name,
                    max(stage_finished_at - stage_started_at, 0.0) * 1000.0,
                )
            latency_sample = runtime.finish_tick(tick_timestamp, self.monotonic_fn())
            if latency_sample is not None:
                self.sink.append_latency_sample(latency_sample, context)
                self.observer.on_latency_sample(latency_sample, context)
            runtime.next_snapshot_time += timedelta(seconds=runtime.tick_interval_seconds)

    def _pump_source_events(self, context: PipelineContext, event_queue: Queue[Any]) -> None:
        try:
            for event in self.source.stream(context):
                event_queue.put(event)
        except Exception as exc:
            event_queue.put(exc)
        finally:
            event_queue.put(_STREAM_DONE)

    @staticmethod
    def _tick_clock_scale(context: PipelineContext) -> float:
        gateway_config = context.config["gateway_stream"]
        zigbee_mode = gateway_config.get("zigbee_source", "replay")
        if zigbee_mode in {"zigbee_mqtt", "edf_service_sdk"}:
            return 1.0
        if bool(gateway_config.get("follow_event_timing", False)):
            return float(gateway_config.get("replay_speed_multiplier", 1.0) or 1.0)
        return 0.0

    @staticmethod
    def _maybe_sleep(context: PipelineContext) -> None:
        if bool(context.config["gateway_stream"].get("follow_event_timing", False)):
            return
        delay_ms = int(context.config["gateway_stream"].get("replay_delay_ms", 0))
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    @staticmethod
    def _stream_runtime_summary(context: PipelineContext) -> dict[str, Any]:
        gateway_config = context.config["gateway_stream"]
        zigbee_mode = gateway_config.get("zigbee_source", "replay")
        if zigbee_mode == "edf_service_sdk":
            listen_seconds = int(
                context.config["edf_service_sdk"].get("listen_seconds", 0)
            )
            return {
                "stream_mode": context.config.get("_stream_mode_override", "edf_sdk_live"),
                "mqtt_listen_seconds": listen_seconds,
                "stop_reason": (
                    "manual_stop_or_external_interruption"
                    if listen_seconds < 0
                    else "configured_listen_window_elapsed"
                ),
            }
        if zigbee_mode != "zigbee_mqtt":
            return {}
        listen_seconds = int(context.config["zigbee_gateway"].get("listen_seconds", 0))
        return {
            "stream_mode": context.config.get("_stream_mode_override", "mqtt_live"),
            "mqtt_listen_seconds": listen_seconds,
            "stop_reason": (
                "manual_stop_or_external_interruption"
                if listen_seconds < 0
                else "configured_listen_window_elapsed"
            ),
        }
