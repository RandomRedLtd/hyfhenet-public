from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from ..core.config import load_pipeline_config, select_fhe_tasks
from ..core.interfaces import (
    PipelineContext,
    StreamingEventSource,
    StreamingObserver,
    StreamingSink,
)
from ..ingestion.stream_sources import (
    CaptureRawEventSource,
    EdfServiceSdkEventSource,
    TimestampPacedStreamSource,
    ZigbeeCsvReplaySource,
    ZigbeeMqttSensorSource,
    build_zigbee_stream_source,
)
from ..processing.stream_stages import build_default_streaming_stages
from .observers import (
    ConsoleStreamingObserver,
    NullStreamingObserver,
)
from .orchestrator import GatewayStreamingPipeline
from .sinks import AppendFileStreamingSink, NullStreamingSink


def build_gateway_stream_context(
    input_path: Path | str,
    output_dir: Path | str,
    config_path: Path | str | None = None,
    interval_seconds: int | None = None,
    input_source: str | None = None,
    zigbee_source: str | None = None,
    console_log_level: str | None = None,
    follow_event_timing: bool | None = None,
    replay_speed_multiplier: float | None = None,
    max_replay_sleep_seconds: float | None = None,
    replay_delay_ms: int | None = None,
    zigbee_gateway_overrides: dict[str, Any] | None = None,
    edf_service_sdk_overrides: dict[str, Any] | None = None,
    fhe_cloud_overrides: dict[str, Any] | None = None,
) -> PipelineContext:
    config = load_pipeline_config(config_path)
    if input_source is not None:
        config["input_source"] = input_source
        config["gateway_stream"]["zigbee_source"] = _zigbee_mode_for_input_source(input_source)
    elif "input_source" not in config:
        config["input_source"] = _input_source_for_zigbee_mode(
            config["gateway_stream"].get("zigbee_source", "replay")
        )
    else:
        config["gateway_stream"]["zigbee_source"] = _zigbee_mode_for_input_source(
            config["input_source"]
        )
    if interval_seconds is not None:
        config["pipeline"]["interval_seconds"] = interval_seconds
    if zigbee_source is not None:
        config["gateway_stream"]["zigbee_source"] = zigbee_source
        config["input_source"] = _input_source_for_zigbee_mode(zigbee_source)
    if console_log_level is not None:
        config["gateway_stream"]["console_log_level"] = console_log_level
    if follow_event_timing is not None:
        config["gateway_stream"]["follow_event_timing"] = follow_event_timing
    if replay_speed_multiplier is not None:
        config["gateway_stream"]["replay_speed_multiplier"] = replay_speed_multiplier
    if max_replay_sleep_seconds is not None:
        config["gateway_stream"]["max_replay_sleep_seconds"] = max_replay_sleep_seconds
    if replay_delay_ms is not None:
        config["gateway_stream"]["replay_delay_ms"] = replay_delay_ms
    if zigbee_gateway_overrides:
        config["zigbee_gateway"].update(
            {k: v for k, v in zigbee_gateway_overrides.items() if v is not None}
        )
    if edf_service_sdk_overrides:
        config["edf_service_sdk"].update(
            {k: v for k, v in edf_service_sdk_overrides.items() if v is not None}
        )
    if fhe_cloud_overrides:
        fhe_cloud_config = config.setdefault("fhe_cloud", {})
        selected_fhe_tasks = fhe_cloud_overrides.get("enabled_tasks")
        fhe_cloud_config.update(
            {
                k: v
                for k, v in fhe_cloud_overrides.items()
                if v is not None and k != "enabled_tasks"
            }
        )
        select_fhe_tasks(config, selected_fhe_tasks)
    return PipelineContext(
        input_path=input_path,
        output_dir=Path(output_dir),
        config=config,
    )


def build_streaming_observer(context: PipelineContext) -> StreamingObserver:
    log_level = str(context.config["gateway_stream"].get("console_log_level", "none"))
    if log_level == "none":
        return NullStreamingObserver()
    return ConsoleStreamingObserver(log_level=log_level)


def build_gateway_stream_pipeline_for_context(
    context: PipelineContext,
    write_artifacts: bool = True,
) -> GatewayStreamingPipeline:
    observer: StreamingObserver = build_streaming_observer(context)
    stream_source = build_gateway_event_source(context, observer)
    sink: StreamingSink = AppendFileStreamingSink() if write_artifacts else NullStreamingSink()
    stages = build_default_streaming_stages(context.config)
    return GatewayStreamingPipeline(
        source=stream_source,
        sink=sink,
        stages=stages,
        observer=observer,
    )


def build_gateway_event_source(
    context: PipelineContext,
    observer: StreamingObserver | None = None,
) -> StreamingEventSource:
    gateway_config = context.config["gateway_stream"]
    zigbee_mode = gateway_config.get("zigbee_source", "replay")
    stream_source: StreamingEventSource = build_zigbee_stream_source(zigbee_mode)
    if bool(gateway_config.get("follow_event_timing", False)):
        stream_source = TimestampPacedStreamSource(
            stream_source,
            observer=observer or NullStreamingObserver(),
        )
    return stream_source


def run_gateway_stream_replay(
    input_path: Path | str,
    output_dir: Path | str,
    config_path: Path | str | None = None,
    interval_seconds: int | None = None,
    console_log_level: str | None = None,
    follow_event_timing: bool = False,
    replay_speed_multiplier: float | None = None,
    max_replay_sleep_seconds: float | None = None,
    replay_delay_ms: int | None = None,
    fhe_cloud_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = build_gateway_stream_context(
        input_path=input_path,
        output_dir=output_dir,
        config_path=config_path,
        interval_seconds=interval_seconds,
        input_source="csv",
        zigbee_source="replay",
        console_log_level=console_log_level,
        follow_event_timing=follow_event_timing,
        replay_speed_multiplier=replay_speed_multiplier,
        max_replay_sleep_seconds=max_replay_sleep_seconds,
        replay_delay_ms=replay_delay_ms,
        fhe_cloud_overrides=fhe_cloud_overrides,
    )
    summary = build_gateway_stream_pipeline_for_context(context, write_artifacts=True).run(context)
    return summary.to_record()


def run_gateway_zigbee_mqtt_live(
    output_dir: Path | str,
    config_path: Path | str | None = None,
    interval_seconds: int | None = None,
    zigbee_host: str | None = None,
    zigbee_port: int | None = None,
    zigbee_topic: str | None = None,
    zigbee_topic_prefix: str | None = None,
    zigbee_listen_seconds: int | None = None,
    zigbee_username: str | None = None,
    zigbee_password: str | None = None,
    zigbee_client_id: str | None = None,
    mqtt_tls_enabled: bool | None = None,
    mqtt_ca_cert_path: str | None = None,
    mqtt_client_cert_path: str | None = None,
    mqtt_client_key_path: str | None = None,
    mqtt_tls_insecure: bool | None = None,
    console_log_level: str | None = None,
    fhe_cloud_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = build_gateway_stream_context(
        input_path=f"zigbee-mqtt://{zigbee_host or 'localhost'}",
        output_dir=output_dir,
        config_path=config_path,
        interval_seconds=interval_seconds,
        input_source="zigbee_mqtt",
        zigbee_source="zigbee_mqtt",
        console_log_level=console_log_level,
        zigbee_gateway_overrides={
            "host": zigbee_host,
            "port": zigbee_port,
            "topic": zigbee_topic,
            "topic_prefix": zigbee_topic_prefix,
            "listen_seconds": zigbee_listen_seconds,
            "username": zigbee_username,
            "password": zigbee_password,
            "client_id": zigbee_client_id,
            "tls_enabled": mqtt_tls_enabled,
            "ca_cert_path": mqtt_ca_cert_path,
            "client_cert_path": mqtt_client_cert_path,
            "client_key_path": mqtt_client_key_path,
            "tls_insecure": mqtt_tls_insecure,
        },
        fhe_cloud_overrides=fhe_cloud_overrides,
    )
    context.config["_stream_mode_override"] = "mqtt_live"
    summary = build_gateway_stream_pipeline_for_context(context, write_artifacts=True).run(context)
    return summary.to_record()


def run_gateway_edf_sdk_live(
    output_dir: Path | str,
    config_path: Path | str | None = None,
    interval_seconds: int | None = None,
    edf_streams: list[str] | str | None = None,
    edf_listen_seconds: int | None = None,
    edf_default_device_id: str | None = None,
    edf_default_device_role: str | None = None,
    edf_stream_field_map: dict[str, str] | None = None,
    edf_register_unknown_devices: bool | None = None,
    edf_include_unsupported_fields: bool | None = None,
    console_log_level: str | None = None,
    fhe_cloud_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = build_gateway_stream_context(
        input_path="edf-service-sdk://DeviceApi.from_env",
        output_dir=output_dir,
        config_path=config_path,
        interval_seconds=interval_seconds,
        input_source="edf_service_sdk",
        zigbee_source="edf_service_sdk",
        console_log_level=console_log_level,
        edf_service_sdk_overrides={
            "streams": edf_streams,
            "listen_seconds": edf_listen_seconds,
            "default_device_id": edf_default_device_id,
            "default_device_role": edf_default_device_role,
            "stream_field_map": edf_stream_field_map,
            "register_unknown_devices": edf_register_unknown_devices,
            "include_unsupported_fields": edf_include_unsupported_fields,
        },
        fhe_cloud_overrides=fhe_cloud_overrides,
    )
    context.config["_stream_mode_override"] = "edf_sdk_live"
    summary = build_gateway_stream_pipeline_for_context(context, write_artifacts=True).run(context)
    return summary.to_record()


def run_gateway_zigbee_mqtt_capture(
    capture_output_path: Path | str,
    output_dir: Path | str,
    config_path: Path | str | None = None,
    interval_seconds: int | None = None,
    zigbee_host: str | None = None,
    zigbee_port: int | None = None,
    zigbee_topic: str | None = None,
    zigbee_topic_prefix: str | None = None,
    zigbee_listen_seconds: int | None = None,
    zigbee_username: str | None = None,
    zigbee_password: str | None = None,
    zigbee_client_id: str | None = None,
    mqtt_tls_enabled: bool | None = None,
    mqtt_ca_cert_path: str | None = None,
    mqtt_client_cert_path: str | None = None,
    mqtt_client_key_path: str | None = None,
    mqtt_tls_insecure: bool | None = None,
    console_log_level: str | None = None,
    run_pipeline: bool = False,
    append: bool = False,
    flush_every_records: int = 1,
    fhe_cloud_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = build_gateway_stream_context(
        input_path=f"zigbee-mqtt://{zigbee_host or 'localhost'}",
        output_dir=output_dir,
        config_path=config_path,
        interval_seconds=interval_seconds,
        input_source="zigbee_mqtt",
        zigbee_source="zigbee_mqtt",
        console_log_level=console_log_level,
        zigbee_gateway_overrides={
            "host": zigbee_host,
            "port": zigbee_port,
            "topic": zigbee_topic,
            "topic_prefix": zigbee_topic_prefix,
            "listen_seconds": zigbee_listen_seconds,
            "username": zigbee_username,
            "password": zigbee_password,
            "client_id": zigbee_client_id,
            "tls_enabled": mqtt_tls_enabled,
            "ca_cert_path": mqtt_ca_cert_path,
            "client_cert_path": mqtt_client_cert_path,
            "client_key_path": mqtt_client_key_path,
            "tls_insecure": mqtt_tls_insecure,
        },
        fhe_cloud_overrides=fhe_cloud_overrides,
    )
    observer = build_streaming_observer(context)
    source = CaptureRawEventSource(
        build_gateway_event_source(context, observer),
        capture_path=capture_output_path,
        append=append,
        flush_every_records=flush_every_records,
    )

    if run_pipeline:
        context.config["_stream_mode_override"] = "mqtt_capture_with_pipeline"
        pipeline = GatewayStreamingPipeline(
            source=source,
            sink=AppendFileStreamingSink(),
            stages=build_default_streaming_stages(context.config),
            observer=observer,
        )
        summary = pipeline.run(context).to_record()
        summary["capture_path"] = str(Path(capture_output_path))
        summary["capture_append"] = append
        _annotate_live_stream_summary(summary, context, "mqtt_capture_with_pipeline")
        return summary

    raw_event_count = 0
    first_timestamp = None
    last_timestamp = None
    devices: Counter[str] = Counter()
    for event in source.stream(context):
        raw_event_count += 1
        first_timestamp = first_timestamp or event.timestamp
        last_timestamp = event.timestamp
        devices[event.device] += 1

    summary = {
        "capture_path": str(Path(capture_output_path)),
        "capture_append": append,
        "output_dir": str(Path(output_dir).resolve()),
        "input_path": str(context.input_path),
        "raw_event_count": raw_event_count,
        "time_range": {
            "start": first_timestamp.isoformat() if first_timestamp else None,
            "end": last_timestamp.isoformat() if last_timestamp else None,
        },
        "devices": dict(devices),
        "pipeline_ran": False,
    }
    _annotate_live_stream_summary(summary, context, "mqtt_capture")
    return summary


def _annotate_live_stream_summary(
    summary: dict[str, Any],
    context: PipelineContext,
    stream_mode: str,
) -> None:
    listen_seconds = int(context.config["zigbee_gateway"].get("listen_seconds", 0))
    summary["stream_mode"] = stream_mode
    summary["mqtt_listen_seconds"] = listen_seconds
    if listen_seconds < 0:
        summary["stop_reason"] = "manual_stop_or_external_interruption"
    else:
        summary["stop_reason"] = "configured_listen_window_elapsed"


def _zigbee_mode_for_input_source(input_source: str) -> str:
    if input_source == "zigbee_mqtt":
        return "zigbee_mqtt"
    if input_source in {"edf_service_sdk", "edf_sdk"}:
        return "edf_service_sdk"
    return "replay"


def _input_source_for_zigbee_mode(zigbee_mode: str) -> str:
    if zigbee_mode == "edf_service_sdk":
        return "edf_service_sdk"
    if _is_live_zigbee_mode(zigbee_mode):
        return "zigbee_mqtt"
    return "csv"


def _is_live_zigbee_mode(zigbee_mode: str) -> bool:
    return zigbee_mode in {"zigbee_mqtt", "edf_service_sdk"}


__all__ = [
    "ConsoleStreamingObserver",
    "NullStreamingObserver",
    "GatewayStreamingPipeline",
    "ZigbeeCsvReplaySource",
    "CaptureRawEventSource",
    "ZigbeeMqttSensorSource",
    "EdfServiceSdkEventSource",
    "TimestampPacedStreamSource",
    "build_gateway_stream_context",
    "build_gateway_event_source",
    "build_gateway_stream_pipeline_for_context",
    "build_streaming_observer",
    "run_gateway_stream_replay",
    "run_gateway_zigbee_mqtt_live",
    "run_gateway_edf_sdk_live",
    "run_gateway_zigbee_mqtt_capture",
]
