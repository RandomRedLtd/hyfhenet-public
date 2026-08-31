from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from .runtime.benchmarking import run_gateway_replay_benchmark
from .runtime.stream import (
    run_gateway_edf_sdk_live,
    run_gateway_stream_replay,
    run_gateway_zigbee_mqtt_capture,
    run_gateway_zigbee_mqtt_live,
)
from .training.edge_forecast import train_edge_short_term_load_forecast
from scripts.prepare_edge_report import add_report_arguments


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="HyFHE-Net Pilot 1.2 streaming edge pipeline."
    )
    subparsers = parser.add_subparsers(dest="command")

    replay = subparsers.add_parser(
        "stream-replay",
        help="Replay the captured Zigbee CSV through the gateway streaming pipeline.",
    )
    replay.add_argument("--input", type=Path, default=Path("data/zigbee_mqtt_capture.csv"))
    replay.add_argument("--output", type=Path, default=Path("artifacts/gateway_stream"))
    replay.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    replay.add_argument("--interval-seconds", type=int, default=None)
    replay.add_argument("--replay-delay-ms", type=int, default=None)
    replay.add_argument(
        "--replay-speed",
        type=float,
        default=1.0,
        help="Event-time replay speed. 1.0 is real time, 60.0 is 60x faster.",
    )
    replay.add_argument("--max-replay-sleep-seconds", type=float, default=None)
    replay.add_argument(
        "--as-fast-as-possible",
        action="store_true",
        help="Disable event-time pacing.",
    )
    _add_fhe_cloud_args(replay)
    _add_console_args(replay)

    mqtt_live = subparsers.add_parser(
        "mqtt-live",
        help="Subscribe to a local Zigbee MQTT gateway and run the same edge pipeline.",
    )
    mqtt_live.add_argument("--output", type=Path, default=Path("artifacts/gateway_mqtt_live"))
    mqtt_live.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    mqtt_live.add_argument("--interval-seconds", type=int, default=None)
    mqtt_live.add_argument("--zigbee-host", default=None)
    mqtt_live.add_argument("--zigbee-port", type=int, default=None)
    mqtt_live.add_argument("--zigbee-topic", default=None)
    mqtt_live.add_argument("--zigbee-topic-prefix", default=None)
    mqtt_live.add_argument("--zigbee-listen-seconds", type=int, default=None)
    mqtt_live.add_argument("--zigbee-username", default=None)
    mqtt_live.add_argument("--zigbee-password", default=None)
    mqtt_live.add_argument("--zigbee-client-id", default=None)
    mqtt_live.add_argument(
        "--continuous",
        action="store_true",
        help="Run until interrupted instead of stopping after the configured MQTT listen window.",
    )
    _add_mqtt_security_args(mqtt_live)
    _add_fhe_cloud_args(mqtt_live)
    _add_console_args(mqtt_live)

    mqtt_capture = subparsers.add_parser(
        "mqtt-capture",
        help="Capture Zigbee MQTT telemetry into replay-compatible CSV, optionally running the edge pipeline.",
    )
    mqtt_capture.add_argument(
        "--capture-output",
        type=Path,
        default=Path("artifacts/zigbee_mqtt_capture.csv"),
        help="Replay-compatible CSV file to write.",
    )
    mqtt_capture.add_argument("--output", type=Path, default=Path("artifacts/gateway_mqtt_capture"))
    mqtt_capture.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    mqtt_capture.add_argument("--interval-seconds", type=int, default=None)
    mqtt_capture.add_argument("--zigbee-host", default=None)
    mqtt_capture.add_argument("--zigbee-port", type=int, default=None)
    mqtt_capture.add_argument("--zigbee-topic", default=None)
    mqtt_capture.add_argument("--zigbee-topic-prefix", default=None)
    mqtt_capture.add_argument("--zigbee-listen-seconds", type=int, default=None)
    mqtt_capture.add_argument("--zigbee-username", default=None)
    mqtt_capture.add_argument("--zigbee-password", default=None)
    mqtt_capture.add_argument("--zigbee-client-id", default=None)
    mqtt_capture.add_argument(
        "--continuous",
        action="store_true",
        help="Capture until interrupted instead of stopping after the configured MQTT listen window.",
    )
    mqtt_capture.add_argument(
        "--run-pipeline",
        action="store_true",
        help="Run the edge analytics pipeline while writing the capture CSV.",
    )
    mqtt_capture.add_argument(
        "--append",
        action="store_true",
        help="Append to the capture CSV instead of overwriting it.",
    )
    mqtt_capture.add_argument("--capture-flush-every-records", type=int, default=1)
    _add_mqtt_security_args(mqtt_capture)
    _add_fhe_cloud_args(mqtt_capture)
    _add_console_args(mqtt_capture)

    edf_sdk_live = subparsers.add_parser(
        "edf-sdk-live",
        help="Consume EDF service SDK DeviceApi events and run the edge pipeline.",
    )
    edf_sdk_live.add_argument("--output", type=Path, default=Path("artifacts/gateway_edf_sdk_live"))
    edf_sdk_live.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    edf_sdk_live.add_argument("--interval-seconds", type=int, default=None)
    edf_sdk_live.add_argument(
        "--edf-streams",
        default=None,
        help="Comma-separated DeviceApi streams. Defaults to configured EDF SDK streams.",
    )
    edf_sdk_live.add_argument("--edf-listen-seconds", type=int, default=None)
    edf_sdk_live.add_argument("--edf-default-device-id", default=None)
    edf_sdk_live.add_argument(
        "--edf-default-device-role",
        choices=["smart_plug", "household_meter", "environment_sensor", "unknown_device"],
        default=None,
        help="Role assigned to SDK devices not present in the pipeline config.",
    )
    edf_sdk_live.add_argument(
        "--edf-stream-field-map",
        default=None,
        help="Comma-separated stream=field overrides, for example POWER=household_power_w.",
    )
    edf_sdk_live.add_argument(
        "--edf-include-unsupported-fields",
        action="store_true",
        default=None,
        help="Forward SDK stream fields not listed in the configured supported fields.",
    )
    edf_sdk_live.add_argument(
        "--edf-no-register-unknown-devices",
        action="store_true",
        default=None,
        help="Do not add unseen SDK device IDs to the runtime device-role map.",
    )
    edf_sdk_live.add_argument(
        "--continuous",
        action="store_true",
        help="Run until interrupted instead of stopping after the configured SDK listen window.",
    )
    _add_fhe_cloud_args(edf_sdk_live)
    _add_console_args(edf_sdk_live)

    benchmark = subparsers.add_parser(
        "benchmark-pipeline",
        help="Run repeated replay passes and write latency/count CSV output.",
    )
    benchmark.add_argument("--input", type=Path, default=Path("data/zigbee_mqtt_capture.csv"))
    benchmark.add_argument("--output", type=Path, default=Path("artifacts/edge_benchmark"))
    benchmark.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    benchmark.add_argument("--runs", type=int, default=4)
    benchmark.add_argument("--interval-seconds", type=int, default=30)
    _add_fhe_cloud_args(benchmark)
    _add_console_args(benchmark)

    train_forecast = subparsers.add_parser(
        "train-edge-forecast",
        help="Train and save the local Ridge short-term load forecasting model.",
    )
    train_forecast.add_argument("--input", type=Path, default=Path("data/zigbee_mqtt_capture.csv"))
    train_forecast.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    train_forecast.add_argument(
        "--output-model",
        type=Path,
        default=Path("models/edge_load_forecast_ridge.json"),
    )
    train_forecast.add_argument(
        "--output-dataset",
        type=Path,
        default=Path("data/edge_load_forecast_training_dataset.csv"),
        help="Save supervised feature/target/prediction rows for other model experiments.",
    )
    train_forecast.add_argument(
        "--output-report",
        type=Path,
        default=Path("models/edge_load_forecast_training_report.md"),
        help="Save the model training and evaluation report.",
    )
    train_forecast.add_argument("--horizon-minutes", type=int, default=1)
    train_forecast.add_argument(
        "--alpha",
        type=float,
        default=300.0,
        help="Ridge regularization strength. The current sensor-only capture was tuned at 300.",
    )
    train_forecast.add_argument("--train-fraction", type=float, default=0.7)

    report = subparsers.add_parser(
        "prepare-edge-report",
        help="Generate a device-specific report package with benchmarks and videos.",
    )
    add_report_arguments(report)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    effective_argv = list(sys.argv[1:] if argv is None else argv)
    if not effective_argv:
        effective_argv = ["stream-replay"]
    args = parser.parse_args(effective_argv)

    if args.command == "stream-replay":
        summary = run_gateway_stream_replay(
            input_path=args.input,
            output_dir=args.output,
            config_path=args.config,
            interval_seconds=args.interval_seconds,
            console_log_level=args.log_level,
            follow_event_timing=not args.as_fast_as_possible,
            replay_speed_multiplier=None if args.as_fast_as_possible else args.replay_speed,
            max_replay_sleep_seconds=args.max_replay_sleep_seconds,
            replay_delay_ms=args.replay_delay_ms,
            fhe_cloud_overrides=_fhe_cloud_overrides_from_args(args),
        )
        _print_stream_summary(summary)
        if args.as_fast_as_possible:
            print("Replay timing: disabled")
        else:
            print(f"Replay timing: event_time at {args.replay_speed}x")
        return 0

    if args.command == "mqtt-live":
        summary = run_gateway_zigbee_mqtt_live(
            output_dir=args.output,
            config_path=args.config,
            interval_seconds=args.interval_seconds,
            zigbee_host=args.zigbee_host,
            zigbee_port=args.zigbee_port,
            zigbee_topic=args.zigbee_topic,
            zigbee_topic_prefix=args.zigbee_topic_prefix,
            zigbee_listen_seconds=-1 if args.continuous else args.zigbee_listen_seconds,
            zigbee_username=args.zigbee_username,
            zigbee_password=args.zigbee_password,
            zigbee_client_id=args.zigbee_client_id,
            mqtt_tls_enabled=args.mqtt_tls,
            mqtt_ca_cert_path=_optional_path_arg(args.mqtt_ca_cert),
            mqtt_client_cert_path=_optional_path_arg(args.mqtt_client_cert),
            mqtt_client_key_path=_optional_path_arg(args.mqtt_client_key),
            mqtt_tls_insecure=args.mqtt_tls_insecure,
            console_log_level=args.log_level,
            fhe_cloud_overrides=_fhe_cloud_overrides_from_args(args),
        )
        _print_stream_summary(summary)
        return 0

    if args.command == "edf-sdk-live":
        summary = run_gateway_edf_sdk_live(
            output_dir=args.output,
            config_path=args.config,
            interval_seconds=args.interval_seconds,
            edf_streams=_optional_csv_arg(args.edf_streams),
            edf_listen_seconds=-1 if args.continuous else args.edf_listen_seconds,
            edf_default_device_id=args.edf_default_device_id,
            edf_default_device_role=args.edf_default_device_role,
            edf_stream_field_map=_optional_mapping_arg(args.edf_stream_field_map),
            edf_register_unknown_devices=(
                False if args.edf_no_register_unknown_devices else None
            ),
            edf_include_unsupported_fields=args.edf_include_unsupported_fields,
            console_log_level=args.log_level,
            fhe_cloud_overrides=_fhe_cloud_overrides_from_args(args),
        )
        _print_stream_summary(summary)
        return 0

    if args.command == "mqtt-capture":
        summary = run_gateway_zigbee_mqtt_capture(
            capture_output_path=args.capture_output,
            output_dir=args.output,
            config_path=args.config,
            interval_seconds=args.interval_seconds,
            zigbee_host=args.zigbee_host,
            zigbee_port=args.zigbee_port,
            zigbee_topic=args.zigbee_topic,
            zigbee_topic_prefix=args.zigbee_topic_prefix,
            zigbee_listen_seconds=-1 if args.continuous else args.zigbee_listen_seconds,
            zigbee_username=args.zigbee_username,
            zigbee_password=args.zigbee_password,
            zigbee_client_id=args.zigbee_client_id,
            mqtt_tls_enabled=args.mqtt_tls,
            mqtt_ca_cert_path=_optional_path_arg(args.mqtt_ca_cert),
            mqtt_client_cert_path=_optional_path_arg(args.mqtt_client_cert),
            mqtt_client_key_path=_optional_path_arg(args.mqtt_client_key),
            mqtt_tls_insecure=args.mqtt_tls_insecure,
            console_log_level=args.log_level,
            run_pipeline=args.run_pipeline,
            append=args.append,
            flush_every_records=args.capture_flush_every_records,
            fhe_cloud_overrides=_fhe_cloud_overrides_from_args(args),
        )
        if args.run_pipeline:
            _print_stream_summary(summary)
        else:
            print(f"MQTT capture complete: {summary['capture_path']}")
            print(f"Source: {summary['input_path']}")
            _print_live_runtime_summary(summary)
            print(f"Raw events: {summary['raw_event_count']}")
            print(f"Coverage: {summary['time_range']['start']} -> {summary['time_range']['end']}")
            print(f"Devices: {summary['devices']}")
        print(f"Capture append: {summary['capture_append']}")
        return 0

    if args.command == "benchmark-pipeline":
        summary = run_gateway_replay_benchmark(
            input_path=args.input,
            output_dir=args.output,
            config_path=args.config,
            runs=args.runs,
            interval_seconds=args.interval_seconds,
            console_log_level=args.log_level,
            fhe_cloud_overrides=_fhe_cloud_overrides_from_args(args),
        )
        _print_benchmark_summary(summary)
        return 0

    if args.command == "train-edge-forecast":
        artifact = train_edge_short_term_load_forecast(
            input_path=args.input,
            config_path=args.config,
            output_model_path=args.output_model,
            output_dataset_path=args.output_dataset,
            output_report_path=args.output_report,
            horizon_minutes=args.horizon_minutes,
            alpha=args.alpha,
            train_fraction=args.train_fraction,
        )
        metrics = artifact["metrics"]
        summary = artifact["training_summary"]
        print(f"Saved edge forecast model: {args.output_model}")
        print(
            "Training examples: "
            f"{summary['example_count']} "
            f"(train={summary['train_count']}, test={summary['test_count']})"
        )
        print(
            "Evaluation: "
            f"MAE={metrics['mae_w']} W "
            f"RMSE={metrics['rmse_w']} W "
            f"R2={metrics['r2']}"
        )
        print(f"Saved supervised dataset: {args.output_dataset}")
        print(f"Saved training report: {args.output_report}")
        return 0

    if args.command == "prepare-edge-report":
        from scripts.prepare_edge_report import run as run_prepare_edge_report

        return run_prepare_edge_report(args)

    parser.print_help()
    return 1


def _add_console_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        choices=["none", "progress", "verbose"],
        default="progress",
        help="Console logging level.",
    )


def _add_fhe_cloud_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--fhe-api-url",
        default=None,
        help="Remote FHE API base URL. Defaults to HYFHENET_FHE_API_URL or API_URL.",
    )
    parser.add_argument(
        "--fhe-api-key",
        default=None,
        help="Remote FHE API key. Defaults to HYFHENET_FHE_API_KEY or API_KEY.",
    )
    parser.add_argument(
        "--fhe-cache-dir",
        type=Path,
        default=None,
        help="Local cache for FHE keys and downloaded client model files.",
    )
    parser.add_argument(
        "--fhe-client-cert",
        type=Path,
        default=None,
        help="Client certificate path for mTLS to the remote FHE API.",
    )
    parser.add_argument(
        "--fhe-client-key",
        type=Path,
        default=None,
        help="Client private-key path for mTLS to the remote FHE API.",
    )
    parser.add_argument(
        "--fhe-ca-bundle",
        type=Path,
        default=None,
        help="CA bundle path used to verify the remote FHE API certificate.",
    )
    parser.add_argument(
        "--fhe-allow-insecure-http",
        action="store_true",
        default=None,
        help="Allow http:// FHE API URLs for isolated lab testing only.",
    )
    parser.add_argument(
        "--fhe-sample-interval-seconds",
        type=int,
        default=None,
        help="Run remote FHE at most once per this many stream seconds. Set 0 to run every tick.",
    )
    parser.add_argument(
        "--fhe-tasks",
        default=None,
        help="Comma-separated FHE tasks to run: forecast,nilm,cohort, all, or none.",
    )
    parser.add_argument(
        "--fhe-async",
        dest="fhe_async_enabled",
        action="store_true",
        default=None,
        help="Dispatch FHE calls in a background worker so edge ticks do not wait on cloud inference.",
    )
    parser.add_argument(
        "--fhe-sync",
        dest="fhe_async_enabled",
        action="store_false",
        help="Use the default synchronous FHE path.",
    )
    parser.add_argument(
        "--fhe-max-pending-requests",
        type=int,
        default=None,
        help="Maximum queued or running async FHE requests before new samples are skipped.",
    )


def _add_mqtt_security_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--mqtt-tls",
        action="store_true",
        default=None,
        help="Use MQTTS/TLS for the MQTT broker connection.",
    )
    parser.add_argument("--mqtt-ca-cert", type=Path, default=None)
    parser.add_argument("--mqtt-client-cert", type=Path, default=None)
    parser.add_argument("--mqtt-client-key", type=Path, default=None)
    parser.add_argument(
        "--mqtt-tls-insecure",
        action="store_true",
        default=None,
        help="Disable MQTT server certificate verification for lab-only testing.",
    )


def _fhe_cloud_overrides_from_args(args: argparse.Namespace) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if getattr(args, "fhe_api_url", None):
        overrides["api_url"] = args.fhe_api_url
    if getattr(args, "fhe_api_key", None):
        overrides["api_key"] = args.fhe_api_key
    if getattr(args, "fhe_cache_dir", None):
        overrides["cache_dir"] = str(args.fhe_cache_dir)
    if getattr(args, "fhe_client_cert", None):
        overrides["client_cert_path"] = str(args.fhe_client_cert)
    if getattr(args, "fhe_client_key", None):
        overrides["client_key_path"] = str(args.fhe_client_key)
    if getattr(args, "fhe_ca_bundle", None):
        overrides["ca_bundle_path"] = str(args.fhe_ca_bundle)
    if getattr(args, "fhe_allow_insecure_http", None):
        overrides["allow_insecure_http"] = True
    if getattr(args, "fhe_sample_interval_seconds", None) is not None:
        overrides["sample_interval_seconds"] = args.fhe_sample_interval_seconds
    if getattr(args, "fhe_tasks", None):
        overrides["enabled_tasks"] = args.fhe_tasks
    if getattr(args, "fhe_async_enabled", None) is not None:
        overrides["async_enabled"] = args.fhe_async_enabled
    if getattr(args, "fhe_max_pending_requests", None) is not None:
        overrides["max_pending_requests"] = args.fhe_max_pending_requests
    return overrides


def _optional_path_arg(value: Path | None) -> str | None:
    return str(value) if value is not None else None


def _optional_csv_arg(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _optional_mapping_arg(value: str | None) -> dict[str, str] | None:
    if not value:
        return None
    mapping: dict[str, str] = {}
    for item in value.split(","):
        if not item.strip():
            continue
        separator = "=" if "=" in item else ":"
        if separator not in item:
            raise ValueError(
                f"Invalid mapping item '{item}'. Use stream=field pairs."
            )
        key, mapped_value = item.split(separator, 1)
        key = key.strip()
        mapped_value = mapped_value.strip()
        if key and mapped_value:
            mapping[key] = mapped_value
    return mapping


def _print_stream_summary(summary: dict[str, Any]) -> None:
    print(f"Gateway stream complete: {summary['output_dir']}")
    print(f"Source: {summary['input_path']}")
    _print_live_runtime_summary(summary)
    print(f"Raw events: {summary['raw_event_count']}")
    print(f"Normalized events: {summary['normalized_event_count']}")
    print(f"Aligned snapshots: {summary['snapshot_count']}")
    print(f"Household power source: {summary['household_power_source']}")
    print(f"Quality alerts: {summary['quality_alert_count']}")
    print(f"Feature windows: {summary['feature_window_count']}")
    print(f"Cloud forecast features: {summary['cloud_forecast_feature_count']}")
    print(f"Cloud forecast training examples: {summary['cloud_forecast_training_example_count']}")
    print(f"Load event markers: {summary['load_event_marker_count']}")
    print(f"Edge forecast evaluations: {summary['edge_forecast_evaluation_count']}")
    print(f"Edge service results: {summary['service_result_count']}")
    print(f"Model inputs: {summary['model_input_count']}")
    print(f"Model inference results: {summary['model_result_count']}")
    if summary.get("latency_summary"):
        latency = summary["latency_summary"]
        print(f"Latency samples: {latency['sample_count']}")
        print(
            "Latency avg/p95 total tick ms: "
            f"{latency['avg_total_tick_ms']} / {latency['p95_total_tick_ms']}"
        )
        print(
            "Latency avg/p95 edge local ms: "
            f"{latency.get('avg_edge_local_operations_ms')} / "
            f"{latency.get('p95_edge_local_operations_ms')}"
        )
        print(
            "Latency avg/p95 cloud FHE ms: "
            f"{latency.get('avg_cloud_fhe_operations_ms')} / "
            f"{latency.get('p95_cloud_fhe_operations_ms')}"
        )
    if summary.get("performance_summary"):
        performance = summary["performance_summary"]
        print(
            "Throughput raw events/s: "
            f"{performance['raw_events_per_wall_second']}"
        )
    if summary.get("forecast_quality_summary"):
        quality = summary["forecast_quality_summary"].get("overall")
        if quality:
            print(
                "Forecast MAE/RMSE/R2: "
                f"{quality['mae_w']} / {quality['rmse_w']} / {quality['r2']}"
            )
    print(f"Coverage: {summary['time_range']['start']} -> {summary['time_range']['end']}")


def _print_benchmark_summary(summary: dict[str, Any]) -> None:
    totals = summary["totals"]
    print(f"Benchmark complete: {summary['output_dir']}")
    print(f"Runs: {summary['run_count']}")
    print(f"Benchmark CSV: {summary['benchmark_csv']}")
    print(f"Benchmark JSON: {summary['benchmark_json']}")
    print(f"Total snapshots: {totals['snapshot_count']}")
    print(f"Total model results: {totals['model_result_count']}")
    print(f"Mean avg total tick ms: {totals['mean_avg_total_tick_ms']}")
    print(f"Mean avg edge local ms: {totals.get('mean_avg_edge_local_operations_ms')}")
    print(f"Mean avg cloud FHE ms: {totals.get('mean_avg_cloud_fhe_operations_ms')}")
    print(f"Mean forecast MAE W: {totals.get('mean_forecast_mae_w')}")
    print(f"Cloud/FHE ok rate: {totals.get('cloud_fhe_ok_rate')}")


def _print_live_runtime_summary(summary: dict[str, Any]) -> None:
    if not summary.get("stream_mode"):
        return
    print(f"Stream mode: {summary['stream_mode']}")
    listen_seconds = int(summary.get("mqtt_listen_seconds", 0))
    source_label = "EDF SDK" if str(summary["stream_mode"]).startswith("edf") else "MQTT"
    if listen_seconds < 0:
        print(f"{source_label} listen window: continuous until Ctrl+C or process stop")
    else:
        print(f"{source_label} listen window: {listen_seconds} seconds")
    if summary.get("stop_reason") == "configured_listen_window_elapsed":
        print(
            "Stop reason: configured listen window elapsed "
            "(use --continuous for an unbounded live run)"
        )
    elif summary.get("stop_reason"):
        print(f"Stop reason: {summary['stop_reason']}")
