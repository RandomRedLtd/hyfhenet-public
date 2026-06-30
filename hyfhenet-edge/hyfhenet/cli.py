from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from .runtime.benchmarking import run_gateway_replay_benchmark
from .runtime.stream import (
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
            _print_mqtt_runtime_summary(summary)
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
    return overrides


def _optional_path_arg(value: Path | None) -> str | None:
    return str(value) if value is not None else None


def _print_stream_summary(summary: dict[str, Any]) -> None:
    print(f"Gateway stream complete: {summary['output_dir']}")
    print(f"Source: {summary['input_path']}")
    _print_mqtt_runtime_summary(summary)
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
    print(f"Mean forecast MAE W: {totals.get('mean_forecast_mae_w')}")
    print(f"Cloud/FHE ok rate: {totals.get('cloud_fhe_ok_rate')}")


def _print_mqtt_runtime_summary(summary: dict[str, Any]) -> None:
    if not summary.get("stream_mode"):
        return
    print(f"Stream mode: {summary['stream_mode']}")
    listen_seconds = int(summary.get("mqtt_listen_seconds", 0))
    if listen_seconds < 0:
        print("MQTT listen window: continuous until Ctrl+C or process stop")
    else:
        print(f"MQTT listen window: {listen_seconds} seconds")
    if summary.get("stop_reason") == "configured_listen_window_elapsed":
        print(
            "Stop reason: configured listen window elapsed "
            "(use --continuous for an unbounded live run)"
        )
    elif summary.get("stop_reason"):
        print(f"Stop reason: {summary['stop_reason']}")
