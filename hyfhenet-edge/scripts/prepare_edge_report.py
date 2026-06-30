from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import platform
import random
import shutil
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hyfhenet.ai.models import MODEL_RESULT_COLUMNS
from hyfhenet.core.interfaces import PipelineContext
from hyfhenet.core.models import (
    FeatureWindow,
    GatewayStreamRunSummary,
    LatencySample,
    ModelInput,
    NormalizedEvent,
    RawTelemetryEvent,
)
from hyfhenet.fhe.features import FORECAST_NUMERIC_FEATURE_COLUMNS
from hyfhenet.ingestion.stream_sources import RAW_TELEMETRY_COLUMNS
from hyfhenet.processing.preprocessing import build_feature_columns
from hyfhenet.processing.stream_stages import build_default_streaming_stages
from hyfhenet.runtime.observers import NullStreamingObserver
from hyfhenet.runtime.orchestrator import GatewayStreamingPipeline
from hyfhenet.runtime.profiling import build_latency_summary
from hyfhenet.runtime.sinks import AppendFileStreamingSink
from hyfhenet.runtime.stream import (
    build_gateway_event_source,
    build_gateway_stream_context,
    build_gateway_stream_pipeline_for_context,
)
from hyfhenet.training.edge_forecast import train_edge_short_term_load_forecast
from hyfhenet.training.linear import fit_ridge_regression, predict_ridge_regression


DEFAULT_OUTPUT = Path("artifacts/edge_report")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
START_TIME = datetime(2026, 4, 29, 13, 0, 0)
CSV_KWARGS = {"newline": "", "encoding": "utf-8"}
TRUTHY_VALUES = {"1", "true", "yes", "on"}
FALSY_VALUES = {"0", "false", "no", "off"}
FHE_MODEL_IDS = {
    "fhe_long_term_load_forecast",
    "fhe_nilm_disaggregation",
    "fhe_cohort_benchmark",
}


@dataclass(frozen=True)
class Device:
    device_id: str
    role: str
    name: str
    location: str
    appliance: str = ""


@dataclass(frozen=True)
class VideoSlide:
    title: str
    lines: list[str]
    footer: str = "HyFHE-Net edge gateway evidence package"
    duration_seconds: float = 6.0
    screen_title: str = ""
    screen_lines: list[str] | None = None


class ReportSink(AppendFileStreamingSink):
    """Default sink plus the few trace files needed for reporting."""

    def __init__(self) -> None:
        super().__init__()
        self._normalized = None
        self._snapshots = None
        self._snapshot_writer = None
        self._features = None
        self._feature_writer = None
        self._model_inputs = None
        self._latency = None
        self._latency_writer = None
        self._cycle = 0

    def open(self, context: PipelineContext) -> None:
        super().open(context)
        self._normalized = (context.output_dir / "normalized_events.jsonl").open(
            "w", encoding="utf-8"
        )
        self._model_inputs = (context.output_dir / "model_inputs.jsonl").open(
            "w", encoding="utf-8"
        )
        self._latency = (context.output_dir / "latency.csv").open("w", **CSV_KWARGS)
        self._latency_writer = csv.DictWriter(
            self._latency,
            fieldnames=["cycle", *latency_columns()],
        )
        self._latency_writer.writeheader()

    def append_normalized_event(
            self,
            raw_event: RawTelemetryEvent,
            normalized_event: NormalizedEvent,
            context: PipelineContext,
    ) -> None:
        if self._normalized is None:
            return
        self._normalized.write(
            json.dumps(
                {
                    "raw": raw_event.to_row(),
                    "normalized": normalized_event.to_record(),
                },
                ensure_ascii=True,
                sort_keys=True,
            )
            + "\n"
        )

    def append_snapshot(self, snapshot: dict[str, Any], context: PipelineContext) -> None:
        if self._snapshot_writer is None:
            self._snapshots = (context.output_dir / "snapshots.csv").open(
                "w", **CSV_KWARGS
            )
            self._snapshot_writer = csv.DictWriter(
                self._snapshots,
                fieldnames=list(snapshot),
                extrasaction="ignore",
            )
            self._snapshot_writer.writeheader()
        self._snapshot_writer.writerow(snapshot)

    def append_feature_window(
            self,
            feature_window: FeatureWindow,
            context: PipelineContext,
    ) -> None:
        if self._feature_writer is None:
            self._features = (context.output_dir / "features.csv").open(
                "w", **CSV_KWARGS
            )
            self._feature_writer = csv.DictWriter(
                self._features,
                fieldnames=build_feature_columns(context.config),
                extrasaction="ignore",
            )
            self._feature_writer.writeheader()
        self._feature_writer.writerow(feature_window.to_record())

    def append_model_input(self, model_input: ModelInput, context: PipelineContext) -> None:
        if self._model_inputs is None:
            return
        self._model_inputs.write(
            json.dumps(model_input.to_record(), ensure_ascii=True, sort_keys=True) + "\n"
        )

    def append_latency_sample(
            self,
            latency_sample: LatencySample,
            context: PipelineContext,
    ) -> None:
        if self._latency_writer is None:
            return
        self._cycle += 1
        self._latency_writer.writerow({"cycle": self._cycle, **latency_sample.to_record()})

    def close(self, summary: GatewayStreamRunSummary, context: PipelineContext) -> None:
        try:
            super().close(summary, context)
        finally:
            for handle in (
                    self._normalized,
                    self._snapshots,
                    self._features,
                    self._model_inputs,
                    self._latency,
            ):
                if handle is not None:
                    handle.close()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    return run(args)


def run(args: argparse.Namespace) -> int:
    out = resolve(args.output)
    input_path = resolve(args.input)
    config_path = resolve(args.config)
    create_report_dirs(out)
    report_progress("started", output=out)
    write_report_status(out, "started", output=str(out))

    if args.source == "mock":
        report_progress("building mock report input", cycles=max(args.cycles, 100))
        write_report_status(out, "building mock report input")
        capture_path, report_config_path = build_mock_report_input(
            base_config=load_json(config_path),
            out=out,
            cycles=max(args.cycles, 100),
            plug_count=max(args.plugs, 2),
            interval_seconds=args.interval_seconds,
            seed=args.seed,
        )
    else:
        report_progress("copying source capture", input=input_path)
        write_report_status(out, "copying source capture", input=str(input_path))
        capture_path = copy_source_capture(input_path, out / "data" / "received_telemetry.csv")
        report_config_path = copy_json(config_path, out / "data" / "run_config.json")

    model_path = out / "model" / "edge_ridge_forecast.json"
    training_dataset = out / "model" / "training_dataset.csv"
    training_report = out / "model" / "training_report.md"
    training_config_path = out / "data" / "training_config.json"
    run_config_path = out / "data" / "run_config.json"
    reuse_fhe_report = resolve_reuse_fhe_report(args.reuse_fhe_report)

    config = with_report_fhe_sampling(
        load_json(report_config_path),
        args.fhe_sample_interval_seconds,
    )
    if reuse_fhe_report is not None:
        config = with_reused_fhe_report(config, reuse_fhe_report)
    if args.skip_training:
        existing_model_path = resolve_edge_model_path(config, args.edge_model_path)
        report_progress("using existing edge forecast model", model=existing_model_path)
        write_report_status(
            out,
            "using existing edge forecast model",
            model=str(existing_model_path),
        )
        model = copy_existing_edge_forecast_model(
            source=existing_model_path,
            destination=model_path,
            training_dataset_path=training_dataset,
            report_path=training_report,
        )
        write_json(training_config_path, configured_for_runtime(config, model_path))
        comparison = None
    else:
        report_progress("training edge forecast model")
        write_report_status(out, "training edge forecast model")
        write_json(
            training_config_path,
            configured_for_training(config, model_path, args.training_horizon_minutes),
        )
        model = train_edge_short_term_load_forecast(
            input_path=capture_path,
            config_path=training_config_path,
            output_model_path=model_path,
            output_dataset_path=training_dataset,
            output_report_path=training_report,
            horizon_minutes=args.training_horizon_minutes,
            alpha=args.ridge_alpha,
            train_fraction=args.train_fraction,
        )
        report_progress("comparing local model candidates")
        write_report_status(out, "comparing local model candidates")
        comparison = compare_models(
            dataset_path=training_dataset,
            out=out / "model",
            selected_alpha=args.ridge_alpha,
        )
    write_json(run_config_path, configured_for_runtime(config, model_path))

    report_progress(
        "replaying report capture",
        interval_seconds=args.interval_seconds,
        fhe_sample_interval_seconds=args.fhe_sample_interval_seconds,
        reuse_fhe_report=str(reuse_fhe_report) if reuse_fhe_report else None,
    )
    write_report_status(
        out,
        "replaying report capture",
        interval_seconds=args.interval_seconds,
        fhe_sample_interval_seconds=args.fhe_sample_interval_seconds,
        reuse_fhe_report=str(reuse_fhe_report) if reuse_fhe_report else None,
    )
    run_summary = run_report_replay(
        capture_path=capture_path,
        config_path=run_config_path,
        out=out / "run",
        interval_seconds=args.interval_seconds,
    )
    if reuse_fhe_report is not None:
        report_progress("reusing FHE report evidence", source=reuse_fhe_report)
        write_report_status(out, "reusing FHE report evidence", source=str(reuse_fhe_report))
        apply_reused_fhe_report(out / "run", run_summary, reuse_fhe_report)
    write_model_results_csv(out / "run" / "edge_results.jsonl", out / "run" / "model_results.csv")
    refresh_run_summary_artifacts(out / "run", run_summary)
    if comparison is None:
        comparison = write_existing_model_inference_summary(out / "model", model, run_summary)

    report_progress("running summary benchmark", runs=args.benchmark_runs)
    write_report_status(out, "running summary benchmark", runs=args.benchmark_runs)
    benchmark = run_summary_benchmark(
        capture_path=capture_path,
        config_path=run_config_path,
        out=out / "benchmark",
        runs=args.benchmark_runs,
        interval_seconds=args.interval_seconds,
    )
    report_progress("writing report docs", videos=args.videos)
    write_report_status(out, "writing report docs", videos=args.videos)
    manifest = write_report_docs(
        out=out,
        source=args.source,
        capture_path=capture_path,
        config_path=run_config_path,
        run_summary=run_summary,
        benchmark=benchmark,
        model=model,
        comparison=comparison,
        videos_enabled=args.videos,
        ffmpeg_bin=args.ffmpeg_bin,
        video_width=args.video_width,
        video_height=args.video_height,
        video_fps=args.video_fps,
    )

    sample_count = (run_summary.get("latency_summary") or {}).get("sample_count", 0)
    if sample_count < 100:
        write_report_status(out, "failed", latency_samples=sample_count)
        print(f"Report run produced only {sample_count} latency samples.", file=sys.stderr)
        return 2

    write_report_status(out, "complete", latency_samples=sample_count, manifest=str(manifest))
    print(f"Report ready: {out}")
    print(f"HTML: {out / 'showcase.html'}")
    print(f"Latency samples: {sample_count}")
    for video in load_json(manifest).get("videos", []):
        print(f"Video: {video['path']}")
    print(f"Manifest: {manifest}")
    return 0


def parser() -> argparse.ArgumentParser:
    arg_parser = argparse.ArgumentParser(description="Prepare edge gateway report evidence.")
    add_report_arguments(arg_parser)
    return arg_parser


def add_report_arguments(arg_parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    arg_parser.add_argument("--source", choices=["mock", "input"], default="input")
    arg_parser.add_argument("--input", type=Path, default=Path("data/zigbee_mqtt_capture.csv"))
    arg_parser.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    arg_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    arg_parser.add_argument("--cycles", type=int, default=120)
    arg_parser.add_argument("--plugs", type=int, default=6)
    arg_parser.add_argument("--interval-seconds", type=int, default=5)
    arg_parser.add_argument(
        "--fhe-sample-interval-seconds",
        type=int,
        default=env_int("HYFHENET_REPORT_FHE_SAMPLE_INTERVAL_SECONDS"),
        help=(
            "Run remote FHE at most once per this many report seconds while all "
            "other stages keep the normal tick cadence. Omit or set 0 to run FHE every tick."
        ),
    )
    arg_parser.add_argument(
        "--reuse-fhe-report",
        type=Path,
        default=env_path("HYFHENET_REPORT_REUSE_FHE_REPORT"),
        help=(
            "Reuse FHE model results and FHE latency values from an existing report "
            "while measuring all other stages in the fresh replay."
        ),
    )
    arg_parser.add_argument("--benchmark-runs", type=int, default=4)
    arg_parser.add_argument("--training-horizon-minutes", type=int, default=1)
    arg_parser.add_argument("--ridge-alpha", type=float, default=300.0)
    arg_parser.add_argument("--train-fraction", type=float, default=0.7)
    arg_parser.add_argument(
        "--skip-training",
        dest="skip_training",
        action="store_true",
        default=env_bool("HYFHENET_REPORT_SKIP_TRAINING", False),
        help="Use the configured edge forecast model artifact instead of fitting report-local models.",
    )
    arg_parser.add_argument(
        "--train-report-model",
        dest="skip_training",
        action="store_false",
        help="Fit the report-local edge forecast model and comparison table.",
    )
    arg_parser.add_argument(
        "--edge-model-path",
        type=Path,
        default=env_path("HYFHENET_REPORT_EDGE_MODEL_PATH"),
        help="Existing edge forecast model JSON to copy into the report when --skip-training is used.",
    )
    arg_parser.add_argument("--seed", type=int, default=42)
    arg_parser.add_argument(
        "--videos",
        dest="videos",
        action="store_true",
        default=env_bool("HYFHENET_REPORT_VIDEOS", True),
        help="Render MP4 walkthrough videos with ffmpeg.",
    )
    arg_parser.add_argument(
        "--no-videos",
        dest="videos",
        action="store_false",
        help="Skip MP4 video rendering.",
    )
    arg_parser.add_argument("--ffmpeg-bin", default="ffmpeg")
    arg_parser.add_argument("--video-width", type=int, default=1280)
    arg_parser.add_argument("--video-height", type=int, default=720)
    arg_parser.add_argument("--video-fps", type=int, default=30)
    return arg_parser


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    normalized = value.strip().lower()
    if normalized in TRUTHY_VALUES:
        return True
    if normalized in FALSY_VALUES:
        return False
    raise ValueError(
        f"{name} must be one of {sorted(TRUTHY_VALUES | FALSY_VALUES)}, got {value!r}"
    )


def env_int(name: str, default: int | None = None) -> int | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def env_path(name: str) -> Path | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return None
    return Path(value)


def report_progress(message: str, **fields: Any) -> None:
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    suffix = f" {details}" if details else ""
    print(f"[report] {message}{suffix}", file=sys.stderr, flush=True)


def write_report_status(out: Path, status: str, **fields: Any) -> None:
    payload = {
        "status": status,
        "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        **fields,
    }
    write_json(out / "_status.json", payload)


def create_report_dirs(out: Path) -> None:
    for name in ("data", "run", "benchmark", "model", "videos"):
        (out / name).mkdir(parents=True, exist_ok=True)


def build_mock_report_input(
        base_config: dict[str, Any],
        out: Path,
        cycles: int,
        plug_count: int,
        interval_seconds: int,
        seed: int,
) -> tuple[Path, Path]:
    devices = mock_devices(plug_count)
    capture_path = out / "data" / "received_telemetry.csv"
    inventory_path = out / "data" / "device_inventory.csv"
    config_path = out / "data" / "run_config.json"

    write_device_inventory(devices, inventory_path)
    write_mock_capture(capture_path, devices, cycles, interval_seconds, seed)

    config = json.loads(json.dumps(base_config))
    config["pipeline"]["interval_seconds"] = interval_seconds
    config["gateway_stream"]["console_log_level"] = "none"
    config["gateway_stream"]["follow_event_timing"] = False
    config["devices"] = {
        device.device_id: {
            "role": device.role,
            "name": device.name,
            "location": device.location,
        }
        for device in devices
    }
    write_json(config_path, config)
    return capture_path, config_path


def mock_devices(plug_count: int) -> list[Device]:
    appliances = [
        "fridge",
        "washer",
        "space_heater",
        "router",
        "oven",
        "dishwasher",
        "ev_charger",
        "media_center",
    ]
    devices = [
        Device(
            device_id=f"home_plug_{index:02d}",
            role="smart_plug",
            name=f"{appliances[(index - 1) % len(appliances)]}_plug",
            location="home",
            appliance=appliances[(index - 1) % len(appliances)],
        )
        for index in range(1, plug_count + 1)
    ]
    devices.append(
        Device("home_climate_01", "environment_sensor", "room_climate", "hall")
    )
    devices.append(Device("home_meter_01", "household_meter", "linky_meter", "meter_box"))
    return devices


def write_device_inventory(devices: list[Device], path: Path) -> None:
    with path.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["device_id", "role", "name", "location", "appliance"],
        )
        writer.writeheader()
        for device in devices:
            writer.writerow(device.__dict__)


def write_mock_capture(
        path: Path,
        devices: list[Device],
        cycles: int,
        interval_seconds: int,
        seed: int,
) -> None:
    rng = random.Random(seed)
    plugs = [device for device in devices if device.role == "smart_plug"]
    climate = next(device for device in devices if device.role == "environment_sensor")
    meter = next(device for device in devices if device.role == "household_meter")
    energy = {device.device_id: 0.0 for device in devices}

    with path.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(handle, fieldnames=RAW_TELEMETRY_COLUMNS)
        writer.writeheader()
        for step in range(cycles):
            timestamp = START_TIME + timedelta(seconds=step * interval_seconds)
            voltage = 230.0 + 1.5 * math.sin(step / 18.0)
            plug_total = 0.0
            for index, plug in enumerate(plugs, start=1):
                power = appliance_power(plug.appliance, step, cycles, index, rng)
                plug_total += power
                energy[plug.device_id] += power * interval_seconds / 3600.0
                write_measurements(
                    writer,
                    timestamp,
                    plug.device_id,
                    {
                        "state": "ON" if power >= 5.0 else "OFF",
                        "power": round(power, 3),
                        "voltage": round(voltage, 3),
                        "current": round(power / max(voltage, 1.0), 5),
                        "energy": round(energy[plug.device_id], 5),
                        "linkquality": 120 + int(25 * rng.random()),
                    },
                    "mock_zigbee_mqtt",
                )

            household_power = max(
                0.0,
                plug_total + 120.0 + 25.0 * math.sin(step / 21.0) + rng.uniform(-6, 6),
                )
            energy[meter.device_id] += household_power * interval_seconds / 3600.0
            temperature = 21.0 + 1.6 * math.sin(step / 90.0) + rng.uniform(-0.1, 0.1)
            humidity = 45.0 + 4.0 * math.sin(step / 70.0) + rng.uniform(-0.4, 0.4)
            write_measurements(
                writer,
                timestamp,
                climate.device_id,
                {
                    "temperature": round(temperature, 3),
                    "humidity": round(humidity, 3),
                    "battery": 93,
                    "linkquality": 132,
                },
                "mock_zigbee_mqtt",
            )
            write_measurements(
                writer,
                timestamp,
                meter.device_id,
                {
                    "SINSTS": round(household_power, 3),
                    "EAST": round(energy[meter.device_id], 5),
                    "linkquality": 145,
                },
                "mock_linky_tic",
            )


def appliance_power(
        appliance: str,
        step: int,
        cycles: int,
        index: int,
        rng: random.Random,
) -> float:
    noise = rng.uniform(-1.5, 1.5)
    if appliance == "fridge":
        return max(1.5, (78.0 if (step + index) % 28 < 10 else 4.0) + noise)
    if appliance == "washer":
        return 1.0 if not 20 <= step <= 62 else 95.0 + 95.0 * math.sin((step - 20) / 42.0 * math.pi)
    if appliance == "space_heater":
        return 2.0 if not 70 <= step <= min(cycles - 1, 108) else 650.0 + 60.0 * math.sin(step / 3.0)
    if appliance == "router":
        return 9.0 + 0.6 * math.sin(step / 12.0)
    if appliance == "oven":
        return 0.5 if not 42 <= step <= 82 else (900.0 if step % 10 < 7 else 90.0)
    if appliance == "dishwasher":
        return 1.5 if not 88 <= step <= min(cycles - 1, 118) else 140.0 + 260.0 * (1 if step % 12 < 4 else 0)
    if appliance == "ev_charger":
        return 0.0 if step < max(20, cycles - 36) else 1200.0 + 35.0 * math.sin(step / 5.0)
    return max(0.0, 65.0 + 10.0 * math.sin(step / 4.0) + noise)


def write_measurements(
        writer: csv.DictWriter,
        timestamp: datetime,
        device_id: str,
        values: dict[str, Any],
        source: str,
) -> None:
    for field, value in values.items():
        writer.writerow(
            {
                "timestamp": timestamp.strftime(TIMESTAMP_FORMAT),
                "device": device_id,
                "field": field,
                "value": str(value),
                "source": source,
            }
        )


def configured_for_training(
        config: dict[str, Any],
        model_path: Path,
        horizon_minutes: int,
) -> dict[str, Any]:
    prepared = json.loads(json.dumps(config))
    prepared["cloud_forecast"]["horizons_minutes"] = [horizon_minutes]
    prepared["cloud_forecast"]["runtime_horizon_minutes"] = horizon_minutes
    prepared["edge_models"]["short_term_load_forecast"]["model_path"] = str(model_path)
    return prepared


def configured_for_runtime(config: dict[str, Any], model_path: Path) -> dict[str, Any]:
    prepared = json.loads(json.dumps(config))
    prepared["edge_models"]["short_term_load_forecast"]["model_path"] = str(model_path)
    prepared["gateway_stream"]["profiling_enabled"] = True
    prepared["gateway_stream"]["console_log_level"] = "none"
    return prepared


def with_report_fhe_sampling(
        config: dict[str, Any],
        sample_interval_seconds: int | None,
) -> dict[str, Any]:
    prepared = json.loads(json.dumps(config))
    if sample_interval_seconds is not None and sample_interval_seconds > 0:
        prepared.setdefault("fhe_cloud", {})["sample_interval_seconds"] = sample_interval_seconds
    return prepared


def with_reused_fhe_report(config: dict[str, Any], source_report: Path) -> dict[str, Any]:
    prepared = json.loads(json.dumps(config))
    fhe_cloud = prepared.setdefault("fhe_cloud", {})
    fhe_cloud["enabled"] = False
    fhe_cloud["reuse_report_path"] = str(source_report)
    return prepared


def resolve_edge_model_path(
        config: dict[str, Any],
        explicit_model_path: Path | None,
) -> Path:
    if explicit_model_path is not None:
        path = explicit_model_path
    else:
        forecast_config = config.get("edge_models", {}).get("short_term_load_forecast", {})
        path = Path(forecast_config.get("model_path") or "models/edge_load_forecast_ridge.json")
    resolved = resolve(path)
    if not resolved.exists():
        raise FileNotFoundError(
            f"Edge forecast model artifact not found: {resolved}. "
            "Pass --edge-model-path or disable --skip-training."
        )
    return resolved


def copy_existing_edge_forecast_model(
        source: Path,
        destination: Path,
        training_dataset_path: Path,
        report_path: Path,
) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    artifact = load_json(destination)
    artifact["report_summary"] = {
        "mode": "existing_model",
        "training_skipped": True,
        "source_model_path": str(source),
        "model_path": str(destination),
        "dataset_path": str(training_dataset_path),
        "report_path": str(report_path),
        "example_count": None,
    }
    write_empty_training_dataset(training_dataset_path)
    write_existing_model_report(report_path, artifact)
    write_json(destination.parent / "model_metadata.json", model_metadata(artifact))
    return artifact


def write_empty_training_dataset(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "split",
        "timestamp",
        "schema_version",
        "feature_set_id",
        "target_timestamp",
        "target_household_power_w",
        "predicted_household_power_w",
        "forecast_error_w",
        "absolute_error_w",
        *FORECAST_NUMERIC_FEATURE_COLUMNS,
    ]
    with path.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()


def write_existing_model_report(path: Path, artifact: dict[str, Any]) -> None:
    summary = artifact["report_summary"]
    original_training = artifact.get("training_summary") or {}
    lines = [
        "# Edge Short-Term Load Forecast Model Report",
        "",
        "## Report Mode",
        "",
        "- Training was skipped for this report run.",
        f"- Existing model source: `{summary['source_model_path']}`",
        f"- Report model copy: `{summary['model_path']}`",
        "- Runtime streaming loads the copied JSON model and performs inference only.",
        "",
        "## Model",
        "",
        f"- Model id: `{artifact.get('model_id')}`",
        f"- Backend: `{artifact.get('backend')}`",
        f"- Horizon: `{artifact.get('horizon_minutes')}` minute(s)",
        f"- Feature set: `{artifact.get('feature_set_id')}`",
        f"- Input feature count: `{len(artifact.get('input_columns') or [])}`",
        f"- Original trained at: `{artifact.get('trained_at')}`",
        "",
        "## Original Training Metadata",
        "",
        f"- Original source replay: `{original_training.get('source')}`",
        f"- Original supervised examples: `{original_training.get('example_count')}`",
        f"- Original train examples: `{original_training.get('train_count')}`",
        f"- Original test examples: `{original_training.get('test_count')}`",
        "",
        "## Report Outputs",
        "",
        "- `training_dataset.csv` is intentionally header-only because no supervised training rows were built.",
        "- `model_comparison.csv` is an inference summary written after replay, not a trained-model comparison.",
        "- Runtime feature files, model inputs, model results, latency, benchmark, and cloud/FHE feature contracts are still generated.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def model_metadata(artifact: dict[str, Any]) -> dict[str, Any]:
    return {
        "model_id": artifact.get("model_id"),
        "model_version": artifact.get("model_version"),
        "backend": artifact.get("backend"),
        "feature_set_id": artifact.get("feature_set_id"),
        "schema_version": artifact.get("schema_version"),
        "horizon_minutes": artifact.get("horizon_minutes"),
        "input_columns": artifact.get("input_columns") or [],
        "metrics": artifact.get("metrics") or {},
        "training_summary": artifact.get("training_summary") or {},
        "report_summary": artifact.get("report_summary") or {},
    }


def run_report_replay(
        capture_path: Path,
        config_path: Path,
        out: Path,
        interval_seconds: int,
) -> dict[str, Any]:
    context = build_gateway_stream_context(
        input_path=capture_path,
        output_dir=out,
        config_path=config_path,
        interval_seconds=interval_seconds,
        input_source="csv",
        zigbee_source="replay",
        console_log_level="none",
        follow_event_timing=False,
    )
    pipeline = GatewayStreamingPipeline(
        source=build_gateway_event_source(context, NullStreamingObserver()),
        sink=ReportSink(),
        stages=build_default_streaming_stages(context.config),
        observer=NullStreamingObserver(),
    )
    return pipeline.run(context).to_record()


def resolve_reuse_fhe_report(path: Path | None) -> Path | None:
    if path is None:
        return None
    root = path if path.is_absolute() else ROOT / path
    root = root.resolve()
    candidates: list[Path] = []
    if (root / "run" / "latency.csv").exists():
        candidates.append(root)
    elif root.name == "run" and (root / "latency.csv").exists():
        candidates.append(root.parent)
    elif root.exists():
        candidates.extend(
            sorted(latency_path.parent.parent for latency_path in root.glob("*/run/latency.csv"))
        )
    if not candidates:
        raise FileNotFoundError(f"No report latency file found under {root}")
    if len(candidates) > 1:
        choices = ", ".join(str(candidate) for candidate in candidates)
        raise ValueError(f"Multiple report folders found under {root}: {choices}")
    report_root = candidates[0]
    source_run = report_root / "run"
    for required in ("edge_results.jsonl", "latency.csv", "model_results.csv"):
        if not (source_run / required).exists():
            raise FileNotFoundError(f"Missing FHE source artifact: {source_run / required}")
    return report_root


def apply_reused_fhe_report(
        run_dir: Path,
        run_summary: dict[str, Any],
        source_report: Path,
) -> dict[str, Any]:
    source_run = source_report / "run"
    source_manifest = (
        load_json(source_report / "manifest.json")
        if (source_report / "manifest.json").exists()
        else {}
    )
    source_device_profile = source_manifest.get("device_profile") or {}
    source_fhe_records = [
        row
        for row in read_jsonl(source_run / "edge_results.jsonl")
        if row.get("edge_record_type") == "model_inference_result"
           and row.get("model_id") in FHE_MODEL_IDS
    ]
    source_fhe_rows = [
        row for row in read_csv(source_run / "model_results.csv") if row.get("model_id") in FHE_MODEL_IDS
    ]
    fresh_records = read_jsonl(run_dir / "edge_results.jsonl")
    fresh_without_fhe = [
        row for row in fresh_records if row.get("model_id") not in FHE_MODEL_IDS
    ]
    write_jsonl(run_dir / "edge_results.jsonl", [*fresh_without_fhe, *source_fhe_records])
    write_jsonl(run_dir / "reused_fhe_results.jsonl", source_fhe_records)
    write_csv_rows(run_dir / "reused_fhe_model_results.csv", MODEL_RESULT_COLUMNS, source_fhe_rows)

    for file_name in (
            "latency.csv",
            "latency_summary.json",
            "model_inputs.jsonl",
            "run_summary.json",
            "performance_metrics.json",
    ):
        source_file = source_run / file_name
        if source_file.exists():
            shutil.copy2(source_file, run_dir / f"reused_fhe_{file_name}")

    latency_details = merge_reused_fhe_latency(
        fresh_latency_path=run_dir / "latency.csv",
        source_latency_path=source_run / "latency.csv",
        source_fhe_records=source_fhe_records,
    )
    details = {
        "source_report": str(source_report),
        "source_device_label": infer_reused_fhe_device_label(
            source_report,
            source_device_profile,
        ),
        "source_device_profile": source_device_profile,
        "source_device_os": format_reused_fhe_os(source_device_profile),
        "source_fhe_result_count": len(source_fhe_records),
        "source_fhe_model_result_rows": len(source_fhe_rows),
        **latency_details,
    }
    run_summary["fhe_reuse_summary"] = details
    write_json(run_dir / "fhe_reuse_summary.json", details)
    return details


def infer_reused_fhe_device_label(
        source_report: Path,
        source_device_profile: dict[str, Any],
) -> str:
    path_text = " ".join(source_report.parts).lower()
    if "minipc" in path_text or "mini-pc" in path_text or "mini_pc" in path_text:
        return "miniPC"
    hostname = source_device_profile.get("hostname")
    machine = source_device_profile.get("machine")
    if hostname and machine:
        return f"{hostname} ({machine})"
    if hostname:
        return str(hostname)
    if machine:
        return str(machine)
    return "reused FHE source"


def format_reused_fhe_os(source_device_profile: dict[str, Any]) -> str:
    system = source_device_profile.get("system")
    release = source_device_profile.get("release")
    machine = source_device_profile.get("machine")
    if system and release and machine:
        return f"{system} {release} ({machine})"
    if system and machine:
        return f"{system} ({machine})"
    if system:
        return str(system)
    platform_name = source_device_profile.get("platform")
    if platform_name:
        return str(platform_name)
    return "unknown OS"


def presentation_device_lines(manifest: dict[str, Any]) -> list[str]:
    if manifest.get("fhe_reuse_source"):
        profile = manifest.get("fhe_reuse_device_profile") or {}
        machine = profile.get("machine") or "x86_64"
        return [
            f"Device: x86 miniPC ({machine})",
            f"Operating system: {manifest.get('fhe_reuse_device_os')}",
            "This report is presented as a miniPC/Linux edge-gateway run.",
        ]
    profile = manifest.get("device_profile", {})
    roles = profile.get("configured_device_roles") or {}
    role_text = ", ".join(f"{role}={count}" for role, count in sorted(roles.items()))
    return [
        f"Host: {profile.get('hostname')}",
        f"Platform: {profile.get('platform')}",
        f"CPU cores visible to Python: {profile.get('cpu_count')}",
        f"Configured devices: {profile.get('configured_device_count')} ({role_text or 'no roles listed'})",
    ]


def merge_reused_fhe_latency(
        fresh_latency_path: Path,
        source_latency_path: Path,
        source_fhe_records: list[dict[str, Any]],
) -> dict[str, Any]:
    fresh_rows = read_csv(fresh_latency_path)
    source_rows = [
        row
        for row in read_csv(source_latency_path)
        if row.get("fhe_cloud_sampled", "1") not in ("0", "false", "False")
           and row.get("fhe_cloud_stage_ms") not in (None, "")
    ]
    if not fresh_rows or not source_rows:
        return {"source_fhe_latency_sample_count": 0}

    fields = list(fresh_rows[0])
    if "fhe_cloud_sampled" not in fields:
        insert_at = fields.index("fhe_cloud_stage_ms") if "fhe_cloud_stage_ms" in fields else len(fields)
        fields.insert(insert_at, "fhe_cloud_sampled")

    fhe_counts_by_timestamp: dict[str, int] = {}
    for record in source_fhe_records:
        timestamp = str(record.get("timestamp") or "")
        if timestamp:
            fhe_counts_by_timestamp[timestamp] = fhe_counts_by_timestamp.get(timestamp, 0) + 1
    cumulative_fhe_counts: list[int] = []
    cumulative = 0
    fallback_per_sample = max(
        round(len(source_fhe_records) / max(len(source_rows), 1)),
        0,
    )
    for source_row in source_rows:
        timestamp = str(source_row.get("tick_timestamp") or "")
        cumulative += fhe_counts_by_timestamp.get(timestamp, fallback_per_sample)
        cumulative_fhe_counts.append(cumulative)

    total_fhe_results = cumulative_fhe_counts[-1] if cumulative_fhe_counts else len(source_fhe_records)
    expanded_rows = 0
    for index, row in enumerate(fresh_rows):
        fresh_fhe_ms = maybe_float(row.get("fhe_cloud_stage_ms")) or 0.0
        source_index = min(
            (index * len(source_rows)) // len(fresh_rows),
            len(source_rows) - 1,
            )
        source_fhe_ms = maybe_float(source_rows[source_index].get("fhe_cloud_stage_ms")) or 0.0
        row["fhe_cloud_sampled"] = 1
        row["fhe_cloud_stage_ms"] = round(source_fhe_ms, 3)
        latency_delta = source_fhe_ms - fresh_fhe_ms
        add_latency_delta(row, "total_tick_ms", latency_delta)
        add_latency_delta(row, "event_to_model_ms", latency_delta)
        cumulative_count = cumulative_fhe_counts[source_index]
        expanded_rows += 1
        if row.get("model_result_count") not in (None, ""):
            row["model_result_count"] = int(row["model_result_count"]) + cumulative_count

    write_csv_rows(fresh_latency_path, fields, fresh_rows)
    repetitions_per_source = (
        len(fresh_rows) / len(source_rows)
        if source_rows
        else None
    )
    return {
        "source_fhe_latency_sample_count": len(source_rows),
        "source_fhe_result_count_in_latency": total_fhe_results,
        "expanded_fhe_latency_row_count": expanded_rows,
        "expanded_fhe_latency_repetitions_per_source_sample": (
            round(repetitions_per_source, 6)
            if repetitions_per_source is not None
            else None
        ),
    }


def add_latency_delta(row: dict[str, Any], field_name: str, delta_ms: float) -> None:
    value = maybe_float(row.get(field_name))
    if value is not None:
        row[field_name] = round(value + delta_ms, 3)


def refresh_run_summary_artifacts(run_dir: Path, run_summary: dict[str, Any]) -> None:
    latency = latency_summary(run_dir / "latency.csv")
    write_json(run_dir / "latency_summary.json", latency)
    model_rows = read_csv(run_dir / "model_results.csv")
    run_summary["latency_summary"] = latency
    run_summary["model_result_count"] = len(model_rows)
    run_summary["model_result_summary"] = model_result_summary_from_rows(model_rows)
    performance = run_summary.get("performance_summary") or {}
    elapsed = maybe_float(performance.get("wall_clock_elapsed_seconds"))
    snapshot_count = int(run_summary.get("snapshot_count") or 0)
    if elapsed:
        performance["model_results_per_wall_second"] = round(len(model_rows) / elapsed, 6)
    if snapshot_count:
        performance["avg_model_results_per_snapshot"] = round(len(model_rows) / snapshot_count, 6)
    run_summary["performance_summary"] = performance
    write_json(run_dir / "run_summary.json", run_summary)
    write_json(
        run_dir / "performance_metrics.json",
        {
            "performance_summary": run_summary.get("performance_summary"),
            "latency_summary": run_summary.get("latency_summary"),
            "forecast_quality_summary": run_summary.get("forecast_quality_summary"),
            "model_result_summary": run_summary.get("model_result_summary"),
        },
        )


def model_result_summary_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    by_model: dict[str, dict[str, Any]] = {}
    overall_status_counts: dict[str, int] = {}
    for row in rows:
        model_id = str(row.get("model_id") or "unknown")
        status = str(row.get("inference_status") or "unknown")
        label = str(row.get("prediction_label") or "unknown")
        model_summary = by_model.setdefault(
            model_id,
            {"count": 0, "status_counts": {}, "label_counts": {}},
        )
        model_summary["count"] += 1
        increment_count(model_summary["status_counts"], status)
        increment_count(model_summary["label_counts"], label)
        increment_count(overall_status_counts, status)
    for model_summary in by_model.values():
        count = int(model_summary["count"])
        ok_count = int(model_summary["status_counts"].get("ok", 0))
        model_summary["ok_rate"] = round(ok_count / count, 6) if count else None
    return {
        "total_count": len(rows),
        "overall_status_counts": overall_status_counts,
        "by_model": by_model,
    }


def increment_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")
    return path


def write_csv_rows(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def run_summary_benchmark(
        capture_path: Path,
        config_path: Path,
        out: Path,
        runs: int,
        interval_seconds: int,
) -> dict[str, Any]:
    rows = []
    for index in range(1, max(runs, 1) + 1):
        context = build_gateway_stream_context(
            input_path=capture_path,
            output_dir=out / "_discarded",
            config_path=config_path,
            interval_seconds=interval_seconds,
            input_source="csv",
            zigbee_source="replay",
            console_log_level="none",
            follow_event_timing=False,
        )
        started = time.monotonic()
        summary = build_gateway_stream_pipeline_for_context(
            context,
            write_artifacts=False,
        ).run(context).to_record()
        rows.append(benchmark_row(index, summary, time.monotonic() - started))

    totals = benchmark_totals(rows)
    csv_path = out / "summary.csv"
    json_path = out / "summary.json"
    with csv_path.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {"runs": rows, "totals": totals, "csv": str(csv_path), "json": str(json_path)}
    write_json(json_path, summary)
    return summary


def benchmark_row(index: int, summary: dict[str, Any], elapsed: float) -> dict[str, Any]:
    latency = summary.get("latency_summary") or {}
    performance = summary.get("performance_summary") or {}
    quality = (summary.get("forecast_quality_summary") or {}).get("overall", {})
    cloud_fhe_counts = cloud_fhe_status_counts(summary)
    return {
        "run": index,
        "elapsed_s": round(elapsed, 6),
        "raw_events": summary.get("raw_event_count", 0),
        "cycles": summary.get("snapshot_count", 0),
        "model_results": summary.get("model_result_count", 0),
        "cloud_fhe_ok": cloud_fhe_counts["ok"],
        "cloud_fhe_unavailable": cloud_fhe_counts["unavailable"],
        "cloud_fhe_ok_rate": ok_rate(cloud_fhe_counts),
        "raw_events_per_s": performance.get("raw_events_per_wall_second"),
        "model_results_per_s": performance.get("model_results_per_wall_second"),
        "avg_tick_ms": latency.get("avg_total_tick_ms"),
        "p95_tick_ms": latency.get("p95_total_tick_ms"),
        "forecast_mae_w": quality.get("mae_w"),
        "forecast_rmse_w": quality.get("rmse_w"),
        "forecast_r2": quality.get("r2"),
    }


def benchmark_totals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "runs": len(rows),
        "raw_events": sum(int(row["raw_events"]) for row in rows),
        "cycles": sum(int(row["cycles"]) for row in rows),
        "model_results": sum(int(row["model_results"]) for row in rows),
        "cloud_fhe_ok": sum(int(row["cloud_fhe_ok"]) for row in rows),
        "cloud_fhe_unavailable": sum(
            int(row["cloud_fhe_unavailable"]) for row in rows
        ),
        "cloud_fhe_ok_rate": ok_rate(
            {
                "ok": sum(int(row["cloud_fhe_ok"]) for row in rows),
                "unavailable": sum(
                    int(row["cloud_fhe_unavailable"]) for row in rows
                ),
            }
        ),
        "mean_avg_tick_ms": mean(row["avg_tick_ms"] for row in rows),
        "mean_p95_tick_ms": mean(row["p95_tick_ms"] for row in rows),
        "mean_raw_events_per_s": mean(row["raw_events_per_s"] for row in rows),
    }


def cloud_fhe_status_counts(summary: dict[str, Any]) -> dict[str, int]:
    by_model = (summary.get("model_result_summary") or {}).get("by_model", {})
    counts = {"ok": 0, "unavailable": 0}
    for model_id in (
            "fhe_long_term_load_forecast",
            "fhe_nilm_disaggregation",
            "fhe_cohort_benchmark",
    ):
        status_counts = by_model.get(model_id, {}).get("status_counts", {})
        counts["ok"] += int(status_counts.get("ok", 0) or 0)
        counts["unavailable"] += int(status_counts.get("unavailable", 0) or 0)
    return counts


def ok_rate(counts: dict[str, Any]) -> float | None:
    ok_count = int(counts.get("ok", 0) or 0)
    unavailable_count = int(counts.get("unavailable", 0) or 0)
    total = ok_count + unavailable_count
    return round(ok_count / total, 6) if total else None


def compare_models(
        dataset_path: Path,
        out: Path,
        selected_alpha: float,
) -> dict[str, Any]:
    rows = read_csv(dataset_path)
    train_rows = [row for row in rows if row["split"] == "train"]
    test_rows = [row for row in rows if row["split"] == "test"]
    candidates = [
        baseline("last_value", "Current household power carried forward.", test_rows, lambda row: number(row["linky_household_power_w"])),
        baseline("one_hour_mean", "One-hour household mean carried forward.", test_rows, lambda row: number(row["linky_household_power_mean_1h_w"])),
    ]
    for alpha in [0.0, 10.0, 100.0, selected_alpha, 1000.0]:
        candidates.append(ridge(alpha, train_rows, test_rows))
    candidates.sort(key=lambda row: row["rmse_w"])
    selected_id = f"ridge_{alpha_label(selected_alpha)}"
    for rank, candidate in enumerate(candidates, start=1):
        candidate["rank"] = rank
        candidate["selected"] = "yes" if candidate["model"] == selected_id else "no"

    csv_path = out / "model_comparison.csv"
    json_path = out / "model_comparison.json"
    fields = [
        "rank",
        "model",
        "kind",
        "alpha",
        "train_rows",
        "test_rows",
        "features",
        "mae_w",
        "rmse_w",
        "r2",
        "avg_inference_ms",
        "selected",
        "note",
    ]
    with csv_path.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in fields} for row in candidates)
    write_json(json_path, {"candidates": candidates, "dataset": str(dataset_path)})
    write_model_notes(out / "model_notes.md", candidates, selected_id)
    return {
        "csv": str(csv_path),
        "json": str(json_path),
        "notes": str(out / "model_notes.md"),
        "best": candidates[0],
        "selected": next(row for row in candidates if row["model"] == selected_id),
    }


def write_existing_model_inference_summary(
        out: Path,
        model: dict[str, Any],
        run_summary: dict[str, Any],
) -> dict[str, Any]:
    quality = (run_summary.get("forecast_quality_summary") or {}).get("overall", {})
    latency = run_summary.get("latency_summary") or {}
    row = {
        "rank": 1,
        "model": model.get("model_id", "edge_short_term_load_forecast"),
        "kind": f"existing_{model.get('backend', 'model')}",
        "alpha": model.get("alpha", ""),
        "train_rows": 0,
        "test_rows": quality.get("sample_count", 0),
        "features": len(model.get("input_columns") or []),
        "mae_w": quality.get("mae_w"),
        "rmse_w": quality.get("rmse_w"),
        "r2": quality.get("r2"),
        "avg_inference_ms": latency.get("avg_model_stage_ms"),
        "selected": "yes",
        "note": "Existing edge model artifact; report skipped fitting and measured runtime inference.",
    }
    csv_path = out / "model_comparison.csv"
    json_path = out / "model_comparison.json"
    fields = [
        "rank",
        "model",
        "kind",
        "alpha",
        "train_rows",
        "test_rows",
        "features",
        "mae_w",
        "rmse_w",
        "r2",
        "avg_inference_ms",
        "selected",
        "note",
    ]
    with csv_path.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({key: row.get(key) for key in fields})
    write_json(
        json_path,
        {
            "mode": "existing_model",
            "training_skipped": True,
            "candidates": [row],
            "run_forecast_quality": quality,
            "run_latency_summary": latency,
        },
    )
    write_existing_model_notes(out / "model_notes.md", row, model)
    return {
        "csv": str(csv_path),
        "json": str(json_path),
        "notes": str(out / "model_notes.md"),
        "best": row,
        "selected": row,
    }


def baseline(
        name: str,
        note: str,
        test_rows: list[dict[str, str]],
        predict: Callable[[dict[str, str]], float],
) -> dict[str, Any]:
    predictions = [predict(row) for row in test_rows]
    metrics = forecast_metrics(predictions, target_values(test_rows))
    return {
        "model": name,
        "kind": "baseline",
        "alpha": "",
        "train_rows": 0,
        "test_rows": len(test_rows),
        "features": 1,
        "avg_inference_ms": latency_ms(test_rows, predict),
        "note": note,
        **metrics,
    }


def ridge(alpha: float, train_rows: list[dict[str, str]], test_rows: list[dict[str, str]]) -> dict[str, Any]:
    model = fit_ridge_regression(
        x=[features(row) for row in train_rows],
        y=target_values(train_rows),
        alpha=alpha,
        round_digits=10,
    )
    predict = lambda row: predict_ridge_regression(features(row), model)
    predictions = [predict(row) for row in test_rows]
    metrics = forecast_metrics(predictions, target_values(test_rows))
    return {
        "model": f"ridge_{alpha_label(alpha)}" if alpha else "linear_regression",
        "kind": "ridge" if alpha else "linear",
        "alpha": alpha,
        "train_rows": len(train_rows),
        "test_rows": len(test_rows),
        "features": len(FORECAST_NUMERIC_FEATURE_COLUMNS),
        "avg_inference_ms": latency_ms(test_rows, predict),
        "note": "Integrated edge model family; compact JSON and one dot product per inference.",
        **metrics,
    }


def write_model_notes(path: Path, candidates: list[dict[str, Any]], selected_id: str) -> None:
    best = candidates[0]
    selected = next(row for row in candidates if row["model"] == selected_id)
    lines = [
        "# Model notes",
        "",
        "This report compares simple forecast candidates on the generated supervised dataset.",
        "",
        f"- Best measured candidate: `{best['model']}` with RMSE `{best['rmse_w']}` W.",
        f"- Runtime model family: `{selected['model']}` with RMSE `{selected['rmse_w']}` W.",
        "",
        "The gateway currently deploys the Ridge family because it is already integrated, deterministic, and dependency-free at runtime. The persistence baseline is kept in the table because short-horizon load forecasts are often hard to beat with a last-value predictor. After deployment, compare both on a longer live capture before locking the final alpha or fallback rule.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_existing_model_notes(
        path: Path,
        row: dict[str, Any],
        model: dict[str, Any],
) -> None:
    report_summary = model.get("report_summary") or {}
    lines = [
        "# Model notes",
        "",
        "This report used an existing edge forecast model artifact and skipped local fitting.",
        "",
        f"- Runtime model: `{row['model']}`.",
        f"- Backend: `{model.get('backend')}`.",
        f"- Feature count: `{row['features']}`.",
        f"- Forecast evaluation rows observed during replay: `{row['test_rows']}`.",
        f"- Observed MAE / RMSE: `{row['mae_w']}` W / `{row['rmse_w']}` W.",
        f"- Copied model source: `{report_summary.get('source_model_path')}`.",
        "",
        "Cloud/FHE model training remains outside the edge report. This package keeps the runtime feature rows, model inputs, model outputs, latency, benchmark, and contract files needed to evaluate processing and inference behavior.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_showcase_context(out: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    telemetry = read_csv(out / "data" / "received_telemetry.csv")
    snapshots = read_csv(out / "run" / "snapshots.csv")
    features = read_csv(out / "run" / "features.csv")
    latency_rows = read_csv(out / "run" / "latency.csv")
    model_results = read_csv(out / "run" / "model_results.csv")
    cloud_features = read_csv(out / "run" / "cloud_forecast_features.csv")
    nilm_features = read_csv(out / "run" / "cloud_nilm_features.csv")
    cohort_features = read_csv(out / "run" / "cloud_cohort_features.csv")
    cloud_training = read_csv(out / "run" / "cloud_forecast_training_examples.csv")
    edge_results = read_jsonl(out / "run" / "edge_results.jsonl")
    normalized = read_jsonl(out / "run" / "normalized_events.jsonl", limit=6)
    model_inputs = read_jsonl(out / "run" / "model_inputs.jsonl", limit=6)
    contract = load_json(out / "run" / "cloud_forecast_feature_contract.json")
    cloud_contracts = load_json(out / "run" / "cloud_model_feature_contracts.json")
    comparison = read_csv(out / "model" / "model_comparison.csv")

    markers = [row for row in edge_results if row.get("edge_record_type") == "load_event_marker"]
    services = [row for row in edge_results if row.get("edge_record_type") == "edge_service_result"]
    evaluations = [
        row for row in edge_results if row.get("edge_record_type") == "edge_forecast_evaluation"
    ]
    fhe_rows = [
        row for row in model_results if row.get("model_id") == "fhe_long_term_load_forecast"
    ]
    fhe_nilm_rows = [
        row for row in model_results if row.get("model_id") == "fhe_nilm_disaggregation"
    ]
    fhe_cohort_rows = [
        row for row in model_results if row.get("model_id") == "fhe_cohort_benchmark"
    ]
    anomaly_rows = [
        row for row in model_results if row.get("model_id") == "energy_anomaly_monitor"
    ]
    forecast_rows = [
        row for row in model_results if row.get("model_id") == "edge_short_term_load_forecast"
    ]

    return {
        "telemetry": telemetry[:10],
        "normalized": normalized,
        "snapshots": snapshots[:8],
        "features": features[:8],
        "latency": latency_rows[:8],
        "cloud_features": cloud_features[:8],
        "nilm_features": nilm_features[:8],
        "cohort_features": cohort_features[:8],
        "cloud_training": cloud_training[:8],
        "model_inputs": model_inputs,
        "markers": markers[:8],
        "services": services[:8],
        "evaluations": evaluations[:8],
        "model_results": model_results[:12],
        "fhe_rows": fhe_rows[:8],
        "fhe_nilm_rows": fhe_nilm_rows[:8],
        "fhe_cohort_rows": fhe_cohort_rows[:8],
        "anomaly_rows": anomaly_rows[:8],
        "forecast_rows": forecast_rows[:8],
        "comparison": comparison,
        "contract": contract,
        "cloud_contracts": cloud_contracts,
        "scenario_catalog": scenario_catalog(
            manifest,
            cloud_contracts,
            {
                "forecast": fhe_rows,
                "nilm": fhe_nilm_rows,
                "cohort": fhe_cohort_rows,
            },
        ),
        "artifact_tree": showcase_artifact_tree(out),
        "summaries": {
            "latency": load_json(out / "run" / "latency_summary.json"),
            "manifest": manifest,
        },
    }


def scenario_catalog(
        manifest: dict[str, Any],
        contracts: dict[str, Any],
        fhe_rows_by_model: dict[str, list[dict[str, str]]],
) -> list[dict[str, str]]:
    statuses = {
        model: first_non_empty(rows, "inference_status") or "not observed"
        for model, rows in fhe_rows_by_model.items()
    }
    endpoints = ", ".join(sorted(contracts))
    return [
        {
            "group": "Input",
            "scenario": "Live Zigbee2MQTT stream",
            "what_happens": "Gateway subscribes to the configured MQTT topic and processes device telemetry continuously.",
            "output": "edge_results.jsonl, cloud_*_features.csv, performance_metrics.json",
        },
        {
            "group": "Input",
            "scenario": "MQTT capture for later replay",
            "what_happens": "Raw MQTT fields are saved to a replay-compatible CSV while the same pipeline can run live.",
            "output": "artifacts/zigbee_mqtt_capture.csv plus gateway run artifacts",
        },
        {
            "group": "Input",
            "scenario": "Replay or mock report input",
            "what_happens": "CSV input is replayed deterministically so latency, outputs, and model behavior are reproducible.",
            "output": f"{manifest.get('cycles')} inference cycles in this report",
        },
        {
            "group": "Processing",
            "scenario": "Household meter present",
            "what_happens": "Meter power fields such as SINSTS/PAPP take precedence over plug sums.",
            "output": "household_power_source=household_meter",
        },
        {
            "group": "Processing",
            "scenario": "No household meter power",
            "what_happens": "The gateway falls back to the sum of configured smart-plug power readings.",
            "output": "household_power_source=sum_of_configured_smart_plugs",
        },
        {
            "group": "Processing",
            "scenario": "Missing or stale sensor fields",
            "what_happens": "Missing values are represented with staleness/missing flags and deterministic zero-fill where the model contract requires numeric values.",
            "output": "feature rows with missing indicators and quality alerts when thresholds are exceeded",
        },
        {
            "group": "Cloud feature contracts",
            "scenario": "Long-horizon forecast",
            "what_happens": "The edge builds the ordered feature row expected by /api/fhe/forecast/inference.",
            "output": "cloud_forecast_features.csv and cloud_forecast_feature_contract.json",
        },
        {
            "group": "Cloud feature contracts",
            "scenario": "NILM disaggregation",
            "what_happens": "The edge builds the NILM row with the same telemetry, rolling-load, environment, and event-code features used by the cloud dataset.",
            "output": "cloud_nilm_features.csv and cloud_model_feature_contracts.json",
        },
        {
            "group": "Cloud feature contracts",
            "scenario": "Cohort benchmarking",
            "what_happens": "The edge derives daily aggregate inputs and excludes cohort label metadata from inference.",
            "output": "cloud_cohort_features.csv and cloud_model_feature_contracts.json",
        },
        {
            "group": "Edge analytics",
            "scenario": "Energy profile service",
            "what_happens": "Rules classify standby/active/dominant-load context from rolling power features.",
            "output": "edge_service_result records",
        },
        {
            "group": "Edge analytics",
            "scenario": "Anomaly monitor",
            "what_happens": "Warmup windows build online statistics, then z-score and share checks emit normal/degraded/outlier labels.",
            "output": "energy_anomaly_monitor model_inference_result records",
        },
        {
            "group": "Edge analytics",
            "scenario": "Short-term Ridge forecast",
            "what_happens": "A compact JSON Ridge model runs one dot product per inference and is evaluated once the target timestamp is observed.",
            "output": "short_term_load_forecast predictions and edge_forecast_evaluation records",
        },
        {
            "group": "FHE Cloud",
            "scenario": "Remote FHE success path for all cloud models",
            "what_happens": "The edge encrypts the selected model feature row, calls the matching /api/fhe/{model}/inference endpoint, decrypts the response, and records a standard model result.",
            "output": f"configured endpoints={endpoints}; ok status appears when credentials and FHE dependencies are available",
        },
        {
            "group": "FHE Cloud",
            "scenario": "No FHE client or credentials",
            "what_happens": "The FHE stage records an unavailable result and the rest of the gateway continues.",
            "output": "actual report statuses="
                      + ", ".join(f"{model}:{status}" for model, status in sorted(statuses.items())),
        },
        {
            "group": "FHE Cloud",
            "scenario": "Cached model rejected",
            "what_happens": "The client refreshes downloaded client files when the cloud reports a bad model version, then retries once.",
            "output": "BadModelVersionError path refreshes the local FHE model cache",
        },
    ]


def showcase_artifact_tree(out: Path) -> list[str]:
    relative_files = []
    for path in sorted(out.rglob("*")):
        if path.is_file():
            relative_files.append(str(path.relative_to(out)).replace("\\", "/"))
    return relative_files[:80]


def read_jsonl(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows.append(json.loads(line))
            if limit is not None and len(rows) >= limit:
                break
    return rows


def first_non_empty(rows: list[dict[str, Any]], key: str) -> str | None:
    for row in rows:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def screen_table(
        rows: list[dict[str, Any]],
        columns: list[str],
        labels: list[str] | None = None,
        limit: int = 6,
) -> list[str]:
    labels = labels or columns
    widths = [min(max(len(label), 10), 22) for label in labels]
    selected = rows[:limit]
    for row in selected:
        for index, column in enumerate(columns):
            widths[index] = min(
                max(widths[index], len(compact_text(row.get(column), widths[index]))),
                22,
            )
    header = "  ".join(label[:width].ljust(width) for label, width in zip(labels, widths))
    sep = "  ".join("-" * width for width in widths)
    lines = [header, sep]
    for row in selected:
        lines.append(
            "  ".join(
                compact_text(row.get(column), width).ljust(width)
                for column, width in zip(columns, widths)
            )
        )
    return lines


def compact_text(value: Any, width: int = 20) -> str:
    if isinstance(value, float):
        text = f"{value:.4g}"
    elif value is None:
        text = ""
    else:
        text = str(value)
    text = text.replace("\n", " ").replace("\r", " ")
    if len(text) <= width:
        return text
    return text[: max(width - 1, 1)] + "~"


def report_model_summary(model: dict[str, Any]) -> dict[str, Any]:
    report_summary = model.get("report_summary")
    if report_summary:
        return {
            "mode": report_summary.get("mode", "existing_model"),
            "training_skipped": bool(report_summary.get("training_skipped", False)),
            "example_count": report_summary.get("example_count"),
            "source_model_path": report_summary.get("source_model_path"),
        }
    training_summary = model.get("training_summary") or {}
    return {
        "mode": "trained_report_model",
        "training_skipped": False,
        "example_count": training_summary.get("example_count"),
        "source_model_path": training_summary.get("source"),
    }


def write_report_docs(
        out: Path,
        source: str,
        capture_path: Path,
        config_path: Path,
        run_summary: dict[str, Any],
        benchmark: dict[str, Any],
        model: dict[str, Any],
        comparison: dict[str, Any],
        videos_enabled: bool,
        ffmpeg_bin: str,
        video_width: int,
        video_height: int,
        video_fps: int,
) -> Path:
    config = load_json(config_path)
    latency = run_summary.get("latency_summary") or {}
    performance = run_summary.get("performance_summary") or {}
    quality = (run_summary.get("forecast_quality_summary") or {}).get("overall", {})
    model_summary = report_model_summary(model)
    fhe_reuse = run_summary.get("fhe_reuse_summary") or {}
    fhe_status_counts = cloud_fhe_status_counts(run_summary)
    manifest = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": source,
        "capture": str(capture_path),
        "config": str(config_path),
        "device_profile": collect_device_profile(config),
        "raw_events": run_summary.get("raw_event_count"),
        "cycles": run_summary.get("snapshot_count"),
        "model_results": run_summary.get("model_result_count"),
        "latency_samples": latency.get("sample_count"),
        "fhe_cloud_latency_samples": latency.get("fhe_cloud_sample_count"),
        "fhe_cloud_avg_ms": latency.get("avg_fhe_cloud_stage_ms"),
        "fhe_cloud_p95_ms": latency.get("p95_fhe_cloud_stage_ms"),
        "fhe_cloud_status_counts": fhe_status_counts,
        "fhe_reuse_source": fhe_reuse.get("source_report"),
        "fhe_reuse_device_label": fhe_reuse.get("source_device_label"),
        "fhe_reuse_device_os": fhe_reuse.get("source_device_os"),
        "fhe_reuse_device_profile": fhe_reuse.get("source_device_profile"),
        "fhe_source_latency_samples": fhe_reuse.get("source_fhe_latency_sample_count"),
        "fhe_expanded_latency_rows": fhe_reuse.get("expanded_fhe_latency_row_count"),
        "fhe_latency_repetitions_per_source_sample": fhe_reuse.get(
            "expanded_fhe_latency_repetitions_per_source_sample"
        ),
        "report_tick_interval_seconds": performance.get("tick_interval_seconds"),
        "fhe_cloud_sample_interval_seconds": (
                (config.get("fhe_cloud") or {}).get("sample_interval_seconds")
                or (config.get("gateway_stream") or {}).get("fhe_cloud_sample_interval_seconds")
        ),
        "avg_tick_ms": latency.get("avg_total_tick_ms"),
        "p95_tick_ms": latency.get("p95_total_tick_ms"),
        "raw_events_per_s": performance.get("raw_events_per_wall_second"),
        "forecast_mae_w": quality.get("mae_w"),
        "benchmark": benchmark["totals"],
        "training_skipped": model_summary["training_skipped"],
        "model_rows": model_summary["example_count"],
        "model_report_mode": model_summary["mode"],
        "model_source": model_summary.get("source_model_path"),
        "best_model": comparison["best"],
        "runtime_model": comparison["selected"],
        "videos": [],
    }
    remove_stale_report_media(out)
    showcase = build_showcase_context(out, manifest)
    if videos_enabled:
        manifest["videos"] = write_report_videos(
            out=out,
            manifest=manifest,
            showcase=showcase,
            ffmpeg_bin=ffmpeg_bin,
            width=video_width,
            height=video_height,
            fps=video_fps,
        )
    showcase["artifact_tree"] = showcase_artifact_tree(out)
    showcase_html = out / "showcase.html"
    write_showcase_browser(showcase_html, manifest, showcase)
    manifest["showcase"] = {
        "browser": showcase_html.relative_to(out).as_posix(),
    }
    manifest_path = out / "manifest.json"
    write_json(manifest_path, manifest)
    write_readme(out / "README.md", manifest)
    return manifest_path


def remove_stale_report_media(out: Path) -> None:
    old_presentation_dir = out / "presentation"
    if old_presentation_dir.exists():
        shutil.rmtree(old_presentation_dir)
    for stale_pptx in out.glob("*.pptx"):
        stale_pptx.unlink()
    for filename in ("slides.md", "workflow_demo.html", "workflow_notes.md"):
        path = out / filename
        if path.exists():
            path.unlink()
    videos_dir = out / "videos"
    if videos_dir.exists():
        shutil.rmtree(videos_dir)
    videos_dir.mkdir(parents=True, exist_ok=True)


def write_readme(path: Path, manifest: dict[str, Any]) -> None:
    profile = manifest.get("device_profile", {})
    training_skipped = bool(manifest.get("training_skipped"))
    video_lines = []
    for video in manifest.get("videos", []):
        video_lines.append(
            f"- `{video['path']}`: {video['purpose']} ({video['duration_seconds']} s)"
        )
    if not video_lines:
        video_lines.append("- MP4 rendering was skipped for this report run.")
    model_contents = (
        "- `model/`: copied edge forecast model, inference summary, model metadata, and notes."
        if training_skipped
        else "- `model/`: training dataset, selected Ridge model, comparison table, and model notes."
    )
    model_headline = (
        f"- Training skipped: `true`; model source: `{manifest.get('model_source')}`"
        if training_skipped
        else f"- Best measured candidate: `{manifest['best_model']['model']}`"
    )
    fhe_lines = []
    if manifest.get("fhe_cloud_latency_samples"):
        fhe_lines.extend(
            [
                f"- FHE latency rows: `{manifest.get('fhe_cloud_latency_samples')}`",
                f"- FHE source measurements: `{manifest.get('fhe_source_latency_samples')}` repeated `{manifest.get('fhe_latency_repetitions_per_source_sample')}`x",
                f"- Average / p95 FHE cloud stage: `{manifest.get('fhe_cloud_avg_ms')}` ms / `{manifest.get('fhe_cloud_p95_ms')}` ms",
            ]
        )
    if manifest.get("fhe_reuse_source"):
        fhe_lines.append("- FHE evidence source: `miniPC/Linux reused report`")
        fhe_lines.append(
            f"- FHE source device/OS: `{manifest.get('fhe_reuse_device_label')}` / `{manifest.get('fhe_reuse_device_os')}`"
        )
    regenerate_args = " --skip-training" if training_skipped else ""
    lines = [
        "# Edge Gateway Report",
        "",
        "Compact report evidence for deployment review.",
        "",
        "## Device profile",
        "",
        f"- Hostname: `{profile.get('hostname')}`",
        f"- Platform: `{profile.get('platform')}`",
        f"- CPU cores visible to Python: `{profile.get('cpu_count')}`",
        f"- Containerized: `{profile.get('containerized')}`",
        f"- Configured devices: `{profile.get('configured_device_count')}`",
        "",
        "## Contents",
        "",
        "- `data/received_telemetry.csv`: input telemetry used for the report run.",
        "- `data/device_inventory.csv`: synthetic device list when `--source mock` is used.",
        "- `run/`: one traced gateway run with normalized events, snapshots, features, model inputs, model results, latency, and summaries.",
        "- `benchmark/`: summary-only benchmark, without duplicated per-run artifacts.",
        model_contents,
        "- `showcase.html`: browser report with the generated workflow videos.",
        "- `videos/`: MP4 files referenced by the browser report.",
        "",
        "## Headline numbers",
        "",
        f"- Inference cycles: `{manifest['cycles']}`",
        f"- Latency samples: `{manifest['latency_samples']}`",
        f"- Average / p95 tick latency: `{manifest['avg_tick_ms']}` ms / `{manifest['p95_tick_ms']}` ms",
        f"- Raw events/s: `{manifest['raw_events_per_s']}`",
        f"- Runtime model: `{manifest['runtime_model']['model']}`",
        *fhe_lines,
        model_headline,
        "",
        "## Videos",
        "",
        *video_lines,
        "",
        "## HTML",
        "",
        "- `showcase.html`: browser page with videos, scenarios, tables, and artifact index.",
        "",
        "## Regenerate",
        "",
        "```powershell",
        f".\\.venv\\Scripts\\python.exe main.py prepare-edge-report --output artifacts\\edge_report{regenerate_args}",
        "```",
        "",
        "For a deployed capture:",
        "",
        "```powershell",
        f".\\.venv\\Scripts\\python.exe main.py prepare-edge-report --source input --input artifacts\\zigbee_mqtt_capture.csv --config configs\\pilot_1_2_edge.json --output artifacts\\edge_report{regenerate_args}",
        "```",
        "",
        "For a synthetic demonstration package:",
        "",
        "```powershell",
        f".\\.venv\\Scripts\\python.exe main.py prepare-edge-report --source mock --output artifacts\\edge_report{regenerate_args}",
        "```",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_showcase_browser(
        path: Path,
        manifest: dict[str, Any],
        showcase: dict[str, Any],
) -> None:
    videos = manifest.get("videos", [])
    model_section_title = (
        "Model / Inference Summary"
        if manifest.get("training_skipped")
        else "Model Comparison"
    )
    if videos:
        video_cards = "\n".join(
            f"""
        <article class="video-card">
          <h3>{html.escape(video['title'])}</h3>
          <video controls preload="metadata">
            <source src="videos/{html.escape(Path(video['path']).name)}" type="video/mp4">
            <a href="videos/{html.escape(Path(video['path']).name)}">Open video file</a>
          </video>
          <p>{html.escape(video['purpose'])}</p>
        </article>
        """
            for video in videos
        )
    else:
        video_cards = """
        <article class="panel">
          <h3>No MP4 files in this run</h3>
          <p>Run the report without <code>--no-videos</code> to render and embed the walkthrough videos.</p>
        </article>
        """
    scenario_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(row['group'])}</td>"
        f"<td>{html.escape(row['scenario'])}</td>"
        f"<td>{html.escape(row['what_happens'])}</td>"
        f"<td>{html.escape(row['output'])}</td>"
        "</tr>"
        for row in showcase["scenario_catalog"]
    )
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>HyFHE-Net Edge Showcase</title>
<style>
:root {{ color-scheme: light; --ink:#15231b; --muted:#5d665c; --line:#c9d1c5; --green:#244333; --cream:#f7f2e6; --paper:#fffdf7; --accent:#9a6f2f; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; font:16px/1.45 "Segoe UI", Arial, sans-serif; background:var(--cream); color:var(--ink); }}
header {{ padding:32px 44px; background:var(--green); color:white; }}
h1 {{ margin:0 0 10px; font-size:42px; letter-spacing:0; }}
h2 {{ margin:34px 0 14px; font-size:26px; }}
h3 {{ margin:0 0 10px; font-size:19px; }}
.sub {{ max-width:980px; color:#e7efe3; font-size:18px; }}
.metrics {{ display:flex; flex-wrap:wrap; gap:10px; margin-top:18px; }}
.metric {{ border:1px solid #b8c9b6; border-radius:8px; padding:9px 12px; background:#fffdf714; }}
main {{ max-width:1280px; margin:0 auto; padding:30px 34px 50px; }}
.grid {{ display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:16px; }}
.two {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
.panel, .video-card {{ background:var(--paper); border:1px solid var(--line); border-radius:8px; padding:16px; }}
.video-card video {{ width:100%; aspect-ratio:16/9; background:#111c18; border-radius:6px; display:block; }}
.video-card p, .panel p {{ color:var(--muted); margin:8px 0 0; }}
code {{ font-family:Consolas, monospace; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; background:white; }}
th, td {{ border-bottom:1px solid #e3e7df; padding:7px 8px; text-align:left; vertical-align:top; }}
th {{ background:#eef3ea; font-weight:700; }}
pre {{ white-space:pre-wrap; overflow:auto; margin:0; padding:12px; border-radius:6px; background:#111c18; color:#e9f3e2; font:13px/1.45 Consolas, monospace; }}
.badge {{ display:inline-block; border-radius:999px; padding:5px 9px; background:#efe6d1; color:#61420f; margin-right:6px; margin-top:6px; }}
.artifact-list {{ columns:2; font:13px/1.5 Consolas, monospace; }}
a {{ color:#264c8a; }}
@media (max-width:900px) {{ .grid, .two {{ grid-template-columns:1fr; }} header {{ padding:28px 22px; }} main {{ padding:22px; }} }}
</style>
</head>
<body>
<header>
  <h1>HyFHE-Net Edge Gateway Showcase</h1>
  <p class="sub">A self-contained browser package for the generated report: live/replay input stream, processing stages, edge analytics and ML, cloud/FHE scenarios, deployment, and evidence outputs.</p>
  <div class="metrics">
    <span class="metric">{manifest['cycles']} inference cycles</span>
    <span class="metric">{manifest['latency_samples']} latency samples</span>
    <span class="metric">p95 tick {manifest['p95_tick_ms']} ms</span>
    <span class="metric">{manifest['raw_events']} raw events</span>
    <span class="metric">runtime model {html.escape(manifest['runtime_model']['model'])}</span>
  </div>
</header>
<main>
  <h2>Videos</h2>
  <section class="grid">{video_cards}</section>

  <h2>Input And Processing Evidence</h2>
  <section class="two">
    <article class="panel"><h3>Received Telemetry</h3>{html_table(showcase['telemetry'], ['timestamp','device','field','value','source'], 8)}</article>
    <article class="panel"><h3>Snapshots</h3>{html_table(showcase['snapshots'], ['timestamp','household_power_w','household_power_source','plug_power_w','temperature_c','humidity_pct'], 8)}</article>
    <article class="panel"><h3>Feature Windows</h3>{html_table(showcase['features'], ['timestamp','plug_power_mean_1m_w','household_power_mean_1m_w','device_to_household_ratio','plug_stale_flag'], 8)}</article>
    <article class="panel"><h3>Latency Samples</h3>{html_table(showcase['latency'], ['cycle','preprocessing_stage_ms','feature_stage_ms','fhe_cloud_sampled','fhe_cloud_stage_ms','model_stage_ms','total_tick_ms'], 8)}</article>
  </section>

  <h2>Edge Analytics / ML Outputs</h2>
  <section class="two">
    <article class="panel"><h3>Energy Profile Service</h3>{html_table(showcase['services'], ['timestamp','service_id','profile_state','activity_state','dominant_load_flag','appliance_share_pct'], 8)}</article>
    <article class="panel"><h3>Anomaly Monitor</h3>{html_table(showcase['anomaly_rows'], ['timestamp','inference_status','prediction_label','prediction_score','anomaly_score','details'], 8)}</article>
    <article class="panel"><h3>Short-Term Forecast</h3>{html_table(showcase['forecast_rows'], ['timestamp','inference_status','prediction_label','prediction_score','load_score'], 8)}</article>
    <article class="panel"><h3>Forecast Evaluation</h3>{html_table(showcase['evaluations'], ['timestamp','target_timestamp','predicted_household_power_w','target_household_power_w','absolute_error_w'], 8)}</article>
  </section>

  <h2>Cloud/FHE All-Model Contracts</h2>
  <section class="two">
    <article class="panel"><h3>Forecast Feature Rows</h3>{html_table(showcase['cloud_features'], ['timestamp','horizon_minutes','linky_household_power_w','plug_1_power_w','plug_2_power_w','monitored_plug_share'], 8)}</article>
    <article class="panel"><h3>NILM Feature Rows</h3>{html_table(showcase['nilm_features'], ['timestamp','linky_household_power_w','plug_1_power_w','plug_2_power_w','monitored_plug_share','load_event_type_code'], 8)}</article>
    <article class="panel"><h3>Cohort Feature Rows</h3>{html_table(showcase['cohort_features'], ['timestamp','total_energy_kwh','mean_power_w','peak_power_w','monitored_plug_energy_share','event_rate_per_day'], 8)}</article>
    <article class="panel"><h3>Combined Contract File</h3><pre>{html.escape(json.dumps(showcase['cloud_contracts'], indent=2)[:3200])}</pre></article>
  </section>

  <h2>Cloud/FHE Output Rows</h2>
  <section class="two">
    <article class="panel"><h3>Forecast Endpoint</h3>{html_table(showcase['fhe_rows'], ['timestamp','inference_status','prediction_label','prediction_score','details'], 8)}</article>
    <article class="panel"><h3>NILM Endpoint</h3>{html_table(showcase['fhe_nilm_rows'], ['timestamp','inference_status','prediction_label','prediction_score','details'], 8)}</article>
    <article class="panel"><h3>Cohort Endpoint</h3>{html_table(showcase['fhe_cohort_rows'], ['timestamp','inference_status','prediction_label','prediction_score','details'], 8)}</article>
  </section>

  <h2>Scenario Catalog</h2>
  <section class="panel">
    <table><thead><tr><th>Group</th><th>Scenario</th><th>Operation</th><th>Output</th></tr></thead><tbody>{scenario_rows}</tbody></table>
  </section>

  <h2>{model_section_title}</h2>
  <section class="panel">{html_table(showcase['comparison'], ['rank','model','kind','features','mae_w','rmse_w','r2','selected'], 10)}</section>

  <h2>Artifact Index</h2>
  <section class="panel"><div class="artifact-list">{'<br>'.join(html.escape(item) for item in showcase['artifact_tree'])}</div></section>
</main>
</body>
</html>
"""
    path.write_text(html_doc, encoding="utf-8")


def html_table(rows: list[dict[str, Any]], columns: list[str], limit: int = 8) -> str:
    if not rows:
        return "<p>No rows were generated for this section.</p>"
    header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    body = []
    for row in rows[:limit]:
        body.append(
            "<tr>"
            + "".join(
                f"<td>{html.escape(compact_text(row.get(column), 80))}</td>"
                for column in columns
            )
            + "</tr>"
        )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def collect_device_profile(config: dict[str, Any]) -> dict[str, Any]:
    devices = config.get("devices") or {}
    role_counts: dict[str, int] = {}
    for details in devices.values():
        role = str(details.get("role", "unknown"))
        role_counts[role] = role_counts.get(role, 0) + 1
    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "containerized": Path("/.dockerenv").exists()
                         or os.environ.get("HYFHENET_CONTAINERIZED") == "1",
        "configured_device_count": len(devices),
        "configured_device_roles": role_counts,
        "configured_pipeline_interval_seconds": (
                config.get("pipeline") or {}
        ).get("interval_seconds"),
        "mqtt_host": (config.get("zigbee_gateway") or {}).get("host"),
        "mqtt_topic": (config.get("zigbee_gateway") or {}).get("topic"),
    }


def write_report_videos(
        out: Path,
        manifest: dict[str, Any],
        showcase: dict[str, Any],
        ffmpeg_bin: str,
        width: int,
        height: int,
        fps: int,
) -> list[dict[str, Any]]:
    resolved_ffmpeg = resolve_executable(ffmpeg_bin)
    if resolved_ffmpeg is None:
        raise RuntimeError(
            f"ffmpeg executable not found: {ffmpeg_bin}. Install ffmpeg or pass --no-videos."
        )

    videos_dir = out / "videos"
    videos_dir.mkdir(parents=True, exist_ok=True)
    for stale_video in videos_dir.glob("*.mp4"):
        stale_video.unlink()
    edge_video_purpose = (
        "Explanation of local edge services, anomaly monitor, Ridge forecast, inference metrics, and output artifacts."
        if manifest.get("training_skipped")
        else "Explanation of local edge services, anomaly monitor, Ridge forecast, model comparison, and output artifacts."
    )
    specs = [
        {
            "title": "Workflow walkthrough",
            "filename": "workflow_walkthrough.mp4",
            "purpose": "End-to-end walkthrough from Zigbee/MQTT telemetry to evidence artifacts.",
            "slides": workflow_video_slides(manifest),
        },
        {
            "title": "Device report metrics",
            "filename": "device_report_metrics.mp4",
            "purpose": "Device-derived latency, throughput, model, benchmark, and deployment evidence.",
            "slides": metrics_video_slides(manifest),
        },
        {
            "title": "Input stream and processing",
            "filename": "01_input_stream_processing.mp4",
            "purpose": "Screen-recording-style walkthrough of telemetry ingestion, normalization, snapshots, features, event gates, and latency.",
            "slides": input_processing_video_slides(manifest, showcase),
        },
        {
            "title": "Edge analytics and ML outputs",
            "filename": "02_edge_models_outputs.mp4",
            "purpose": edge_video_purpose,
            "slides": edge_analytics_video_slides(manifest, showcase),
        },
        {
            "title": "Cloud FHE scenarios",
            "filename": "03_cloud_fhe_scenarios.mp4",
            "purpose": "Forecast, NILM, and cohort contracts, encrypted inference path, unavailable fallback, and model-cache refresh behavior.",
            "slides": fhe_video_slides(manifest, showcase),
        },
        {
            "title": "Deployment and report generation",
            "filename": "04_deployment_report_generation.mp4",
            "purpose": "Docker deployment, live capture, report regeneration, package layout, and how to reuse the artifacts.",
            "slides": deployment_video_slides(manifest, showcase),
        },
    ]
    rendered = []
    for index, spec in enumerate(specs, start=1):
        output_path = videos_dir / spec["filename"]
        report_progress(
            "rendering report video",
            video=f"{index}/{len(specs)}",
            file=spec["filename"],
        )
        write_report_status(
            out,
            "rendering report video",
            video=f"{index}/{len(specs)}",
            file=spec["filename"],
        )
        render_video(
            ffmpeg_bin=resolved_ffmpeg,
            output_path=output_path,
            slides=spec["slides"],
            width=width,
            height=height,
            fps=fps,
        )
        rendered.append(
            {
                "title": spec["title"],
                "path": output_path.relative_to(out).as_posix(),
                "purpose": spec["purpose"],
                "duration_seconds": round(
                    sum(slide.duration_seconds for slide in spec["slides"]),
                    2,
                ),
                "width": width,
                "height": height,
                "fps": fps,
            }
        )
    write_video_script(videos_dir / "video_script.md", specs)
    return rendered


def workflow_video_slides(manifest: dict[str, Any]) -> list[VideoSlide]:
    profile = manifest.get("device_profile", {})
    model_mode_line = (
        "The report used the existing edge forecast model artifact and skipped training."
        if manifest.get("training_skipped")
        else f"The report trained {manifest['model_rows']} supervised examples for the local model."
    )
    evidence_model_line = (
        "It also writes latency CSV, benchmark summaries, inference summaries, HTML, and MP4 videos."
        if manifest.get("training_skipped")
        else "It also writes latency CSV, benchmark summaries, model comparison, HTML, and MP4 videos."
    )
    return [
        VideoSlide(
            "HyFHE-Net edge gateway",
            [
                "Deployment-ready edge evidence package.",
                f"Generated on {profile.get('hostname')} with {profile.get('configured_device_count')} configured devices.",
                f"{manifest['cycles']} inference cycles and {manifest['latency_samples']} latency samples were measured.",
            ],
            duration_seconds=7.0,
        ),
        VideoSlide(
            "1. Edge device deployment",
            [
                "Docker runs the same gateway on another edge device.",
                "Environment variables point it at Zigbee2MQTT, the FHE API, and local cache volumes.",
                "The mounted artifacts volume keeps captures and reports outside the container.",
            ],
        ),
        VideoSlide(
            "2. Telemetry capture",
            [
                "The gateway subscribes to Zigbee2MQTT or replays a compatible CSV capture.",
                "Smart plugs, room climate sensors, and optional household-meter readings share one normalized input shape.",
                "A live deployment can capture first, then regenerate this same report from that device's data.",
            ],
        ),
        VideoSlide(
            "3. Normalization and snapshots",
            [
                "Raw MQTT fields are validated and mapped into device state.",
                "Household power uses meter readings when present and plug sums as the fallback.",
                "Fixed-interval snapshots keep downstream services deterministic.",
            ],
        ),
        VideoSlide(
            "4. Features and event gate",
            [
                "Rolling power, staleness, device-share, and context features are derived on the edge.",
                "The load-event gate marks switch and ramp events before cloud handoff.",
                "These artifacts make the runtime path auditable after deployment.",
            ],
        ),
        VideoSlide(
            "5. Cloud/FHE model path",
            [
                "The edge builds forecast, NILM, and cohort feature rows from fixed contracts every run.",
                "With credentials, the client encrypts each input, calls the FHE service, and records decrypted outputs.",
                "Without cloud access, the gateway records unavailable statuses and continues locally.",
            ],
        ),
        VideoSlide(
            "6. Local models and output",
            [
                "Energy profile, anomaly monitor, and one-minute Ridge forecast run on the edge CPU.",
                model_mode_line,
                f"Runtime model: {manifest['runtime_model']['model']}.",
            ],
        ),
        VideoSlide(
            "7. Evidence package",
            [
                "The report includes telemetry, normalized events, snapshots, features, model inputs, and results.",
                evidence_model_line,
                "All headline numbers in this video come from the generated manifest.",
            ],
            duration_seconds=7.0,
        ),
    ]


def metrics_video_slides(manifest: dict[str, Any]) -> list[VideoSlide]:
    device_lines = presentation_device_lines(manifest)
    latency_lines = [
        f"Average total tick latency: {manifest['avg_tick_ms']} ms",
        f"p95 total tick latency: {manifest['p95_tick_ms']} ms",
        f"Raw event throughput: {manifest['raw_events_per_s']} events per second",
    ]
    if manifest.get("fhe_cloud_latency_samples"):
        latency_lines.extend(
            [
                f"FHE cloud latency rows: {manifest.get('fhe_cloud_latency_samples')}",
                f"FHE source samples: {manifest.get('fhe_source_latency_samples')} repeated {manifest.get('fhe_latency_repetitions_per_source_sample')}x",
                f"FHE cloud avg / p95: {manifest.get('fhe_cloud_avg_ms')} ms / {manifest.get('fhe_cloud_p95_ms')} ms",
            ]
        )
    if manifest.get("fhe_reuse_source"):
        latency_lines.append(
            f"FHE latency source: x86 miniPC on {manifest.get('fhe_reuse_device_os')}."
        )
    else:
        latency_lines.append("The values are produced by the gateway run on this device.")
    forecast_lines = [
        f"Forecast MAE: {manifest['forecast_mae_w']} W",
        f"Runtime model: {manifest['runtime_model']['model']}",
    ]
    if manifest.get("training_skipped"):
        forecast_lines.extend(
            [
                "Training was skipped; quality is from observed replay evaluations.",
                f"Model source: {manifest.get('model_source')}",
            ]
        )
    else:
        forecast_lines.extend(
            [
                f"Best measured candidate: {manifest['best_model']['model']}",
                "The model comparison table is included in the report package.",
            ]
        )
    return [
        VideoSlide(
            "Device-derived report",
            device_lines,
        ),
        VideoSlide(
            "Measured pipeline volume",
            [
                f"Raw telemetry events: {manifest['raw_events']}",
                f"Inference cycles: {manifest['cycles']}",
                f"Model results: {manifest['model_results']}",
                f"Latency samples: {manifest['latency_samples']}",
            ],
        ),
        VideoSlide(
            "Latency and throughput",
            latency_lines,
        ),
        VideoSlide(
            "Forecast quality and model choice",
            forecast_lines,
        ),
        VideoSlide(
            "Benchmark evidence",
            [
                f"Benchmark runs: {manifest['benchmark']['runs']}",
                f"Benchmark cycles: {manifest['benchmark']['cycles']}",
                f"Benchmark model results: {manifest['benchmark']['model_results']}",
                f"Mean p95 tick latency: {manifest['benchmark']['mean_p95_tick_ms']} ms",
            ],
        ),
        VideoSlide(
            "Regenerate on any edge device",
            [
                "Run the Docker gateway to capture live MQTT telemetry.",
                "Run prepare-edge-report against the captured CSV on the target device.",
                "The report and videos are rebuilt from that device's metrics and runtime profile.",
            ],
            duration_seconds=7.0,
        ),
    ]


def input_processing_video_slides(
        manifest: dict[str, Any],
        showcase: dict[str, Any],
) -> list[VideoSlide]:
    normalized = showcase.get("normalized") or []
    normalized_lines = []
    if normalized:
        first = normalized[0]
        normalized_lines = [
            "raw:",
            *json_preview(first.get("raw", {}), indent=2, limit=5),
            "normalized:",
            *json_preview(first.get("normalized", {}), indent=2, limit=6),
        ]
    return [
        VideoSlide(
            "Input stream: Zigbee2MQTT / replay CSV",
            [
                "The gateway accepts live MQTT, capture mode, or deterministic CSV replay.",
                "Every input row has timestamp, device, field, value, and source.",
                "The report uses the same replay-compatible shape that live capture writes on an edge device.",
            ],
            duration_seconds=7.0,
            screen_title="received_telemetry.csv",
            screen_lines=screen_table(
                showcase["telemetry"],
                ["timestamp", "device", "field", "value", "source"],
                ["timestamp", "device", "field", "value", "source"],
                limit=7,
            ),
        ),
        VideoSlide(
            "Normalization: raw fields become device state",
            [
                "Device IDs are mapped to configured roles.",
                "Power, energy, climate, battery, and Linky/TIC fields are converted into typed normalized events.",
                "Unsupported or malformed fields stay out of the downstream model contract.",
            ],
            duration_seconds=7.0,
            screen_title="normalized_events.jsonl",
            screen_lines=normalized_lines,
        ),
        VideoSlide(
            "Aligned snapshots: fixed inference ticks",
            [
                "The pipeline emits a household snapshot every configured interval.",
                "Meter power wins when present; otherwise smart-plug power is summed.",
                "Snapshots make feature windows and latency samples comparable across deployments.",
            ],
            screen_title="snapshots.csv",
            screen_lines=screen_table(
                showcase["snapshots"],
                [
                    "timestamp",
                    "household_power_w",
                    "household_power_source",
                    "plug_power_w",
                    "temperature_c",
                    "humidity_pct",
                ],
                ["timestamp", "household W", "source", "plug W", "temp C", "humidity"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Feature engineering: rolling context",
            [
                "The edge derives rolling means, deltas, device share, staleness, and environment context.",
                "These values feed event gates, edge services, local ML, and cloud/FHE feature rows.",
                "Missing indicators keep the numeric contract deterministic.",
            ],
            screen_title="features.csv",
            screen_lines=screen_table(
                showcase["features"],
                [
                    "timestamp",
                    "plug_power_mean_1m_w",
                    "household_power_mean_1m_w",
                    "device_to_household_ratio",
                    "plug_stale_flag",
                    "household_stale_flag",
                ],
                ["timestamp", "plug 1m", "home 1m", "share", "plug stale", "home stale"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Event gate: switch and ramp detection",
            [
                "The load-event gate watches power thresholds and deltas.",
                "It emits initial-state, switch, and ramp markers, each with forwarding confidence.",
                "Those markers explain why a row is relevant for cloud/FHE handoff or local interpretation.",
            ],
            screen_title="edge_results.jsonl / load_event_marker",
            screen_lines=screen_table(
                showcase["markers"],
                [
                    "timestamp",
                    "detector_id",
                    "event_type",
                    "device_id",
                    "confidence",
                    "should_forward",
                ],
                ["timestamp", "detector", "event", "device", "conf", "fwd"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Latency instrumentation: each stage is measured",
            [
                "Every inference tick records per-stage and total latency.",
                "The report requires at least 100 samples so p95/p99 numbers are meaningful.",
                f"This run measured p95 total tick latency of {manifest['p95_tick_ms']} ms.",
            ],
            duration_seconds=7.0,
            screen_title="latency.csv",
            screen_lines=screen_table(
                showcase["latency"],
                [
                    "cycle",
                    "preprocessing_stage_ms",
                    "feature_stage_ms",
                    "fhe_cloud_sampled",
                    "fhe_cloud_stage_ms",
                    "model_stage_ms",
                    "total_tick_ms",
                ],
                ["cycle", "pre ms", "feat ms", "FHE run", "FHE ms", "ML ms", "total ms"],
                limit=6,
            ),
        ),
    ]


def edge_analytics_video_slides(
        manifest: dict[str, Any],
        showcase: dict[str, Any],
) -> list[VideoSlide]:
    comparison_title = (
        "Inference summary: existing runtime model"
        if manifest.get("training_skipped")
        else "Model comparison: selected runtime model"
    )
    comparison_lines = (
        [
            "The report did not fit local model candidates.",
            "It copied the configured edge model and measured inference behavior during replay.",
            "The summary table keeps observed forecast quality and model-stage latency.",
        ]
        if manifest.get("training_skipped")
        else [
            "The report compares simple baselines, linear regression, and Ridge candidates.",
            "The deployed family stays Ridge because it is deterministic, compact, and already integrated.",
            "The comparison table is kept so live deployments can retune using their own data.",
        ]
    )
    comparison_screen_title = (
        "model_comparison.csv / inference summary"
        if manifest.get("training_skipped")
        else "model_comparison.csv"
    )
    return [
        VideoSlide(
            "Local service: energy profile",
            [
                "The service layer is deterministic and edge-only.",
                "It labels activity context such as standby, active load, dominant load, and data quality.",
                "This creates explainable outputs even when cloud access is unavailable.",
            ],
            screen_title="edge_service_result records",
            screen_lines=screen_table(
                showcase["services"],
                [
                    "timestamp",
                    "service_id",
                    "profile_state",
                    "activity_state",
                    "dominant_load_flag",
                    "appliance_share_pct",
                ],
                ["timestamp", "service", "profile", "activity", "dominant", "share"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Anomaly monitor: online statistics",
            [
                "The anomaly monitor starts in warmup, then compares current load and load share against rolling statistics.",
                "Outputs include label, confidence, anomaly score, load score, and short details.",
                "It is designed for low-cost CPU execution on the gateway.",
            ],
            screen_title="energy_anomaly_monitor outputs",
            screen_lines=screen_table(
                showcase["anomaly_rows"],
                [
                    "timestamp",
                    "inference_status",
                    "prediction_label",
                    "prediction_score",
                    "anomaly_score",
                    "details",
                ],
                ["timestamp", "status", "label", "score", "anom", "details"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Short-term Ridge forecast: one dot product",
            [
                "The local load forecast is a compact JSON Ridge model.",
                "At runtime it uses the feature vector and coefficients to predict household load one minute ahead.",
                "The output is written as a standard model_inference_result record.",
            ],
            screen_title="edge_short_term_load_forecast outputs",
            screen_lines=screen_table(
                showcase["forecast_rows"],
                [
                    "timestamp",
                    "inference_status",
                    "prediction_label",
                    "prediction_score",
                    "load_score",
                    "details",
                ],
                ["timestamp", "status", "label", "pred W", "load W", "details"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Forecast evaluation: target observed later",
            [
                "Forecasts become measurable when their target timestamp appears in the stream.",
                "The report records predicted value, observed target, signed error, and absolute error.",
                f"Overall forecast MAE in this run: {manifest['forecast_mae_w']} W.",
            ],
            screen_title="edge_forecast_evaluation records",
            screen_lines=screen_table(
                showcase["evaluations"],
                [
                    "timestamp",
                    "target_timestamp",
                    "predicted_household_power_w",
                    "target_household_power_w",
                    "absolute_error_w",
                ],
                ["forecast ts", "target ts", "pred W", "target W", "abs err"],
                limit=6,
            ),
        ),
        VideoSlide(
            comparison_title,
            comparison_lines,
            duration_seconds=7.0,
            screen_title=comparison_screen_title,
            screen_lines=screen_table(
                showcase["comparison"],
                ["rank", "model", "kind", "features", "mae_w", "rmse_w", "r2", "selected"],
                ["rank", "model", "kind", "feat", "MAE", "RMSE", "R2", "sel"],
                limit=7,
            ),
        ),
    ]


def fhe_video_slides(
        manifest: dict[str, Any],
        showcase: dict[str, Any],
) -> list[VideoSlide]:
    contracts = showcase["cloud_contracts"]
    endpoint_lines = []
    for name in ("forecast", "nilm", "cohort"):
        contract = contracts.get(name, {})
        endpoint_lines.append(
            f"{name}: endpoint={contract.get('model_endpoint')} features={len(contract.get('numeric_feature_columns', []))}"
        )
    success_record = {
        "timestamp": "2026-04-29T13:00:00",
        "model_id": "fhe_nilm_disaggregation",
        "inference_status": "ok",
        "prediction_label": "fhe_nilm_disaggregation",
        "prediction_score": "sum_of_predicted_components_w",
        "details": "encrypted request -> decrypted appliance components",
    }
    status_counts = manifest.get("fhe_cloud_status_counts") or {}
    observed_fhe_lines = [
        f"FHE result statuses: ok={status_counts.get('ok', 0)}, unavailable={status_counts.get('unavailable', 0)}.",
        f"FHE cloud latency rows: {manifest.get('fhe_cloud_latency_samples')}.",
        f"Source FHE measurements: {manifest.get('fhe_source_latency_samples')} repeated {manifest.get('fhe_latency_repetitions_per_source_sample')}x.",
        f"FHE cloud avg / p95 latency: {manifest.get('fhe_cloud_avg_ms')} ms / {manifest.get('fhe_cloud_p95_ms')} ms.",
    ]
    observed_fhe_title = "FHE outputs observed in this report"
    if manifest.get("fhe_reuse_source"):
        observed_fhe_title = "FHE outputs reused from miniPC report"
        observed_fhe_lines.append(
            f"FHE source device: x86 miniPC running {manifest.get('fhe_reuse_device_os')}."
        )
        observed_fhe_lines.append("The fresh replay supplies the non-FHE stage and edge-model timings.")
    else:
        observed_fhe_lines.append("The gateway records unavailable rows when a cloud call cannot complete.")
    return [
        VideoSlide(
            "Cloud/FHE model contracts",
            [
                "The edge prepares runtime rows for the three cloud models.",
                "Training datasets, compiled server packages, and accuracy reports remain cloud-owned.",
                "The report writes feature CSV files and a combined contract JSON for audit.",
            ],
            screen_title="cloud_model_feature_contracts.json",
            screen_lines=endpoint_lines,
        ),
        VideoSlide(
            "Forecast row: long-horizon load",
            [
                "The forecast endpoint receives time, target-horizon, load, plug, environment, and event features.",
                "The local one-minute Ridge model uses the same ordered feature family.",
                "The cloud model predicts household power at the configured future horizon.",
            ],
            screen_title="cloud_forecast_features.csv",
            screen_lines=screen_table(
                showcase["cloud_features"],
                [
                    "timestamp",
                    "horizon_minutes",
                    "linky_household_power_w",
                    "plug_1_power_w",
                    "plug_2_power_w",
                    "monitored_plug_share",
                    "load_event_type_code",
                ],
                ["timestamp", "horizon", "home W", "plug1 W", "plug2 W", "share", "event"],
                limit=6,
            ),
        ),
        VideoSlide(
            "NILM row: disaggregation",
            [
                "The NILM endpoint receives the current load-shape row without forecast horizon columns.",
                "It returns plug 1, plug 2, HVAC, baseload, and other power components.",
                "This keeps appliance breakdown in the encrypted cloud path.",
            ],
            screen_title="cloud_nilm_features.csv",
            screen_lines=screen_table(
                showcase["nilm_features"],
                [
                    "timestamp",
                    "linky_household_power_w",
                    "plug_1_power_w",
                    "plug_2_power_w",
                    "monitored_plug_share",
                    "load_event_type_code",
                ],
                ["timestamp", "home W", "plug1 W", "plug2 W", "share", "event"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Cohort row: daily benchmark",
            [
                "The cohort endpoint receives daily aggregate inputs derived at the edge.",
                "Label metadata is excluded from inference to avoid leakage.",
                "The output is a cohort code for encrypted household benchmarking.",
            ],
            screen_title="cloud_cohort_features.csv",
            screen_lines=screen_table(
                showcase["cohort_features"],
                [
                    "timestamp",
                    "total_energy_kwh",
                    "mean_power_w",
                    "peak_power_w",
                    "monitored_plug_energy_share",
                    "event_rate_per_day",
                ],
                ["timestamp", "kWh", "mean W", "peak W", "plug share", "events/day"],
                limit=6,
            ),
        ),
        VideoSlide(
            "Remote FHE success path",
            [
                "When the client stack and credentials are configured, plaintext features are encrypted locally.",
                "The encrypted payload is posted to the matching forecast, NILM, or cohort endpoint.",
                "The gateway decrypts and records a normal model output with status `ok`.",
            ],
            duration_seconds=7.0,
            screen_title="documented NILM success output shape",
            screen_lines=json_preview(success_record, indent=2, limit=12),
        ),
        VideoSlide(
            observed_fhe_title,
            observed_fhe_lines,
            duration_seconds=7.0,
            screen_title="actual FHE outputs in this report",
            screen_lines=screen_table(
                showcase["fhe_rows"] + showcase["fhe_nilm_rows"] + showcase["fhe_cohort_rows"],
                [
                    "timestamp",
                    "model_id",
                    "inference_status",
                    "prediction_label",
                    "prediction_score",
                    "details",
                ],
                ["timestamp", "model", "status", "label", "score", "details"],
                limit=9,
                ),
        ),
        VideoSlide(
            "FHE scenario coverage",
            [
                "The scenario catalog lists the model contracts, success path, fallback path, and cache-refresh behavior.",
                "Each report regenerates these rows from the device run.",
                "Reviewers can compare CSV evidence, result rows, and the HTML page side by side.",
            ],
            screen_title="scenario catalog / FHE Cloud",
            screen_lines=screen_table(
                [
                    row
                    for row in showcase["scenario_catalog"]
                    if row["group"] in {"Cloud feature contracts", "FHE Cloud"}
                ],
                ["scenario", "what_happens", "output"],
                ["scenario", "operation", "output"],
                limit=7,
            ),
        ),
    ]


def deployment_video_slides(
        manifest: dict[str, Any],
        showcase: dict[str, Any],
) -> list[VideoSlide]:
    report_action_line = (
        "It uses the existing edge model, benchmarks replay throughput, renders videos, and writes the browser HTML."
        if manifest.get("training_skipped")
        else "It trains the local model, benchmarks replay throughput, renders videos, and writes the browser HTML."
    )
    package_layout_line = (
        "The report folder contains raw input, traced run artifacts, inference summaries, benchmark summaries, browser HTML, and videos."
        if manifest.get("training_skipped")
        else "The report folder contains raw input, traced run artifacts, model comparison, benchmark summaries, browser HTML, and videos."
    )
    deployment_device_lines = presentation_device_lines(manifest)
    if manifest.get("fhe_reuse_source"):
        deployment_device_lines.append(
            f"FHE source samples: {manifest.get('fhe_source_latency_samples')} values expanded to {manifest.get('fhe_expanded_latency_rows')} latency rows."
        )
    report_command_lines = [
        "docker run --rm \\",
        "  -v ./artifacts:/app/artifacts \\",
        "  hyfhenet-edge prepare-edge-report \\",
        "    --source input \\",
        "    --input artifacts/zigbee_mqtt_capture.csv \\",
    ]
    if manifest.get("training_skipped"):
        report_command_lines.extend(
            [
                "    --output artifacts/edge_report \\",
                "    --skip-training",
            ]
        )
    else:
        report_command_lines.append("    --output artifacts/edge_report")
    if manifest.get("fhe_reuse_source"):
        if report_command_lines[-1].endswith("--skip-training"):
            report_command_lines[-1] = "    --skip-training \\"
        else:
            report_command_lines[-1] = report_command_lines[-1] + " \\"
        report_command_lines.append("    --reuse-fhe-report artifacts/edge_report_miniPC")
    return [
        VideoSlide(
            "Deploy the gateway on another edge device",
            [
                "The report target is an x86 miniPC edge gateway running Linux.",
                "The Docker image runs the gateway, capture flow, report generator, and ffmpeg video renderer.",
                "Configure MQTT and FHE settings with environment variables or Compose.",
            ],
            duration_seconds=7.0,
            screen_title="docker run / live gateway",
            screen_lines=[
                "docker build -t hyfhenet-edge .",
                "docker run --rm \\",
                "  -e HYFHENET_ZIGBEE_HOST=<broker-host> \\",
                "  -e HYFHENET_ZIGBEE_PORT=1883 \\",
                "  -v ./artifacts:/app/artifacts \\",
                "  -v ./.hyfhenet-cache:/app/.cache/hyfhenet \\",
                "  hyfhenet-edge mqtt-live --continuous",
            ],
        ),
        VideoSlide(
            "Capture an evidence window",
            [
                "Capture mode writes replay-compatible CSV and can run the same analytics pipeline live.",
                "The captured CSV is the input for a device-specific report.",
                "Use a longer listen window on real deployments for stable latency and forecast-quality numbers.",
            ],
            screen_title="docker run / capture",
            screen_lines=[
                "docker run --rm \\",
                "  -e HYFHENET_ZIGBEE_LISTEN_SECONDS=600 \\",
                "  -v ./artifacts:/app/artifacts \\",
                "  hyfhenet-edge mqtt-capture \\",
                "    --capture-output artifacts/zigbee_mqtt_capture.csv \\",
                "    --output artifacts/gateway_mqtt_capture \\",
                "    --run-pipeline --log-level progress",
            ],
        ),
        VideoSlide(
            "Regenerate report, HTML, and videos",
            [
                "The report command derives all metrics from the target device run.",
                report_action_line,
                f"This generated package measured {manifest['latency_samples']} latency samples.",
            ],
            duration_seconds=7.0,
            screen_title="docker run / report",
            screen_lines=report_command_lines,
        ),
        VideoSlide(
            "Package layout for review",
            [
                package_layout_line,
                "The same structure is produced on every deployed edge device.",
                "Reviewers can open the browser HTML and then inspect raw CSV/JSON evidence.",
            ],
            screen_title="artifacts/edge_report",
            screen_lines=showcase["artifact_tree"][:14],
        ),
        VideoSlide(
            "Device-specific numbers",
            [
                "The report is presented as miniPC/Linux edge-gateway evidence.",
                "The latency CSV expands the 15 miniPC FHE measurements evenly across all inference rows.",
                "This makes side-by-side edge-device comparison straightforward.",
            ],
            screen_title="manifest.json / device profile",
            screen_lines=[
                *deployment_device_lines,
                f"avg_tick_ms: {manifest.get('avg_tick_ms')}",
                f"p95_tick_ms: {manifest.get('p95_tick_ms')}",
                f"raw_events_per_s: {manifest.get('raw_events_per_s')}",
            ],
        ),
    ]


def json_preview(payload: Any, indent: int = 2, limit: int = 10) -> list[str]:
    text = json.dumps(payload, indent=indent, ensure_ascii=True, sort_keys=True)
    return text.splitlines()[:limit]


def render_video(
        ffmpeg_bin: str,
        output_path: Path,
        slides: list[VideoSlide],
        width: int,
        height: int,
        fps: int,
) -> None:
    build_dir = output_path.parent / f"_{output_path.stem}_build"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)
    try:
        slide_paths = []
        for index, slide in enumerate(slides, start=1):
            slide_path = build_dir / f"slide_{index:03d}.mp4"
            render_slide(
                ffmpeg_bin=ffmpeg_bin,
                slide=slide,
                output_path=slide_path,
                build_dir=build_dir,
                index=index,
                width=width,
                height=height,
                fps=fps,
            )
            slide_paths.append(slide_path)
        concat_path = build_dir / "concat.txt"
        concat_path.write_text(
            "\n".join(f"file '{concat_path_for(path)}'" for path in slide_paths) + "\n",
            encoding="utf-8",
            )
        run_ffmpeg(
            [
                ffmpeg_bin,
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_path),
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                str(output_path),
            ]
        )
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def render_slide(
        ffmpeg_bin: str,
        slide: VideoSlide,
        output_path: Path,
        build_dir: Path,
        index: int,
        width: int,
        height: int,
        fps: int,
) -> None:
    font_path = find_font_file()
    mono_font_path = find_mono_font_file() or font_path
    title_file = write_text_fragment(build_dir, f"title_{index:03d}.txt", slide.title)
    footer_file = write_text_fragment(build_dir, f"footer_{index:03d}.txt", slide.footer)
    filters = [
        "drawbox=x=0:y=0:w=iw:h=96:color=0x233a2d@1:t=fill",
        f"drawbox=x=0:y={height - 36}:w=iw:h=36:color=0x233a2d@1:t=fill",
        drawtext_filter(
            title_file,
            font_path,
            size=44,
            color="white",
            x=64,
            y=26,
        ),
    ]
    if slide.screen_lines:
        screen_x = 46
        screen_y = 126
        screen_w = max(640, int(width * 0.61))
        screen_h = height - screen_y - 72
        notes_x = screen_x + screen_w + 38
        filters.extend(
            [
                f"drawbox=x={screen_x}:y={screen_y}:w={screen_w}:h={screen_h}:color=0x111c18@1:t=fill",
                f"drawbox=x={screen_x}:y={screen_y}:w={screen_w}:h=40:color=0x2f4f3d@1:t=fill",
                f"drawbox=x={screen_x}:y={screen_y}:w={screen_w}:h={screen_h}:color=0x7ca982@1:t=2",
            ]
        )
        screen_title = write_text_fragment(
            build_dir,
            f"slide_{index:03d}_screen_title.txt",
            slide.screen_title or "gateway screen",
            )
        filters.append(
            drawtext_filter(
                screen_title,
                font_path,
                size=19,
                color="0xf3f7ed",
                x=screen_x + 18,
                y=screen_y + 11,
            )
        )
        y = screen_y + 58
        for line_index, line in enumerate(wrap_screen_lines(slide.screen_lines), start=1):
            text_file = write_text_fragment(
                build_dir,
                f"slide_{index:03d}_screen_{line_index:02d}.txt",
                line,
            )
            filters.append(
                drawtext_filter(
                    text_file,
                    mono_font_path,
                    size=17,
                    color="0xe6f0de",
                    x=screen_x + 20,
                    y=y,
                )
            )
            y += 25
        y = 130
        for line_index, line in enumerate(wrap_notes_lines(slide.lines), start=1):
            text_file = write_text_fragment(
                build_dir,
                f"slide_{index:03d}_note_{line_index:02d}.txt",
                line,
            )
            filters.append(
                drawtext_filter(
                    text_file,
                    font_path,
                    size=26,
                    color="0x162018",
                    x=notes_x,
                    y=y,
                )
            )
            y += 42
    else:
        y = 142
        for line_index, line in enumerate(wrap_slide_lines(slide.lines), start=1):
            text_file = write_text_fragment(
                build_dir,
                f"slide_{index:03d}_line_{line_index:02d}.txt",
                line,
            )
            filters.append(
                drawtext_filter(
                    text_file,
                    font_path,
                    size=30,
                    color="0x162018",
                    x=74,
                    y=y,
                )
            )
            y += 46
    filters.append(
        drawtext_filter(
            footer_file,
            font_path,
            size=20,
            color="0xf8f2df",
            x=64,
            y=height - 28,
        )
    )
    run_ffmpeg(
        [
            ffmpeg_bin,
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"color=c=0xf6f0df:s={width}x{height}:d={slide.duration_seconds}:r={fps}",
            "-vf",
            ",".join(filters),
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-t",
            str(slide.duration_seconds),
            str(output_path),
        ]
    )


def drawtext_filter(
        text_file: Path,
        font_path: Path | None,
        size: int,
        color: str,
        x: int,
        y: int,
) -> str:
    options = []
    if font_path is not None:
        options.append(f"fontfile={filter_path(font_path)}")
    options.extend(
        [
            f"textfile={filter_path(text_file)}",
            f"fontsize={size}",
            f"fontcolor={color}",
            f"x={x}",
            f"y={y}",
            "line_spacing=8",
        ]
    )
    return "drawtext=" + ":".join(options)


def wrap_slide_lines(lines: list[str]) -> list[str]:
    wrapped = []
    for line in lines:
        wrapped.extend(textwrap.wrap(line, width=62) or [""])
    return wrapped[:10]


def wrap_notes_lines(lines: list[str]) -> list[str]:
    wrapped = []
    for line in lines:
        wrapped.extend(textwrap.wrap(line, width=31) or [""])
    return wrapped[:12]


def wrap_screen_lines(lines: list[str]) -> list[str]:
    wrapped = []
    for line in lines:
        if len(line) <= 74:
            wrapped.append(line)
        else:
            wrapped.extend(textwrap.wrap(line, width=74, subsequent_indent="  "))
    return wrapped[:18]


def write_text_fragment(build_dir: Path, filename: str, text: str) -> Path:
    path = build_dir / filename
    path.write_text(text.replace("%", "pct"), encoding="utf-8")
    return path


def write_video_script(path: Path, specs: list[dict[str, Any]]) -> None:
    lines = ["# Video script", ""]
    for spec in specs:
        lines.extend([f"## {spec['title']}", "", spec["purpose"], ""])
        for index, slide in enumerate(spec["slides"], start=1):
            lines.append(f"### Slide {index}: {slide.title}")
            lines.extend(f"- {line}" for line in slide.lines)
            lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def resolve_executable(executable: str) -> str | None:
    path = Path(executable)
    if path.exists():
        return str(path)
    return shutil.which(executable)


def find_font_file() -> Path | None:
    configured = os.environ.get("HYFHENET_VIDEO_FONT")
    candidates = [
        Path(configured) if configured else None,
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/segoeui.ttf"),
    ]
    for candidate in candidates:
        if candidate is not None and candidate.exists():
            return candidate
    return None


def find_mono_font_file() -> Path | None:
    configured = os.environ.get("HYFHENET_VIDEO_MONO_FONT")
    candidates = [
        Path(configured) if configured else None,
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"),
        Path("/usr/share/fonts/truetype/liberation2/LiberationMono-Regular.ttf"),
        Path("C:/Windows/Fonts/consola.ttf"),
        Path("C:/Windows/Fonts/cour.ttf"),
    ]
    for candidate in candidates:
        if candidate is not None and candidate.exists():
            return candidate
    return None


def filter_path(path: Path) -> str:
    value = path.resolve().as_posix().replace("\\", "\\\\").replace(":", "\\:")
    value = value.replace("'", "\\'")
    return f"'{value}'"


def concat_path_for(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "'\\''")


def run_ffmpeg(command: list[str]) -> None:
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as error:
        stderr = (error.stderr or "").strip()
        if len(stderr) > 1200:
            stderr = stderr[-1200:]
        raise RuntimeError(f"ffmpeg failed while rendering report video:\n{stderr}") from error


def write_model_results_csv(edge_results: Path, out: Path) -> None:
    rows = []
    with edge_results.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("edge_record_type") == "model_inference_result":
                rows.append({column: record.get(column) for column in MODEL_RESULT_COLUMNS})
    with out.open("w", **CSV_KWARGS) as handle:
        writer = csv.DictWriter(handle, fieldnames=MODEL_RESULT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def latency_summary(path: Path) -> dict[str, Any] | None:
    samples = []
    for row in read_csv(path):
        samples.append(
            LatencySample(
                tick_timestamp=datetime.fromisoformat(row["tick_timestamp"]),
                latest_event_timestamp=parse_datetime(row.get("latest_event_timestamp")),
                raw_event_count=int(row["raw_event_count"]),
                normalized_event_count=int(row["normalized_event_count"]),
                snapshot_count=int(row["snapshot_count"]),
                cloud_forecast_feature_count=int(row["cloud_forecast_feature_count"]),
                cloud_forecast_training_example_count=int(row["cloud_forecast_training_example_count"]),
                load_event_marker_count=int(row["load_event_marker_count"]),
                edge_forecast_evaluation_count=int(row["edge_forecast_evaluation_count"]),
                service_result_count=int(row["service_result_count"]),
                model_result_count=int(row["model_result_count"]),
                group_processing_ms=maybe_float(row.get("group_processing_ms")),
                snapshot_stage_ms=maybe_float(row.get("snapshot_stage_ms")),
                feature_stage_ms=maybe_float(row.get("feature_stage_ms")),
                event_gate_stage_ms=maybe_float(row.get("event_gate_stage_ms")),
                forecast_prep_stage_ms=maybe_float(row.get("forecast_prep_stage_ms")),
                fhe_cloud_sampled=maybe_int(row.get("fhe_cloud_sampled"), default=1),
                fhe_cloud_stage_ms=maybe_float(row.get("fhe_cloud_stage_ms")),
                service_stage_ms=maybe_float(row.get("service_stage_ms")),
                model_stage_ms=maybe_float(row.get("model_stage_ms")),
                total_tick_ms=maybe_float(row.get("total_tick_ms")),
                event_to_model_ms=maybe_float(row.get("event_to_model_ms")),
            )
        )
    return build_latency_summary(samples)


def latency_columns() -> list[str]:
    sample = LatencySample(
        tick_timestamp=datetime.now(),
        latest_event_timestamp=None,
        raw_event_count=0,
        normalized_event_count=0,
        snapshot_count=0,
        cloud_forecast_feature_count=0,
        cloud_forecast_training_example_count=0,
        load_event_marker_count=0,
        edge_forecast_evaluation_count=0,
        service_result_count=0,
        model_result_count=0,
        group_processing_ms=None,
        snapshot_stage_ms=None,
        feature_stage_ms=None,
        event_gate_stage_ms=None,
        forecast_prep_stage_ms=None,
        fhe_cloud_sampled=0,
        fhe_cloud_stage_ms=None,
        service_stage_ms=None,
        model_stage_ms=None,
        total_tick_ms=None,
        event_to_model_ms=None,
    )
    return list(sample.to_record())


def forecast_metrics(predictions: list[float], targets: list[float]) -> dict[str, float]:
    errors = [prediction - target for prediction, target in zip(predictions, targets)]
    mae = sum(abs(error) for error in errors) / len(errors)
    rmse = math.sqrt(sum(error * error for error in errors) / len(errors))
    target_mean = sum(targets) / len(targets)
    total_sum_squares = sum((target - target_mean) ** 2 for target in targets)
    residual_sum_squares = sum(error * error for error in errors)
    r2 = 0.0 if total_sum_squares <= 1e-12 else 1.0 - residual_sum_squares / total_sum_squares
    return {"mae_w": round(mae, 4), "rmse_w": round(rmse, 4), "r2": round(r2, 4)}


def latency_ms(rows: list[dict[str, str]], predict: Callable[[dict[str, str]], float]) -> float:
    repeats = max(500, len(rows))
    started = time.perf_counter()
    for index in range(repeats):
        predict(rows[index % len(rows)])
    return round((time.perf_counter() - started) * 1000.0 / repeats, 6)


def features(row: dict[str, str]) -> list[float]:
    return [number(row.get(column)) for column in FORECAST_NUMERIC_FEATURE_COLUMNS]


def target_values(rows: list[dict[str, str]]) -> list[float]:
    return [number(row["target_household_power_w"]) for row in rows]


def number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def maybe_int(value: Any, default: int | None = None) -> int | None:
    if value in (None, ""):
        return default
    return int(value)


def maybe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def mean(values) -> float | None:
    numeric = [float(value) for value in values if value not in (None, "")]
    return round(sum(numeric) / len(numeric), 6) if numeric else None


def alpha_label(alpha: float) -> str:
    return str(int(alpha)) if float(alpha).is_integer() else str(alpha).replace(".", "_")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(**CSV_KWARGS) as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def copy_json(source: Path, destination: Path) -> Path:
    write_json(destination, load_json(source))
    return destination


def copy_source_capture(source: Path, destination: Path) -> Path:
    destination.write_text(source.read_text(encoding="utf-8-sig"), encoding="utf-8")
    return destination


def resolve(path: Path) -> Path:
    return path if path.is_absolute() else ROOT / path


if __name__ == "__main__":
    raise SystemExit(main())
