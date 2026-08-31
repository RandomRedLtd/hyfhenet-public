from __future__ import annotations

import contextlib
import csv
import io
import json
import threading
import tempfile
import time
import unittest
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from hyfhenet.ai.event_gates import LoadEventGateDetector
from hyfhenet.ai.models import (
    EnergyAnomalyMonitorModel,
    build_model_input,
)
from hyfhenet.core.config import load_pipeline_config
from hyfhenet.core.interfaces import PipelineContext, StreamingPipelineStage
from hyfhenet.core.models import (
    CloudForecastFeatureRecord,
    FeatureWindow,
    GatewayStreamRunSummary,
    ModelInferenceResult,
    RawTelemetryEvent,
    StreamingPipelineRuntime,
)
from hyfhenet.fhe import (
    FheCohortBenchmarkTask,
    FheLongTermLoadForecastTask,
    FheNilmDisaggregationTask,
)
from hyfhenet.fhe.client import FheClientConfig, HyfhenetFheClient
from hyfhenet.fhe.features import (
    COHORT_INPUT_COLUMNS,
    FORECAST_NUMERIC_FEATURE_COLUMNS,
    NILM_INPUT_COLUMNS,
)
from hyfhenet.ingestion.stream_sources import (
    CaptureRawEventSource,
    EdfServiceSdkEventSource,
    ZigbeeMqttSensorSource,
    TimestampPacedStreamSource,
    ZigbeeCsvReplaySource,
    decode_edf_sdk_event,
    decode_mqtt_message,
    permit_join,
    set_plug_state,
)
from hyfhenet.ingestion.normalization import (
    apply_normalized_event_to_state,
    build_snapshot,
    normalize_raw_row,
)
from hyfhenet.cli import build_parser
from hyfhenet.processing.stream_stages import StreamingAiModelStage, StreamingFheCloudInferenceStage
from hyfhenet.runtime.profiling import build_latency_summary
from hyfhenet.runtime.stream import (
    GatewayStreamingPipeline,
    NullStreamingObserver,
    build_gateway_event_source,
    build_gateway_stream_context,
    build_gateway_stream_pipeline_for_context,
    run_gateway_stream_replay,
)
from hyfhenet.runtime.sinks import AppendFileStreamingSink, NullStreamingSink
from scripts.prepare_edge_report import run_summary_benchmark
from hyfhenet.training import edge_forecast as edge_forecast_training
from hyfhenet.training.edge_forecast import train_edge_short_term_load_forecast


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs/pilot_1_2_edge.json"
INPUT_PATH = ROOT / "data/zigbee_mqtt_capture.csv"


class FakeMqttClient:
    def __init__(self, messages):
        self.messages = messages
        self.on_connect = None
        self.on_message = None
        self.on_disconnect = None
        self.subscriptions = []
        self.connected = None
        self.credentials = None
        self.reconnect_delay = None
        self.tls = None
        self.tls_insecure = None

    def username_pw_set(self, username, password=None):
        self.credentials = (username, password)

    def tls_set(self, ca_certs=None, certfile=None, keyfile=None):
        self.tls = (ca_certs, certfile, keyfile)

    def tls_insecure_set(self, value):
        self.tls_insecure = value

    def reconnect_delay_set(self, min_delay, max_delay):
        self.reconnect_delay = (min_delay, max_delay)

    def connect(self, host, port, keepalive):
        self.connected = (host, port, keepalive)

    def subscribe(self, topic):
        self.subscriptions.append(topic)

    def loop_start(self):
        if self.on_connect:
            self.on_connect(self, None, None, 0, None)
        if self.on_message:
            for message in self.messages:
                self.on_message(self, None, message)

    def loop_stop(self):
        return None

    def disconnect(self):
        return None


class FakePublisherClient:
    def __init__(self):
        self.connected = None
        self.credentials = None
        self.published = []
        self.started = False
        self.stopped = False
        self.tls = None
        self.tls_insecure = None

    def username_pw_set(self, username, password=None):
        self.credentials = (username, password)

    def tls_set(self, ca_certs=None, certfile=None, keyfile=None):
        self.tls = (ca_certs, certfile, keyfile)

    def tls_insecure_set(self, value):
        self.tls_insecure = value

    def connect(self, host, port, keepalive):
        self.connected = (host, port, keepalive)

    def loop_start(self):
        self.started = True

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def loop_stop(self):
        self.stopped = True

    def disconnect(self):
        return None


class FakeEdfDeviceApi:
    def __init__(self, events):
        self.events = events
        self.requested_streams = None
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exited = True
        return None

    async def event(self, streams=None):
        self.requested_streams = streams
        for event in self.events:
            yield event


class FakeSuccessReasonCode:
    value = 0
    name = "Success"
    is_failure = False


class StaticStreamSource:
    def __init__(self, events):
        self.events = events

    def stream(self, context):
        yield from self.events


class EmptyStreamSource:
    def stream(self, context):
        return
        yield


class TrackingStreamingStage(StreamingPipelineStage):
    def __init__(self):
        self.started = False
        self.group_count = 0
        self.tick_count = 0
        self.completed = False

    def on_start(self, runtime, context, sink, observer):
        self.started = True

    def on_event_group(self, events, runtime, context, sink, observer):
        self.group_count += 1

    def on_tick(self, tick_timestamp, runtime, context, sink, observer):
        self.tick_count += 1

    def on_complete(self, runtime, context, sink, observer):
        self.completed = True


class CountingFheRunner:
    def __init__(self):
        self.calls = []

    def task_ids(self):
        return [
            "fhe_long_term_load_forecast",
            "fhe_nilm_disaggregation",
            "fhe_cohort_benchmark",
        ]

    def infer_cloud_features(self, forecast_feature, nilm_feature, cohort_feature, context):
        self.calls.append(forecast_feature.timestamp)
        return []


class BlockingFheRunner:
    def __init__(self):
        self.calls = []
        self.started = threading.Event()
        self.release = threading.Event()

    def task_ids(self):
        return ["fhe_long_term_load_forecast"]

    def infer_cloud_features(self, forecast_feature, nilm_feature, cohort_feature, context):
        self.calls.append(forecast_feature.timestamp)
        self.started.set()
        self.release.wait(timeout=2.0)
        return [
            ModelInferenceResult(
                timestamp=forecast_feature.timestamp,
                model_id="fhe_long_term_load_forecast",
                model_version="1.0",
                backend="concrete_ml_remote_fhe",
                input_contract_version="1.0",
                output_contract_version="1.0",
                inference_status="ok",
                prediction_label="fhe_long_term_load_forecast",
                prediction_score=42.0,
                anomaly_score=None,
                load_score=42.0,
                details=json.dumps(
                    {
                        "timing_ms": {
                            "cloud_fhe_inference_wait_ms": 123.0,
                        }
                    }
                ),
            )
        ]


class StreamingPipelineTests(unittest.TestCase):
    def test_replay_source_matches_sensor_capture_shape(self) -> None:
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "source_shape",
            config_path=CONFIG_PATH,
        )
        events = list(ZigbeeCsvReplaySource().stream(context))

        self.assertGreaterEqual(len(events), 400)
        self.assertGreater(sum(1 for event in events if event.device == "0xa4c1380538efffff"), 0)
        self.assertGreater(sum(1 for event in events if event.device == "0xa4c138057801ffff"), 0)

    def test_stream_replay_writes_edge_analytics_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = run_gateway_stream_replay(
                input_path=INPUT_PATH,
                output_dir=Path(tmpdir),
                config_path=CONFIG_PATH,
                interval_seconds=30,
            )

            self.assertEqual(summary["normalized_event_count"], summary["raw_event_count"])
            self.assertEqual(summary["feature_window_count"], summary["snapshot_count"])
            self.assertEqual(summary["cloud_forecast_feature_count"], summary["snapshot_count"])
            self.assertEqual(summary["cloud_forecast_training_example_count"], 0)
            self.assertGreater(summary["edge_forecast_evaluation_count"], 0)
            self.assertEqual(summary["service_result_count"], summary["snapshot_count"])
            self.assertEqual(summary["model_input_count"], summary["snapshot_count"] * 2)
            self.assertEqual(summary["model_result_count"], summary["snapshot_count"] * 5)
            self.assertGreater(summary["load_event_marker_count"], 0)
            self.assertEqual(summary["household_power_source"], "sum_of_configured_smart_plugs")

            output_dir = Path(tmpdir)
            with (output_dir / "cloud_forecast_features.csv").open(newline="", encoding="utf-8") as handle:
                forecast_features = list(csv.DictReader(handle))
            with (output_dir / "cloud_nilm_features.csv").open(newline="", encoding="utf-8") as handle:
                nilm_features = list(csv.DictReader(handle))
            with (output_dir / "cloud_cohort_features.csv").open(newline="", encoding="utf-8") as handle:
                cohort_features = list(csv.DictReader(handle))
            with (output_dir / "cloud_forecast_training_examples.csv").open(newline="", encoding="utf-8") as handle:
                forecast_training = list(csv.DictReader(handle))
            with (output_dir / "edge_results.jsonl").open(encoding="utf-8") as handle:
                edge_results = [json.loads(line) for line in handle if line.strip()]

            markers = [
                row for row in edge_results if row["edge_record_type"] == "load_event_marker"
            ]
            services = [
                row for row in edge_results if row["edge_record_type"] == "edge_service_result"
            ]
            model_results = [
                row for row in edge_results if row["edge_record_type"] == "model_inference_result"
            ]
            anomaly_results = [
                row for row in model_results if row["model_id"] == "energy_anomaly_monitor"
            ]
            forecast_results = [
                row for row in model_results if row["model_id"] == "edge_short_term_load_forecast"
            ]
            fhe_forecast_results = [
                row for row in model_results if row["model_id"] == "fhe_long_term_load_forecast"
            ]
            fhe_nilm_results = [
                row for row in model_results if row["model_id"] == "fhe_nilm_disaggregation"
            ]
            fhe_cohort_results = [
                row for row in model_results if row["model_id"] == "fhe_cohort_benchmark"
            ]
            forecast_evaluations = [
                row for row in edge_results if row["edge_record_type"] == "edge_forecast_evaluation"
            ]

            self.assertFalse((output_dir / "normalized_events.jsonl").exists())
            self.assertFalse((output_dir / "aligned_snapshots.csv").exists())
            self.assertFalse((output_dir / "feature_windows.csv").exists())
            self.assertFalse((output_dir / "latency_samples.csv").exists())
            self.assertFalse((output_dir / "latency_summary.json").exists())
            self.assertFalse((output_dir / "edge_forecast_evaluation.csv").exists())
            self.assertTrue((output_dir / "latency.csv").exists())
            self.assertTrue((output_dir / "performance_metrics.json").exists())
            self.assertTrue(all(row["detector_id"] == "load_event_gate" for row in markers))
            self.assertTrue(any(row["event_type"] in {"initial_state", "ramp_up", "ramp_down", "switch_on", "switch_off"} for row in markers))
            self.assertEqual(len(forecast_features), summary["snapshot_count"])
            self.assertEqual(len(nilm_features), summary["snapshot_count"])
            self.assertEqual(len(cohort_features), summary["snapshot_count"])
            self.assertTrue(all(row["feature_set_id"] == "long_term_load_forecast_v1" for row in forecast_features))
            self.assertTrue(all(row["feature_set_id"] == "nilm_disaggregation_v1" for row in nilm_features))
            self.assertTrue(all(row["feature_set_id"] == "cohort_benchmark_v1" for row in cohort_features))
            self.assertTrue(any(row["linky_household_power_mean_1h_w"] for row in forecast_features))
            self.assertTrue(all(row["horizon_minutes"] == "120" for row in forecast_features))
            self.assertIn("plug_1_power_w", nilm_features[0])
            self.assertNotIn("target_plug_1_power_w", nilm_features[0])
            self.assertIn("total_energy_kwh", cohort_features[0])
            self.assertEqual(len(forecast_training), summary["cloud_forecast_training_example_count"])
            self.assertEqual(forecast_training, [])
            self.assertEqual(len(forecast_evaluations), summary["edge_forecast_evaluation_count"])
            self.assertIn("target_household_power_w", forecast_evaluations[0])
            self.assertIn("predicted_household_power_w", forecast_evaluations[0])
            contract = json.loads((output_dir / "cloud_forecast_feature_contract.json").read_text(encoding="utf-8"))
            self.assertEqual(contract["fhe_backend_target"], "zama_concrete_ml")
            self.assertEqual(contract["purpose"], "long_term_load_forecasting")
            self.assertEqual(contract["feature_set_id"], "long_term_load_forecast_v1")
            self.assertEqual(contract["model_endpoint"], "forecast")
            self.assertIn("linky_household_power_w", contract["numeric_feature_columns"])
            self.assertIn("plug_1_power_w", contract["numeric_feature_columns"])
            self.assertIn("plug_2_power_w", contract["numeric_feature_columns"])
            contracts = json.loads((output_dir / "cloud_model_feature_contracts.json").read_text(encoding="utf-8"))
            self.assertEqual(set(contracts), {"forecast", "nilm", "cohort"})
            self.assertEqual(contracts["nilm"]["model_endpoint"], "nilm")
            self.assertEqual(contracts["cohort"]["model_endpoint"], "cohort")
            self.assertIn("cohort_label", contracts["cohort"]["excluded_columns"])
            self.assertTrue(any(row["profile_state"] for row in services))
            self.assertEqual(len(anomaly_results), summary["snapshot_count"])
            self.assertEqual(len(forecast_results), summary["snapshot_count"])
            self.assertEqual(len(fhe_forecast_results), summary["snapshot_count"])
            self.assertEqual(len(fhe_nilm_results), summary["snapshot_count"])
            self.assertEqual(len(fhe_cohort_results), summary["snapshot_count"])
            self.assertTrue(all(row["prediction_label"] == "short_term_load_forecast" for row in forecast_results))
            self.assertTrue(all(row["prediction_score"] for row in forecast_results))
            self.assertTrue(all(row["prediction_label"] == "fhe_long_term_forecast_unavailable" for row in fhe_forecast_results))
            self.assertTrue(all(row["prediction_label"] == "fhe_nilm_unavailable" for row in fhe_nilm_results))
            self.assertTrue(all(row["prediction_label"] == "fhe_cohort_unavailable" for row in fhe_cohort_results))

            run_summary = json.loads((output_dir / "run_summary.json").read_text(encoding="utf-8"))
            latency_summary = run_summary["latency_summary"]
            performance_metrics = json.loads(
                (output_dir / "performance_metrics.json").read_text(encoding="utf-8")
            )
            self.assertEqual(latency_summary["sample_count"], summary["snapshot_count"])
            self.assertIn("avg_event_gate_stage_ms", latency_summary)
            self.assertIn("avg_forecast_prep_stage_ms", latency_summary)
            self.assertIn("avg_edge_local_operations_ms", latency_summary)
            self.assertIn("avg_cloud_fhe_operations_ms", latency_summary)
            self.assertIn("avg_fhe_cloud_stage_ms", latency_summary)
            self.assertIn("p99_total_tick_ms", latency_summary)
            self.assertIn("raw_events_per_wall_second", run_summary["performance_summary"])
            self.assertIn("overall", run_summary["forecast_quality_summary"])
            self.assertIn("mae_w", run_summary["forecast_quality_summary"]["overall"])
            self.assertEqual(
                performance_metrics["forecast_quality_summary"],
                run_summary["forecast_quality_summary"],
            )
            self.assertEqual(
                run_summary["model_result_summary"]["by_model"]
                ["fhe_long_term_load_forecast"]["status_counts"]["unavailable"],
                summary["snapshot_count"],
            )
            self.assertEqual(
                run_summary["model_result_summary"]["by_model"]
                ["fhe_nilm_disaggregation"]["status_counts"]["unavailable"],
                summary["snapshot_count"],
            )
            self.assertEqual(
                run_summary["model_result_summary"]["by_model"]
                ["fhe_cohort_benchmark"]["status_counts"]["unavailable"],
                summary["snapshot_count"],
            )

    def test_fhe_stage_samples_remote_inference_by_configured_interval(self) -> None:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        config.setdefault("fhe_cloud", {})["sample_interval_seconds"] = 60
        base = datetime(2026, 4, 29, 13, 0, 0)
        context = PipelineContext(input_path=INPUT_PATH, output_dir=ROOT / "artifacts", config=config)
        runtime = StreamingPipelineRuntime(
            tick_interval_seconds=5,
            tick_clock_scale=0.0,
            idle_poll_seconds=0.1,
            group_flush_seconds=0.2,
        )
        runtime.profiling_enabled = True
        runner = CountingFheRunner()
        stage = StreamingFheCloudInferenceStage(runner)
        samples = []

        for offset_seconds in range(0, 121, 5):
            timestamp = base + timedelta(seconds=offset_seconds)
            runtime.start_tick(float(offset_seconds))
            runtime.latest_feature_window = FeatureWindow(
                timestamp=timestamp,
                schema_version="1.0",
                values={"household_power_w": 42.0},
            )
            runtime.latest_cloud_forecast_feature = CloudForecastFeatureRecord(
                timestamp=timestamp,
                schema_version="1.0",
                feature_set_id="long_term_load_forecast_v1",
                values={},
            )
            stage.on_tick(
                timestamp,
                runtime,
                context,
                NullStreamingSink(),
                NullStreamingObserver(),
            )
            if runtime.current_tick_fhe_cloud_sampled:
                runtime.record_tick_stage_duration(
                    "cloud_fhe_operations",
                    1000.0 + offset_seconds,
                    )
            runtime.record_tick_stage_duration("fhe_cloud_inference", 0.1)
            sample = runtime.finish_tick(timestamp, float(offset_seconds) + 0.001)
            self.assertIsNotNone(sample)
            samples.append(sample)

        self.assertEqual(
            runner.calls,
            [
                base,
                base + timedelta(seconds=60),
                base + timedelta(seconds=120),
                ],
        )
        self.assertEqual(runtime.model_input_count, 3)
        latency_summary = build_latency_summary(samples)
        self.assertEqual(latency_summary["sample_count"], 25)
        self.assertEqual(latency_summary["fhe_cloud_sample_count"], 3)
        self.assertEqual(latency_summary["avg_fhe_cloud_stage_ms"], 1060.0)
        self.assertEqual(latency_summary["avg_cloud_fhe_operations_ms"], 1060.0)
        self.assertIn("avg_edge_local_operations_ms", latency_summary)

    def test_fhe_stage_async_dispatch_does_not_block_tick_loop(self) -> None:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        config.setdefault("fhe_cloud", {})["async_enabled"] = True
        config.setdefault("fhe_cloud", {})["sample_interval_seconds"] = 60
        config.setdefault("fhe_cloud", {})["max_pending_requests"] = 1
        base = datetime(2026, 4, 29, 13, 0, 0)
        context = PipelineContext(input_path=INPUT_PATH, output_dir=ROOT / "artifacts", config=config)
        runtime = StreamingPipelineRuntime(
            tick_interval_seconds=5,
            tick_clock_scale=0.0,
            idle_poll_seconds=0.1,
            group_flush_seconds=0.2,
        )
        runner = BlockingFheRunner()
        stage = StreamingFheCloudInferenceStage(runner)

        runtime.start_tick(0.0)
        runtime.latest_feature_window = FeatureWindow(
            timestamp=base,
            schema_version="1.0",
            values={"household_power_w": 42.0},
        )
        runtime.latest_cloud_forecast_feature = CloudForecastFeatureRecord(
            timestamp=base,
            schema_version="1.0",
            feature_set_id="long_term_load_forecast_v1",
            values={},
        )
        stage.on_tick(base, runtime, context, NullStreamingSink(), NullStreamingObserver())

        self.assertTrue(runner.started.wait(timeout=1.0))
        self.assertEqual(runner.calls, [base])
        self.assertEqual(runtime.model_input_count, 1)
        self.assertFalse(runtime.current_tick_fhe_cloud_sampled)

        queued_while_pending = base + timedelta(seconds=60)
        runtime.start_tick(0.001)
        runtime.latest_feature_window = FeatureWindow(
            timestamp=queued_while_pending,
            schema_version="1.0",
            values={"household_power_w": 43.0},
        )
        runtime.latest_cloud_forecast_feature = CloudForecastFeatureRecord(
            timestamp=queued_while_pending,
            schema_version="1.0",
            feature_set_id="long_term_load_forecast_v1",
            values={},
        )
        stage.on_tick(
            queued_while_pending,
            runtime,
            context,
            NullStreamingSink(),
            NullStreamingObserver(),
        )

        self.assertEqual(runtime.model_input_count, 1)
        self.assertEqual(runner.calls, [base])

        runner.release.set()
        completed = False
        for index in range(20):
            drain_timestamp = base + timedelta(seconds=65 + index)
            runtime.start_tick(float(index))
            runtime.latest_feature_window = FeatureWindow(
                timestamp=drain_timestamp,
                schema_version="1.0",
                values={"household_power_w": 44.0},
            )
            runtime.latest_cloud_forecast_feature = CloudForecastFeatureRecord(
                timestamp=drain_timestamp,
                schema_version="1.0",
                feature_set_id="long_term_load_forecast_v1",
                values={},
            )
            stage.on_tick(
                drain_timestamp,
                runtime,
                context,
                NullStreamingSink(),
                NullStreamingObserver(),
            )
            if runtime.model_result_count == 1:
                completed = True
                break
            time.sleep(0.01)

        stage.on_complete(runtime, context, NullStreamingSink(), NullStreamingObserver())
        self.assertTrue(completed)
        self.assertTrue(runtime.current_tick_fhe_cloud_sampled)
        self.assertEqual(runtime.current_tick_stage_durations_ms["cloud_fhe_operations"], 123.0)
        self.assertEqual(runtime.model_result_count, 1)

    def test_stream_replay_uses_derived_household_power(self) -> None:
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "gateway_stream_derived_household",
            config_path=CONFIG_PATH,
            interval_seconds=60,
            zigbee_source="replay",
        )
        summary = build_gateway_stream_pipeline_for_context(context, write_artifacts=False).run(context)

        self.assertGreaterEqual(summary.raw_event_count, 400)
        self.assertEqual(summary.feature_window_count, summary.snapshot_count)
        self.assertEqual(summary.cloud_forecast_feature_count, summary.snapshot_count)
        self.assertGreater(summary.load_event_marker_count, 0)
        self.assertEqual(summary.household_power_source, "sum_of_configured_smart_plugs")

    def test_forecast_prep_emits_delayed_training_examples_for_observed_horizon(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            context = build_gateway_stream_context(
                input_path=INPUT_PATH,
                output_dir=Path(tmpdir),
                config_path=CONFIG_PATH,
                interval_seconds=30,
                zigbee_source="replay",
            )
            context.config["cloud_forecast"]["horizons_minutes"] = [1]
            summary = build_gateway_stream_pipeline_for_context(context, write_artifacts=True).run(context)

            self.assertEqual(summary.cloud_forecast_feature_count, summary.snapshot_count)
            self.assertGreater(summary.cloud_forecast_training_example_count, 0)

            with (Path(tmpdir) / "cloud_forecast_training_examples.csv").open(
                    newline="",
                    encoding="utf-8",
            ) as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), summary.cloud_forecast_training_example_count)
            self.assertTrue(all(row["horizon_minutes"] == "1" for row in rows))
            self.assertTrue(all(row["target_household_power_w"] for row in rows))

    def test_stream_pipeline_uses_configured_stage_order(self) -> None:
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "stage_order",
            config_path=CONFIG_PATH,
            zigbee_source="replay",
        )
        pipeline = build_gateway_stream_pipeline_for_context(context, write_artifacts=False)

        self.assertEqual(
            [stage.__class__.__name__ for stage in pipeline.stages],
            [
                "StreamingPreprocessingStage",
                "StreamingFeatureStage",
                "StreamingEventGateStage",
                "StreamingCloudForecastPrepStage",
                "StreamingFheCloudInferenceStage",
                "StreamingServiceStage",
                "StreamingAiModelStage",
            ],
        )

    def test_gateway_event_source_builds_registered_replay_mode(self) -> None:
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "paced",
            config_path=CONFIG_PATH,
            zigbee_source="replay",
            follow_event_timing=True,
        )

        self.assertIsInstance(
            build_gateway_event_source(context, observer=NullStreamingObserver()),
            TimestampPacedStreamSource,
        )

    def test_mqtt_live_defaults_to_zigbee_only(self) -> None:
        args = build_parser().parse_args(["mqtt-live"])
        self.assertFalse(args.continuous)

        context = build_gateway_stream_context(
            input_path="zigbee-mqtt://192.168.1.65",
            output_dir=ROOT / "artifacts" / "mqtt_live_default",
            config_path=CONFIG_PATH,
            zigbee_source="zigbee_mqtt",
        )
        self.assertIsInstance(build_gateway_event_source(context), ZigbeeMqttSensorSource)

    def test_stream_replay_defaults_to_checked_in_capture(self) -> None:
        args = build_parser().parse_args(["stream-replay"])

        self.assertEqual(args.input, Path("data/zigbee_mqtt_capture.csv"))

    def test_mqtt_live_supports_continuous_mode(self) -> None:
        args = build_parser().parse_args(["mqtt-live", "--continuous"])
        self.assertTrue(args.continuous)

    def test_mqtt_live_supports_mqtts_certificate_args(self) -> None:
        args = build_parser().parse_args(
            [
                "mqtt-live",
                "--mqtt-tls",
                "--mqtt-ca-cert",
                "certs/ca.pem",
                "--mqtt-client-cert",
                "certs/client.pem",
                "--mqtt-client-key",
                "certs/client.key",
            ]
        )

        self.assertTrue(args.mqtt_tls)
        self.assertEqual(args.mqtt_ca_cert, Path("certs/ca.pem"))
        self.assertEqual(args.mqtt_client_cert, Path("certs/client.pem"))
        self.assertEqual(args.mqtt_client_key, Path("certs/client.key"))

    def test_mqtt_capture_parser_defaults_to_capture_only(self) -> None:
        args = build_parser().parse_args(["mqtt-capture"])
        self.assertEqual(args.capture_output, Path("artifacts/zigbee_mqtt_capture.csv"))
        self.assertFalse(args.run_pipeline)
        self.assertFalse(args.append)

    def test_edf_sdk_live_parser_defaults_to_finite_window(self) -> None:
        args = build_parser().parse_args(["edf-sdk-live"])

        self.assertEqual(args.output, Path("artifacts/gateway_edf_sdk_live"))
        self.assertFalse(args.continuous)
        self.assertIsNone(args.edf_streams)

    def test_edf_sdk_live_parser_accepts_stream_mapping(self) -> None:
        args = build_parser().parse_args(
            [
                "edf-sdk-live",
                "--edf-streams",
                "TEMPERATURE,POWER",
                "--edf-stream-field-map",
                "POWER=household_power_w",
                "--edf-default-device-role",
                "household_meter",
                "--continuous",
            ]
        )

        self.assertEqual(args.edf_streams, "TEMPERATURE,POWER")
        self.assertEqual(args.edf_stream_field_map, "POWER=household_power_w")
        self.assertEqual(args.edf_default_device_role, "household_meter")
        self.assertTrue(args.continuous)

    def test_fhe_cloud_forecast_cli_args_configure_default_stage(self) -> None:
        args = build_parser().parse_args(
            [
                "stream-replay",
                "--fhe-api-url",
                "https://fhe.example",
                "--fhe-api-key",
                "secret",
                "--fhe-cache-dir",
                "artifacts/fhe-cache",
                "--fhe-client-cert",
                "certs/fhe-client.pem",
                "--fhe-client-key",
                "certs/fhe-client.key",
                "--fhe-ca-bundle",
                "certs/fhe-ca.pem",
                "--fhe-allow-insecure-http",
            ]
        )

        self.assertEqual(args.fhe_api_url, "https://fhe.example")
        self.assertEqual(args.fhe_api_key, "secret")
        self.assertEqual(args.fhe_cache_dir, Path("artifacts/fhe-cache"))
        self.assertEqual(args.fhe_client_cert, Path("certs/fhe-client.pem"))
        self.assertEqual(args.fhe_client_key, Path("certs/fhe-client.key"))
        self.assertEqual(args.fhe_ca_bundle, Path("certs/fhe-ca.pem"))
        self.assertTrue(args.fhe_allow_insecure_http)

    def test_fhe_cloud_cli_args_support_sampling_async_and_task_subset(self) -> None:
        args = build_parser().parse_args(
            [
                "mqtt-live",
                "--fhe-sample-interval-seconds",
                "60",
                "--fhe-tasks",
                "forecast,nilm",
                "--fhe-async",
                "--fhe-max-pending-requests",
                "2",
            ]
        )

        self.assertEqual(args.fhe_sample_interval_seconds, 60)
        self.assertEqual(args.fhe_tasks, "forecast,nilm")
        self.assertTrue(args.fhe_async_enabled)
        self.assertEqual(args.fhe_max_pending_requests, 2)

    def test_fhe_env_overrides_support_sampling_async_and_task_subset(self) -> None:
        with mock.patch.dict(
                "os.environ",
                {
                    "HYFHENET_FHE_SAMPLE_INTERVAL_SECONDS": "60",
                    "HYFHENET_FHE_ASYNC": "true",
                    "HYFHENET_FHE_MAX_PENDING_REQUESTS": "2",
                    "HYFHENET_FHE_TASKS": "forecast,nilm",
                },
        ):
            config = load_pipeline_config(CONFIG_PATH)

        fhe_cloud = config["fhe_cloud"]
        self.assertEqual(fhe_cloud["sample_interval_seconds"], 60)
        self.assertTrue(fhe_cloud["async_enabled"])
        self.assertEqual(fhe_cloud["max_pending_requests"], 2)
        self.assertTrue(fhe_cloud["tasks"]["forecast"]["enabled"])
        self.assertTrue(fhe_cloud["tasks"]["nilm"]["enabled"])
        self.assertFalse(fhe_cloud["tasks"]["cohort"]["enabled"])

    def test_edf_sdk_env_overrides_configure_streams_and_mapping(self) -> None:
        with mock.patch.dict(
                "os.environ",
                {
                    "HYFHENET_EDF_SDK_STREAMS": "TEMPERATURE,POWER",
                    "HYFHENET_EDF_SDK_LISTEN_SECONDS": "120",
                    "HYFHENET_EDF_SDK_DEFAULT_DEVICE_ROLE": "household_meter",
                    "HYFHENET_EDF_SDK_STREAM_FIELD_MAP": "POWER=household_power_w",
                },
        ):
            config = load_pipeline_config(CONFIG_PATH)

        edf_config = config["edf_service_sdk"]
        self.assertEqual(edf_config["streams"], ["TEMPERATURE", "POWER"])
        self.assertEqual(edf_config["listen_seconds"], 120)
        self.assertEqual(edf_config["default_device_role"], "household_meter")
        self.assertEqual(edf_config["stream_field_map"], {"POWER": "household_power_w"})

    def test_fhe_env_aliases_configure_api_url_and_key(self) -> None:
        with mock.patch.dict(
                "os.environ",
                {
                    "FHE_URL": "https://hyfhe.net",
                    "FHE_API": "token",
                    "HYFHENET_FHE_API_URL": "",
                    "HYFHENET_FHE_API_KEY": "",
                    "API_URL": "",
                    "API_KEY": "",
                },
                clear=False,
        ):
            config = load_pipeline_config(CONFIG_PATH)
            client = HyfhenetFheClient(
                FheClientConfig(
                    api_url=None,
                    api_key=None,
                    cache_dir=ROOT / "artifacts" / "alias_fhe_cache",
                )
            )

        self.assertEqual(config["fhe_cloud"]["api_url"], "https://hyfhe.net")
        self.assertEqual(config["fhe_cloud"]["api_key"], "token")
        self.assertEqual(client.api_url, "https://hyfhe.net")
        self.assertEqual(client.api_key, "token")

    def test_benchmark_pipeline_parser_defaults_to_device_evidence_run(self) -> None:
        args = build_parser().parse_args(["benchmark-pipeline"])

        self.assertEqual(args.input, Path("data/zigbee_mqtt_capture.csv"))
        self.assertEqual(args.output, Path("artifacts/edge_benchmark"))
        self.assertEqual(args.runs, 4)
        self.assertEqual(args.interval_seconds, 30)

    def test_prepare_edge_report_parser_defaults_to_video_report(self) -> None:
        with mock.patch.dict(
                "os.environ",
                {
                    "HYFHENET_REPORT_OUTPUT": "",
                    "HYFHENET_REPORT_SOURCE": "",
                    "HYFHENET_REPORT_INTERVAL_SECONDS": "",
                    "HYFHENET_REPORT_BENCHMARK_RUNS": "",
                    "HYFHENET_REPORT_FHE_SAMPLE_INTERVAL_SECONDS": "",
                    "HYFHENET_REPORT_SKIP_TRAINING": "",
                    "HYFHENET_REPORT_EDGE_MODEL_PATH": "",
                    "HYFHENET_REPORT_VIDEOS": "",
                    "HYFHENET_FFMPEG_BIN": "",
                },
        ):
            args = build_parser().parse_args(["prepare-edge-report"])

        self.assertEqual(args.output, Path("artifacts/edge_report"))
        self.assertEqual(args.source, "input")
        self.assertEqual(args.interval_seconds, 5)
        self.assertEqual(args.benchmark_runs, 4)
        self.assertIsNone(args.fhe_sample_interval_seconds)
        self.assertIsNone(args.reuse_fhe_report)
        self.assertFalse(args.skip_training)
        self.assertIsNone(args.edge_model_path)
        self.assertTrue(args.videos)
        self.assertEqual(args.ffmpeg_bin, "ffmpeg")

    def test_prepare_edge_report_parser_supports_skip_training(self) -> None:
        args = build_parser().parse_args(
            [
                "prepare-edge-report",
                "--skip-training",
                "--edge-model-path",
                "models/custom_edge_model.json",
            ]
        )

        self.assertTrue(args.skip_training)
        self.assertEqual(args.edge_model_path, Path("models/custom_edge_model.json"))

    def test_prepare_edge_report_parser_supports_fhe_sample_interval(self) -> None:
        args = build_parser().parse_args(
            [
                "prepare-edge-report",
                "--interval-seconds",
                "5",
                "--fhe-sample-interval-seconds",
                "60",
                "--fhe-tasks",
                "forecast",
            ]
        )

        self.assertEqual(args.interval_seconds, 5)
        self.assertEqual(args.fhe_sample_interval_seconds, 60)
        self.assertEqual(args.fhe_tasks, "forecast")

    def test_prepare_edge_report_benchmark_runs_can_be_disabled_by_env(self) -> None:
        with mock.patch.dict("os.environ", {"HYFHENET_REPORT_BENCHMARK_RUNS": "0"}):
            args = build_parser().parse_args(["prepare-edge-report"])

        self.assertEqual(args.benchmark_runs, 0)

    def test_report_summary_benchmark_can_be_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = run_summary_benchmark(
                capture_path=Path("unused.csv"),
                config_path=CONFIG_PATH,
                out=Path(tmpdir),
                runs=0,
                interval_seconds=5,
            )

            self.assertEqual(summary["runs"], [])
            self.assertEqual(summary["totals"]["runs"], 0)
            self.assertEqual(summary["totals"]["raw_events"], 0)
            self.assertTrue((Path(tmpdir) / "summary.csv").exists())
            self.assertTrue((Path(tmpdir) / "summary.json").exists())

    def test_prepare_edge_report_parser_supports_reused_fhe_report(self) -> None:
        args = build_parser().parse_args(
            [
                "prepare-edge-report",
                "--reuse-fhe-report",
                "artifacts/edge_report_miniPC",
            ]
        )

        self.assertEqual(args.reuse_fhe_report, Path("artifacts/edge_report_miniPC"))

    def test_prepare_edge_report_skip_training_env_can_be_disabled_by_flag(self) -> None:
        with mock.patch.dict(
                "os.environ",
                {
                    "HYFHENET_REPORT_SKIP_TRAINING": "true",
                    "HYFHENET_REPORT_EDGE_MODEL_PATH": "models/env_edge_model.json",
                },
        ):
            args = build_parser().parse_args(["prepare-edge-report", "--train-report-model"])

        self.assertFalse(args.skip_training)
        self.assertEqual(args.edge_model_path, Path("models/env_edge_model.json"))

    def test_prepare_edge_report_video_env_can_disable_video_report(self) -> None:
        with mock.patch.dict("os.environ", {"HYFHENET_REPORT_VIDEOS": "false"}):
            args = build_parser().parse_args(["prepare-edge-report"])

        self.assertFalse(args.videos)

    def test_prepare_edge_report_video_flag_overrides_env(self) -> None:
        with mock.patch.dict("os.environ", {"HYFHENET_REPORT_VIDEOS": "false"}):
            args = build_parser().parse_args(["prepare-edge-report", "--videos"])

        self.assertTrue(args.videos)

    def test_capture_raw_event_source_writes_replay_compatible_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            capture_path = Path(tmpdir) / "capture.csv"
            events = [
                RawTelemetryEvent(
                    timestamp=datetime(2026, 3, 4, 11, 38, 37),
                    device="0xd44867fffe0ace1f",
                    field="power",
                    value="61.71",
                    source="zigbee",
                ),
                RawTelemetryEvent(
                    timestamp=datetime(2026, 3, 4, 11, 38, 37),
                    device="0xd44867fffe0ace1f",
                    field="state",
                    value="ON",
                    source="zigbee",
                ),
            ]
            context = build_gateway_stream_context(
                input_path="zigbee-mqtt://192.168.1.65",
                output_dir=Path(tmpdir) / "artifacts",
                config_path=CONFIG_PATH,
            )
            captured_source = CaptureRawEventSource(
                StaticStreamSource(events),
                capture_path=capture_path,
            )

            self.assertEqual(list(captured_source.stream(context)), events)

            with capture_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(
                set(rows[0]),
                {"timestamp", "device", "field", "value", "source"},
            )
            replayed = list(ZigbeeCsvReplaySource().stream(
                build_gateway_stream_context(
                    input_path=capture_path,
                    output_dir=Path(tmpdir) / "replay",
                    config_path=CONFIG_PATH,
                )
            ))
            self.assertEqual(replayed, events)

    def test_empty_capture_does_not_truncate_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            capture_path = Path(tmpdir) / "capture.csv"
            original = "timestamp,device,field,value,source\n2026-03-04 11:38:37,plug_1,power,61.71,zigbee_mqtt\n"
            capture_path.write_text(original, encoding="utf-8")
            context = build_gateway_stream_context(
                input_path="zigbee-mqtt://localhost",
                output_dir=Path(tmpdir) / "artifacts",
                config_path=CONFIG_PATH,
            )
            captured_source = CaptureRawEventSource(
                EmptyStreamSource(),
                capture_path=capture_path,
            )

            self.assertEqual(list(captured_source.stream(context)), [])
            self.assertEqual(capture_path.read_text(encoding="utf-8"), original)

    def test_progress_logging_reports_all_edge_stages(self) -> None:
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "logging",
            config_path=CONFIG_PATH,
            interval_seconds=60,
            zigbee_source="replay",
            console_log_level="progress",
            follow_event_timing=False,
        )
        output_buffer = io.StringIO()

        with contextlib.redirect_stdout(output_buffer):
            summary = build_gateway_stream_pipeline_for_context(context, write_artifacts=False).run(context)

        output = output_buffer.getvalue()
        self.assertGreaterEqual(summary.normalized_event_count, 400)
        self.assertIn("[step] stage=preprocessing", output)
        self.assertIn("[step] stage=feature_engineering", output)
        self.assertIn("[step] stage=event_gating", output)
        self.assertIn("[step] stage=cloud_forecast_prep", output)
        self.assertIn("[step] stage=fhe_cloud_inference", output)
        self.assertIn("[step] stage=service_inference", output)
        self.assertIn("[step] stage=ai_modeling", output)
        self.assertIn("[timing] ts=", output)
        self.assertIn("edge_local_ms=", output)
        self.assertIn("cloud_fhe_ms=", output)
        self.assertIn("id=fhe_long_term_load_forecast", output)
        self.assertIn("[event] ts=", output)

    def test_decode_edf_sdk_event_supports_stream_value_shape(self) -> None:
        rows = decode_edf_sdk_event(
            SimpleNamespace(
                timestamp="2026-03-04T11:38:37",
                device_id="edf_device_1",
                stream="TEMPERATURE",
                payload=21.5,
            ),
            received_at=datetime(2026, 3, 4, 11, 38, 40),
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["timestamp"], "2026-03-04 11:38:37")
        self.assertEqual(rows[0]["device"], "edf_device_1")
        self.assertEqual(rows[0]["field"], "temperature")
        self.assertEqual(rows[0]["value"], "21.5")
        self.assertEqual(rows[0]["edf_stream"], "TEMPERATURE")

    def test_decode_edf_sdk_event_supports_payload_value_shape(self) -> None:
        rows = decode_edf_sdk_event(
            SimpleNamespace(
                date=datetime(2026, 3, 4, 11, 38, 37),
                device=SimpleNamespace(id="edf_meter"),
                stream="APPARENT_POWER",
                payload={"value": 612.4},
            ),
            received_at=datetime(2026, 3, 4, 11, 38, 40),
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["field"], "PAPP")
        self.assertEqual(rows[0]["value"], "612.4")

    def test_decode_edf_sdk_event_supports_meter_indexes_payload(self) -> None:
        rows = decode_edf_sdk_event(
            {
                "date": datetime(2026, 3, 4, 11, 38, 37),
                "device": {"id": "edf_meter"},
                "stream": "METER_INDEXES",
                "payload": {"indexes": [12345]},
            },
            received_at=datetime(2026, 3, 4, 11, 38, 40),
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["field"], "BASE")
        self.assertEqual(rows[0]["value"], "12345")

    def test_decode_edf_sdk_event_supports_nested_payload_shape(self) -> None:
        rows = decode_edf_sdk_event(
            {
                "created_at": "2026-03-04T11:38:37",
                "device": {"id": "edf_meter"},
                "payload": {
                    "POWER": 612.4,
                    "TEMPERATURE": 20.1,
                    "IGNORED": 1,
                },
            },
            received_at=datetime(2026, 3, 4, 11, 38, 40),
            stream_field_map={"POWER": "household_power_w"},
        )

        self.assertEqual({row["field"] for row in rows}, {"household_power_w", "temperature"})
        self.assertEqual({row["device"] for row in rows}, {"edf_meter"})

    def test_decode_mqtt_message_supports_zigbee2mqtt_payloads(self) -> None:
        rows = decode_mqtt_message(
            topic="zigbee2mqtt/S60ZBTPF",
            payload=b'{"timestamp":"2026-03-04T11:38:37","state":"ON","power":61.71,"linkquality":144,"network_indicator":true}',
            received_at=datetime(2026, 3, 4, 11, 38, 40),
            topic_prefix="zigbee2mqtt/",
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["timestamp"], "2026-03-04 11:38:37")
        self.assertEqual(rows[0]["device"], "S60ZBTPF")
        self.assertEqual({row["field"] for row in rows}, {"state", "power", "linkquality"})
        self.assertNotIn("network_indicator", {row["field"] for row in rows})

    def test_decode_mqtt_message_supports_exact_zigbee2mqtt_device_topic(self) -> None:
        rows = decode_mqtt_message(
            topic="zigbee2mqtt/0xd44867fffe0ace1f",
            payload=b'{"state":"ON","power":61.71}',
            received_at=datetime(2026, 3, 4, 11, 38, 40),
            topic_prefix="zigbee2mqtt/",
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["device"], "0xd44867fffe0ace1f")
        self.assertEqual({row["field"] for row in rows}, {"state", "power"})
        self.assertEqual(rows[0]["device_name"], "0xd44867fffe0ace1f")
        self.assertIn("raw_payload", rows[0])
        self.assertIn("mqtt_topic", rows[0])

    def test_decode_mqtt_message_ignores_bridge_topics_and_malformed_payloads(self) -> None:
        bridge_logs = []
        bridge_rows = decode_mqtt_message(
            topic="zigbee2mqtt/bridge/state",
            payload=b'{"state":"online"}',
            received_at=datetime(2026, 3, 4, 11, 38, 40),
            topic_prefix="zigbee2mqtt/",
            allow_bridge_debug=True,
            debug_log_fn=bridge_logs.append,
        )
        malformed_rows = decode_mqtt_message(
            topic="zigbee2mqtt/plug_1",
            payload=b"{not-json",
            received_at=datetime(2026, 3, 4, 11, 38, 40),
            topic_prefix="zigbee2mqtt/",
        )

        self.assertEqual(bridge_rows, [])
        self.assertEqual(malformed_rows, [])
        self.assertEqual(len(bridge_logs), 1)
        self.assertIn("ignoring bridge topic", bridge_logs[0])

    def test_mqtt_stream_source_replays_fake_messages(self) -> None:
        messages = [
            SimpleNamespace(
                topic="zigbee2mqtt/S60ZBTPF",
                payload=b'{"timestamp":"2026-03-04T11:38:37","state":"ON","power":61.71}',
            )
        ]
        fake_client = FakeMqttClient(messages)
        source = ZigbeeMqttSensorSource(
            client_factory=lambda cfg: fake_client,
            now_provider=lambda: datetime(2026, 3, 4, 11, 38, 40),
            sleep_fn=lambda seconds: None,
        )
        with mock.patch.dict(
                "os.environ",
                {
                    "HYFHENET_ZIGBEE_HOST": "",
                    "HYFHENET_ZIGBEE_TOPIC": "",
                    "HYFHENET_ZIGBEE_TOPIC_PREFIX": "",
                },
        ):
            context = build_gateway_stream_context(
                input_path="zigbee-mqtt://localhost",
                output_dir=ROOT / "artifacts" / "mqtt_source",
                config_path=CONFIG_PATH,
                zigbee_source="zigbee_mqtt",
            )
        context.config["zigbee_gateway"]["listen_seconds"] = 0

        events = list(source.stream(context))
        self.assertEqual(fake_client.connected, ("localhost", 1883, 60))
        self.assertEqual(fake_client.reconnect_delay, (1, 30))
        self.assertEqual(fake_client.subscriptions, ["zigbee2mqtt/#"])
        self.assertEqual(len(events), 2)
        self.assertEqual({event.field for event in events}, {"state", "power"})
        self.assertEqual(events[0].source, "zigbee_mqtt")
        self.assertIn("raw_payload", events[0].metadata)

    def test_mqtt_stream_source_accepts_paho_reason_code_objects(self) -> None:
        messages = [
            SimpleNamespace(
                topic="zigbee2mqtt/0xa4c138057801ffff",
                payload=b'{"state":"ON","power":57.34,"network_indicator":true}',
            )
        ]

        class ReasonCodeMqttClient(FakeMqttClient):
            def loop_start(self):
                if self.on_connect:
                    self.on_connect(self, None, None, FakeSuccessReasonCode(), None)
                if self.on_message:
                    for message in self.messages:
                        self.on_message(self, None, message)

        fake_client = ReasonCodeMqttClient(messages)
        source = ZigbeeMqttSensorSource(
            client_factory=lambda cfg: fake_client,
            now_provider=lambda: datetime(2026, 4, 29, 11, 48, 53),
            sleep_fn=lambda seconds: None,
        )
        context = build_gateway_stream_context(
            input_path="zigbee-mqtt://localhost",
            output_dir=ROOT / "artifacts" / "mqtt_reason_code",
            config_path=CONFIG_PATH,
            zigbee_source="zigbee_mqtt",
        )
        context.config["zigbee_gateway"]["listen_seconds"] = 0
        context.config["zigbee_gateway"]["tls_enabled"] = True
        context.config["zigbee_gateway"]["ca_cert_path"] = "certs/ca.pem"
        context.config["zigbee_gateway"]["client_cert_path"] = "certs/client.pem"
        context.config["zigbee_gateway"]["client_key_path"] = "certs/client.key"

        events = list(source.stream(context))

        self.assertEqual({event.field for event in events}, {"state", "power"})
        self.assertEqual(
            fake_client.tls,
            ("certs/ca.pem", "certs/client.pem", "certs/client.key"),
        )

    def test_edf_sdk_stream_source_replays_fake_device_events(self) -> None:
        fake_api = FakeEdfDeviceApi(
            [
                SimpleNamespace(
                    timestamp="2026-03-04T11:38:37",
                    device_id="edf_plug_1",
                    stream="TEMPERATURE",
                    payload=21.5,
                ),
                {
                    "timestamp": "2026-03-04T11:38:38",
                    "device": {"id": "edf_plug_1"},
                    "payload": {"POWER": 61.71},
                },
            ]
        )
        source = EdfServiceSdkEventSource(
            api_context_factory=lambda: fake_api,
            now_provider=lambda: datetime(2026, 3, 4, 11, 38, 40),
        )
        context = build_gateway_stream_context(
            input_path="edf-service-sdk://DeviceApi.from_env",
            output_dir=ROOT / "artifacts" / "edf_sdk_source",
            config_path=CONFIG_PATH,
            zigbee_source="edf_service_sdk",
        )

        events = list(source.stream(context))

        self.assertEqual(
            fake_api.requested_streams,
            ["TEMPERATURE", "POWER", "APPARENT_POWER", "METER_INDEXES"],
        )
        self.assertTrue(fake_api.entered)
        self.assertTrue(fake_api.exited)
        self.assertEqual(len(events), 2)
        self.assertEqual({event.field for event in events}, {"temperature", "power"})
        self.assertEqual({event.source for event in events}, {"edf_service_sdk"})
        self.assertEqual(context.config["devices"]["edf_plug_1"]["role"], "smart_plug")
        self.assertEqual(events[0].metadata["edf_stream"], "TEMPERATURE")

    def test_household_meter_events_take_precedence_over_plug_sum(self) -> None:
        timestamp = datetime(2026, 3, 4, 12, 0, 0)
        devices = {
            "linky_meter": {"role": "household_meter", "name": "linky_household_meter"},
            "plug_1": {"role": "smart_plug", "name": "plug_1"},
            "plug_2": {"role": "smart_plug", "name": "plug_2"},
        }
        state = {}
        last_update = {}

        for raw_event in [
            RawTelemetryEvent(timestamp, "linky_meter", "SINSTS", "800", "linky_tic"),
            RawTelemetryEvent(timestamp, "plug_1", "power", "100", "zigbee_mqtt"),
            RawTelemetryEvent(timestamp, "plug_2", "power", "200", "zigbee_mqtt"),
        ]:
            normalized = normalize_raw_row(raw_event, devices)
            apply_normalized_event_to_state(normalized, state, last_update)

        snapshot = build_snapshot(timestamp, 30, state, last_update, devices)

        self.assertEqual(snapshot["plug_power_w"], 300.0)
        self.assertEqual(snapshot["household_power_w"], 800.0)
        self.assertEqual(snapshot["household_power_source"], "household_meter")

    def test_household_meter_energy_does_not_disable_plug_power_fallback(self) -> None:
        timestamp = datetime(2026, 3, 4, 12, 0, 0)
        devices = {
            "linky_meter": {"role": "household_meter", "name": "linky_household_meter"},
            "plug_1": {"role": "smart_plug", "name": "plug_1"},
            "plug_2": {"role": "smart_plug", "name": "plug_2"},
        }
        state = {}
        last_update = {}

        for raw_event in [
            RawTelemetryEvent(timestamp, "linky_meter", "EAST", "12000", "linky_tic"),
            RawTelemetryEvent(timestamp, "plug_1", "power", "100", "zigbee_mqtt"),
            RawTelemetryEvent(timestamp, "plug_2", "power", "200", "zigbee_mqtt"),
        ]:
            normalized = normalize_raw_row(raw_event, devices)
            apply_normalized_event_to_state(normalized, state, last_update)

        snapshot = build_snapshot(timestamp, 30, state, last_update, devices)

        self.assertEqual(snapshot["household_energy_wh"], 12000.0)
        self.assertEqual(snapshot["household_power_w"], 300.0)
        self.assertEqual(snapshot["household_power_source"], "sum_of_configured_smart_plugs")

    def test_edf_virtual_datalogger_can_mix_temperature_and_meter_fields(self) -> None:
        timestamp = datetime(2026, 3, 4, 12, 0, 0)
        devices = {
            "edf_virtual_datalogger": {
                "role": "smart_plug",
                "name": "edf_virtual_datalogger",
            }
        }
        state = {}
        last_update = {}

        for raw_event in [
            RawTelemetryEvent(timestamp, "edf_virtual_datalogger", "temperature", "21.5", "edf_service_sdk"),
            RawTelemetryEvent(timestamp, "edf_virtual_datalogger", "PAPP", "612.4", "edf_service_sdk"),
            RawTelemetryEvent(timestamp, "edf_virtual_datalogger", "BASE", "12345", "edf_service_sdk"),
        ]:
            normalized = normalize_raw_row(raw_event, devices)
            apply_normalized_event_to_state(normalized, state, last_update)

        snapshot = build_snapshot(timestamp, 30, state, last_update, devices)

        self.assertEqual(snapshot["indoor_temperature_c"], 21.5)
        self.assertEqual(snapshot["household_power_w"], 612.4)
        self.assertEqual(snapshot["household_energy_wh"], 12345.0)
        self.assertEqual(snapshot["household_power_source"], "household_meter")

    def test_input_source_alias_maps_to_live_zigbee_source(self) -> None:
        context = build_gateway_stream_context(
            input_path="zigbee-mqtt://localhost",
            output_dir=ROOT / "artifacts" / "input_source_alias",
            config_path=CONFIG_PATH,
            input_source="zigbee_mqtt",
        )

        self.assertEqual(context.config["gateway_stream"]["zigbee_source"], "zigbee_mqtt")
        self.assertIsInstance(build_gateway_event_source(context), ZigbeeMqttSensorSource)

    def test_input_source_alias_maps_to_edf_sdk_source(self) -> None:
        context = build_gateway_stream_context(
            input_path="edf-service-sdk://DeviceApi.from_env",
            output_dir=ROOT / "artifacts" / "edf_input_source_alias",
            config_path=CONFIG_PATH,
            input_source="edf_service_sdk",
        )

        self.assertEqual(context.config["gateway_stream"]["zigbee_source"], "edf_service_sdk")
        self.assertIsInstance(build_gateway_event_source(context), EdfServiceSdkEventSource)

    def test_zigbee_control_helpers_publish_expected_topics(self) -> None:
        publisher = FakePublisherClient()
        client_factory = lambda cfg: publisher
        config = {"host": "localhost", "port": 1883, "topic_prefix": "zigbee2mqtt/"}

        set_plug_state("plug_1", "ON", config=config, client_factory=client_factory)
        permit_join(True, 120, config=config, client_factory=client_factory)

        self.assertEqual(publisher.connected, ("localhost", 1883, 60))
        self.assertEqual(
            publisher.published[0],
            ("zigbee2mqtt/plug_1/set", '{"state": "ON"}'),
        )
        self.assertEqual(
            publisher.published[1],
            ("zigbee2mqtt/bridge/request/permit_join", '{"value": true, "time": 120}'),
        )

    def test_stage_injection_works_in_streaming_pipeline(self) -> None:
        event = RawTelemetryEvent(
            timestamp=datetime(2026, 3, 4, 11, 38, 37),
            device="S60ZBTPF",
            field="power",
            value="61.71",
            source="zigbee",
        )
        stage = TrackingStreamingStage()
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "stage_injection",
            config_path=CONFIG_PATH,
            interval_seconds=30,
            zigbee_source="replay",
        )
        pipeline = GatewayStreamingPipeline(
            source=StaticStreamSource([event]),
            sink=NullStreamingSink(),
            stages=[stage],
            observer=NullStreamingObserver(),
        )
        summary = pipeline.run(context)

        self.assertTrue(stage.started)
        self.assertEqual(stage.group_count, 1)
        self.assertGreaterEqual(stage.tick_count, 1)
        self.assertTrue(stage.completed)
        self.assertEqual(summary.raw_event_count, 1)

    def test_streaming_sink_flushes_on_record_threshold(self) -> None:
        current_time = [0.0]
        sink = AppendFileStreamingSink(
            flush_every_records=3,
            flush_interval_seconds=999.0,
            monotonic_fn=lambda: current_time[0],
        )
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=Path(tempfile.mkdtemp()),
            config_path=CONFIG_PATH,
        )
        sink.open(context)

        flush_calls = [0]
        original_flush = sink._flush_open_handles

        def tracked_flush():
            flush_calls[0] += 1
            original_flush()

        sink._flush_open_handles = tracked_flush
        alert = {
            "timestamp": "2026-03-04T11:38:37",
            "device_id": "S60ZBTPF",
            "field": "plug_power_w",
            "code": "test",
            "severity": "warning",
            "value": 61.71,
        }

        sink.append_quality_alert(alert, context)
        sink.append_quality_alert(alert, context)
        self.assertEqual(flush_calls[0], 0)
        sink.append_quality_alert(alert, context)
        self.assertEqual(flush_calls[0], 1)
        sink.close(self._summary_for(context.output_dir), context)

    def test_streaming_sink_flushes_on_elapsed_time(self) -> None:
        current_time = [0.0]
        sink = AppendFileStreamingSink(
            flush_every_records=100,
            flush_interval_seconds=2.0,
            monotonic_fn=lambda: current_time[0],
        )
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=Path(tempfile.mkdtemp()),
            config_path=CONFIG_PATH,
        )
        sink.open(context)

        flush_calls = [0]
        original_flush = sink._flush_open_handles

        def tracked_flush():
            flush_calls[0] += 1
            original_flush()

        sink._flush_open_handles = tracked_flush
        alert = {
            "timestamp": "2026-03-04T11:38:37",
            "device_id": "S60ZBTPF",
            "field": "plug_power_w",
            "code": "test",
            "severity": "warning",
            "value": 61.71,
        }

        sink.append_quality_alert(alert, context)
        self.assertEqual(flush_calls[0], 0)
        current_time[0] = 3.0
        sink.append_quality_alert(alert, context)
        self.assertEqual(flush_calls[0], 1)
        sink.close(self._summary_for(context.output_dir), context)

    def test_timestamp_paced_source_uses_relative_event_delay(self) -> None:
        event_a = RawTelemetryEvent(
            timestamp=datetime(2026, 3, 4, 11, 38, 37),
            device="S60ZBTPF",
            field="power",
            value="61.71",
        )
        event_b = RawTelemetryEvent(
            timestamp=datetime(2026, 3, 4, 11, 38, 42),
            device="S60ZBTPF",
            field="power",
            value="61.80",
        )
        sleeps = []
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "timestamp_paced",
            config_path=CONFIG_PATH,
            follow_event_timing=True,
            replay_speed_multiplier=1.0,
        )
        paced_source = TimestampPacedStreamSource(
            StaticStreamSource([event_a, event_b]),
            observer=NullStreamingObserver(),
            sleep_fn=sleeps.append,
        )

        self.assertEqual(list(paced_source.stream(context)), [event_a, event_b])
        self.assertEqual(sleeps, [5.0])

    def test_streaming_ai_stage_preserves_lazy_model_state(self) -> None:
        feature_window = FeatureWindow(
            timestamp=datetime(2026, 3, 4, 12, 0, 0),
            schema_version="1.0",
            values={
                "plug_power_mean_1m_w": 65.0,
                "plug_power_mean_5m_w": 64.0,
                "plug_power_delta_1m_w": 1.0,
                "device_to_household_ratio": 0.22,
                "plug_on_fraction_5m": 1.0,
                "plug_stale_flag": 0,
            },
        )
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "ai_stage",
            config_path=CONFIG_PATH,
        )
        runtime = StreamingPipelineRuntime(
            tick_interval_seconds=30,
            tick_clock_scale=0.0,
            idle_poll_seconds=0.1,
            group_flush_seconds=0.2,
        )
        stage = StreamingAiModelStage()
        sink = NullStreamingSink()
        observer = NullStreamingObserver()

        stage.on_start(runtime, context, sink, observer)
        last_label = None
        for index in range(6):
            runtime.latest_feature_window = FeatureWindow(
                timestamp=feature_window.timestamp + timedelta(seconds=30 * index),
                schema_version=feature_window.schema_version,
                values=dict(feature_window.values),
            )
            stage.on_tick(runtime.latest_feature_window.timestamp, runtime, context, sink, observer)
            last_label = [
                result.prediction_label
                for result in runtime.latest_model_results
                if result.model_id == "energy_anomaly_monitor"
            ][-1]

        self.assertEqual(last_label, "normal")

    def test_anomaly_model_produces_supported_labels(self) -> None:
        feature_window = FeatureWindow(
            timestamp=datetime(2026, 3, 4, 12, 0, 0),
            schema_version="1.0",
            values={
                "plug_power_mean_1m_w": 65.0,
                "plug_power_mean_5m_w": 64.0,
                "plug_power_delta_1m_w": 1.0,
                "device_to_household_ratio": 0.22,
                "plug_on_fraction_5m": 1.0,
                "plug_stale_flag": 0,
            },
        )
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "model",
            config_path=CONFIG_PATH,
        )
        model = EnergyAnomalyMonitorModel(context.config)
        results = []
        for _ in range(6):
            results = model.infer(build_model_input(feature_window), context)

        self.assertEqual(len(results), 1)
        self.assertIn(
            results[0].prediction_label,
            {"warming_up", "normal", "power_spike", "dominant_load_outlier", "sustained_high_load", "degraded_signal"},
        )

    def test_fhe_forecast_model_uses_edge_fhe_feature_contract(self) -> None:
        class FakeFheClient:
            def __init__(self):
                self.calls = []

            def forecast(self, values, model_name="forecast"):
                self.calls.append((values, model_name))
                return [123.45]

        fake_client = FakeFheClient()
        feature_time = datetime(2026, 3, 4, 12, 0, 0)
        forecast_feature = CloudForecastFeatureRecord(
            timestamp=feature_time,
            schema_version="1.0",
            feature_set_id="long_term_load_forecast_v1",
            values={column: 1.0 for column in FORECAST_NUMERIC_FEATURE_COLUMNS},
        )
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "fhe_model",
            config_path=CONFIG_PATH,
        )
        context.config["fhe_cloud"]["tasks"]["forecast"]["enabled"] = True
        model = FheLongTermLoadForecastTask(
            context.config,
            client_factory=lambda config: fake_client,
        )

        result = model.infer_forecast_feature(forecast_feature, context)[0]

        self.assertEqual(result.model_id, "fhe_long_term_load_forecast")
        self.assertEqual(result.inference_status, "ok")
        self.assertEqual(result.prediction_label, "fhe_long_term_load_forecast")
        self.assertEqual(result.prediction_score, 123.45)
        self.assertIn("timing_ms", json.loads(result.details))
        self.assertEqual(fake_client.calls[0][1], "forecast")
        self.assertEqual(set(fake_client.calls[0][0]), set(FORECAST_NUMERIC_FEATURE_COLUMNS))
        self.assertIn("plug_1_power_w", fake_client.calls[0][0])
        self.assertIn("plug_2_power_w", fake_client.calls[0][0])

    def test_fhe_task_caches_static_unavailable_configuration_failure(self) -> None:
        class MissingConfigClient:
            def __init__(self):
                self.calls = 0

            def forecast(self, values, model_name="forecast"):
                self.calls += 1
                raise RuntimeError(
                    "FHE API URL is not configured. Set HYFHENET_FHE_API_URL or API_URL."
                )

        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        fake_client = MissingConfigClient()
        task = FheLongTermLoadForecastTask(
            config,
            client_factory=lambda cfg: fake_client,
        )
        context = PipelineContext(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "fhe_static_unavailable",
            config=config,
        )
        feature = CloudForecastFeatureRecord(
            timestamp=datetime(2026, 4, 29, 13, 0, 0),
            schema_version="1.0",
            feature_set_id="long_term_load_forecast_v1",
            values={column: 1.0 for column in FORECAST_NUMERIC_FEATURE_COLUMNS},
        )

        first = task.infer_feature(feature, context)[0]
        second_timestamp = feature.timestamp + timedelta(seconds=60)
        second = task.infer_feature(
            CloudForecastFeatureRecord(
                timestamp=second_timestamp,
                schema_version=feature.schema_version,
                feature_set_id=feature.feature_set_id,
                values=feature.values,
            ),
            context,
        )[0]

        self.assertEqual(fake_client.calls, 1)
        self.assertEqual(first.inference_status, "unavailable")
        self.assertEqual(second.inference_status, "unavailable")
        self.assertEqual(second.timestamp, second_timestamp)

    def test_fhe_nilm_and_cohort_tasks_use_their_feature_contracts(self) -> None:
        class FakeFheClient:
            def __init__(self):
                self.calls = []

            def nilm(self, values, model_name="nilm"):
                self.calls.append(("nilm", values, model_name))
                return [10.0, 20.0, 30.0, 40.0, 50.0]

            def cohort(self, values, model_name="cohort"):
                self.calls.append(("cohort", values, model_name))
                return [2.0]

        fake_client = FakeFheClient()
        feature_time = datetime(2026, 3, 4, 12, 0, 0)
        nilm_feature = CloudForecastFeatureRecord(
            timestamp=feature_time,
            schema_version="1.0",
            feature_set_id="nilm_disaggregation_v1",
            values={column: 1.0 for column in NILM_INPUT_COLUMNS},
        )
        cohort_feature = CloudForecastFeatureRecord(
            timestamp=feature_time,
            schema_version="1.0",
            feature_set_id="cohort_benchmark_v1",
            values={column: 1.0 for column in COHORT_INPUT_COLUMNS},
        )
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "fhe_all_models",
            config_path=CONFIG_PATH,
        )

        nilm = FheNilmDisaggregationTask(
            context.config,
            client_factory=lambda config: fake_client,
        )
        cohort = FheCohortBenchmarkTask(
            context.config,
            client_factory=lambda config: fake_client,
        )

        nilm_result = nilm.infer_feature(nilm_feature, context)[0]
        cohort_result = cohort.infer_feature(cohort_feature, context)[0]

        self.assertEqual(nilm_result.model_id, "fhe_nilm_disaggregation")
        self.assertEqual(nilm_result.inference_status, "ok")
        self.assertEqual(nilm_result.prediction_label, "fhe_nilm_disaggregation")
        self.assertEqual(nilm_result.prediction_score, 150.0)
        self.assertEqual(cohort_result.model_id, "fhe_cohort_benchmark")
        self.assertEqual(cohort_result.inference_status, "ok")
        self.assertEqual(cohort_result.prediction_label, "fhe_cohort_benchmark")
        self.assertEqual(cohort_result.prediction_score, 2.0)
        self.assertEqual(fake_client.calls[0][0], "nilm")
        self.assertEqual(set(fake_client.calls[0][1]), set(NILM_INPUT_COLUMNS))
        self.assertEqual(fake_client.calls[1][0], "cohort")
        self.assertEqual(set(fake_client.calls[1][1]), set(COHORT_INPUT_COLUMNS))
        self.assertNotIn("cohort_label", fake_client.calls[1][1])

    def test_fhe_client_rejects_plain_http_by_default(self) -> None:
        client = HyfhenetFheClient(
            FheClientConfig(
                api_url="http://fhe.example",
                api_key="secret",
            )
        )

        with self.assertRaisesRegex(RuntimeError, "must use HTTPS"):
            client._validate_api_config()

    def test_fhe_client_allows_plain_http_only_with_lab_override(self) -> None:
        client = HyfhenetFheClient(
            FheClientConfig(
                api_url="http://localhost:8000",
                api_key="secret",
                allow_insecure_http=True,
            )
        )

        client._validate_api_config()

    def test_fhe_client_uses_architecture_specific_cache_and_headers(self) -> None:
        class FakeRequests:
            def __init__(self):
                self.post_kwargs = None

            def post(self, **kwargs):
                self.post_kwargs = kwargs
                return SimpleNamespace(status_code=200)

        with tempfile.TemporaryDirectory() as tmpdir:
            client = HyfhenetFheClient(
                FheClientConfig(
                    api_url="https://fhe.example",
                    api_key="secret",
                    cache_dir=Path(tmpdir) / "cache",
                    architecture="AMD64",
                )
            )
            fake_requests = FakeRequests()

            client._post_inference("forecast", "abc123", b"payload", fake_requests)

            self.assertEqual(client.architecture, "x86_64")
            self.assertEqual(client.models_dir, Path(tmpdir) / "cache" / "models" / "x86_64")
            self.assertEqual(client.keys_dir, Path(tmpdir) / "cache" / "fhe-keys" / "x86_64")
            self.assertEqual(fake_requests.post_kwargs["headers"]["X-Architecture"], "x86_64")
            self.assertEqual(fake_requests.post_kwargs["headers"]["X-Model-Version"], "abc123")

    def test_fhe_client_rejects_unsafe_model_archive_paths(self) -> None:
        class FakeResponse:
            status_code = 200

            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def raise_for_status(self) -> None:
                return None

            def iter_content(self, chunk_size=1024):
                yield self.payload

        class FakeRequests:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload
                self.get_kwargs = None

            def get(self, **kwargs):
                self.get_kwargs = kwargs
                return FakeResponse(self.payload)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            archive_path = tmp / "client-files.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("../outside.txt", "unsafe")
                archive.writestr("client.zip", "client")
                archive.writestr("data-pre-processor.pkl", "preprocessor")
            client = HyfhenetFheClient(
                FheClientConfig(
                    api_url="https://fhe.example",
                    api_key="secret",
                    cache_dir=tmp / "cache",
                    architecture="arm64",
                )
            )
            fake_requests = FakeRequests(archive_path.read_bytes())

            with self.assertRaisesRegex(RuntimeError, "Unsafe model archive path"):
                client._download_model_files(
                    "forecast",
                    tmp / "model",
                    fake_requests,
                    )
            self.assertEqual(fake_requests.get_kwargs["headers"]["X-Architecture"], "aarch64")
            self.assertFalse((tmp / "outside.txt").exists())

    def test_edge_forecast_training_saves_ridge_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = Path(tmpdir) / "edge_load_forecast_ridge.json"
            dataset_path = Path(tmpdir) / "edge_load_forecast_training_dataset.csv"
            report_path = Path(tmpdir) / "edge_load_forecast_training_report.md"
            artifact = train_edge_short_term_load_forecast(
                input_path=INPUT_PATH,
                config_path=CONFIG_PATH,
                output_model_path=model_path,
                output_dataset_path=dataset_path,
                output_report_path=report_path,
                horizon_minutes=1,
                alpha=3.0,
                train_fraction=0.7,
            )

            self.assertTrue(model_path.exists())
            self.assertTrue(dataset_path.exists())
            self.assertTrue(report_path.exists())
            self.assertEqual(artifact["model_id"], "edge_short_term_load_forecast")
            self.assertEqual(artifact["backend"], "ridge_regression")
            self.assertEqual(artifact["horizon_minutes"], 1)
            self.assertGreater(artifact["training_summary"]["test_count"], 0)
            self.assertEqual(
                artifact["training_summary"]["household_target_source"],
                "sum_of_configured_smart_plugs",
            )
            self.assertIn("rmse_w", artifact["metrics"])
            self.assertIn("test", artifact["metrics_by_split"])
            self.assertIn("linky_household_power_w", artifact["input_columns"])
            self.assertIn("plug_1_power_w", artifact["input_columns"])
            self.assertIn("plug_2_power_w", artifact["input_columns"])
            with dataset_path.open(newline="", encoding="utf-8") as handle:
                dataset_rows = list(csv.DictReader(handle))
            self.assertEqual(len(dataset_rows), artifact["training_summary"]["example_count"])
            self.assertIn("target_household_power_w", dataset_rows[0])
            self.assertIn("predicted_household_power_w", dataset_rows[0])

    def test_edge_forecast_training_rows_disable_remote_fhe(self) -> None:
        captured = {}

        class FakePipeline:
            def run(self, context):
                captured["fhe_enabled"] = context.config["fhe_cloud"]["enabled"]
                captured["edge_forecast_enabled"] = context.config["edge_models"][
                    "short_term_load_forecast"
                ]["enabled"]
                path = context.output_dir / "cloud_forecast_training_examples.csv"
                with path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "timestamp",
                            "schema_version",
                            "feature_set_id",
                            "target_timestamp",
                            "target_household_power_w",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "timestamp": "2026-04-29T13:00:00",
                            "schema_version": "1.0",
                            "feature_set_id": "long_term_load_forecast_v1",
                            "target_timestamp": "2026-04-29T13:01:00",
                            "target_household_power_w": "42.0",
                        }
                    )

        with mock.patch.object(
                edge_forecast_training,
                "build_gateway_stream_pipeline_for_context",
                return_value=FakePipeline(),
        ):
            rows = edge_forecast_training._build_training_rows(
                INPUT_PATH,
                CONFIG_PATH,
                horizon_minutes=1,
            )

        self.assertEqual(len(rows), 1)
        self.assertFalse(captured["fhe_enabled"])
        self.assertFalse(captured["edge_forecast_enabled"])

    def test_load_event_gate_detects_hysteresis_transitions(self) -> None:
        context = build_gateway_stream_context(
            input_path=INPUT_PATH,
            output_dir=ROOT / "artifacts" / "event_gate",
            config_path=CONFIG_PATH,
        )
        context.config["edge_event_gates"]["load_event_gate"]["emit_initial_state_marker"] = False
        detector = LoadEventGateDetector(context.config)
        windows = [
            FeatureWindow(
                timestamp=datetime(2026, 3, 4, 12, 0, 0) + timedelta(seconds=30 * index),
                schema_version="1.0",
                values={
                    "plug_power_latest_w": value,
                    "plug_power_mean_1m_w": value,
                    "device_to_household_ratio": 0.2,
                    "plug_stale_flag": 0,
                    "household_stale_flag": 0,
                },
            )
            for index, value in enumerate([0.0, 65.0, 62.0, 2.0])
        ]

        markers = []
        for window in windows:
            markers.extend(detector.evaluate(window, context))

        self.assertEqual([marker.event_type for marker in markers], ["switch_on", "switch_off"])
        self.assertTrue(all(marker.should_forward == 1 for marker in markers))

    @staticmethod
    def _summary_for(output_dir: Path) -> GatewayStreamRunSummary:
        return GatewayStreamRunSummary(
            input_path=str(INPUT_PATH),
            output_dir=str(output_dir),
            raw_event_count=0,
            normalized_event_count=0,
            snapshot_count=0,
            quality_alert_count=0,
            feature_window_count=0,
            cloud_forecast_feature_count=0,
            cloud_forecast_training_example_count=0,
            load_event_marker_count=0,
            edge_forecast_evaluation_count=0,
            service_result_count=0,
            model_input_count=0,
            model_result_count=0,
            household_power_source="sum_of_configured_smart_plugs",
            time_range={"start": None, "end": None},
            devices={},
        )


if __name__ == "__main__":
    unittest.main()
