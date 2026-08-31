from __future__ import annotations

import csv
import json
import time
from datetime import datetime
from typing import Any

from ..fhe.features import (
    COHORT_FEATURE_COLUMNS,
    FORECAST_FEATURE_COLUMNS,
    FORECAST_TRAINING_COLUMNS,
    NILM_FEATURE_COLUMNS,
    FheForecastFeaturePreparer,
    FheCohortFeaturePreparer,
    FheNilmFeaturePreparer,
)
from ..core.interfaces import PipelineContext, StreamingSink
from ..core.models import (
    CloudCohortFeatureRecord,
    CloudForecastFeatureRecord,
    CloudForecastTrainingExample,
    CloudNilmFeatureRecord,
    EdgeForecastEvaluationRecord,
    EdgeServiceResult,
    GatewayStreamRunSummary,
    LoadEventMarker,
    ModelInferenceResult,
    LatencySample,
)


class AppendFileStreamingSink(StreamingSink):
    """Writes the compact artifact set for replay and live edge runs."""

    def __init__(
        self,
        flush_every_records: int | None = None,
        flush_interval_seconds: float | None = None,
        monotonic_fn=None,
    ) -> None:
        self.flush_every_records = flush_every_records
        self.flush_interval_seconds = flush_interval_seconds
        self.monotonic_fn = monotonic_fn or time.monotonic
        self._edge_results_handle = None
        self._forecast_features_handle = None
        self._forecast_features_writer = None
        self._nilm_features_handle = None
        self._nilm_features_writer = None
        self._cohort_features_handle = None
        self._cohort_features_writer = None
        self._forecast_training_handle = None
        self._forecast_training_writer = None
        self._latency_handle = None
        self._latency_writer = None
        self._latency_cycle = 0
        self._dirty_record_count = 0
        self._last_flush_at = 0.0

    def open(self, context: PipelineContext) -> None:
        context.output_dir.mkdir(parents=True, exist_ok=True)
        stream_config = context.config.get("gateway_stream", {})
        self.flush_every_records = int(
            self.flush_every_records or stream_config.get("flush_every_records", 10)
        )
        self.flush_interval_seconds = float(
            self.flush_interval_seconds
            if self.flush_interval_seconds is not None
            else stream_config.get("flush_interval_seconds", 2.0)
        )
        self._dirty_record_count = 0
        self._last_flush_at = self.monotonic_fn()
        self._latency_cycle = 0

        self._edge_results_handle = (context.output_dir / "edge_results.jsonl").open(
            "w", encoding="utf-8", newline=""
        )
        self._forecast_features_handle = (
            context.output_dir / "cloud_forecast_features.csv"
        ).open("w", encoding="utf-8", newline="")
        self._forecast_features_writer = csv.DictWriter(
            self._forecast_features_handle,
            fieldnames=FORECAST_FEATURE_COLUMNS,
        )
        self._forecast_features_writer.writeheader()

        self._nilm_features_handle = (
            context.output_dir / "cloud_nilm_features.csv"
        ).open("w", encoding="utf-8", newline="")
        self._nilm_features_writer = csv.DictWriter(
            self._nilm_features_handle,
            fieldnames=NILM_FEATURE_COLUMNS,
        )
        self._nilm_features_writer.writeheader()

        self._cohort_features_handle = (
            context.output_dir / "cloud_cohort_features.csv"
        ).open("w", encoding="utf-8", newline="")
        self._cohort_features_writer = csv.DictWriter(
            self._cohort_features_handle,
            fieldnames=COHORT_FEATURE_COLUMNS,
        )
        self._cohort_features_writer.writeheader()

        self._forecast_training_handle = (
            context.output_dir / "cloud_forecast_training_examples.csv"
        ).open("w", encoding="utf-8", newline="")
        self._forecast_training_writer = csv.DictWriter(
            self._forecast_training_handle,
            fieldnames=FORECAST_TRAINING_COLUMNS,
        )
        self._forecast_training_writer.writeheader()
        self._latency_handle = (context.output_dir / "latency.csv").open(
            "w", encoding="utf-8", newline=""
        )
        self._latency_writer = csv.DictWriter(
            self._latency_handle,
            fieldnames=["cycle", *_latency_fieldnames()],
        )
        self._latency_writer.writeheader()
        self._flush_open_handles()

    def append_quality_alert(self, alert: dict[str, Any], context: PipelineContext) -> None:
        self._write_edge_result("quality_alert", alert)

    def append_service_result(
        self,
        service_result: EdgeServiceResult,
        context: PipelineContext,
    ) -> None:
        self._write_edge_result("edge_service_result", service_result.to_record())

    def append_load_event_marker(
        self,
        marker: LoadEventMarker,
        context: PipelineContext,
    ) -> None:
        self._write_edge_result("load_event_marker", marker.to_record())

    def append_model_result(
        self,
        model_result: ModelInferenceResult,
        context: PipelineContext,
    ) -> None:
        self._write_edge_result("model_inference_result", model_result.to_record())

    def append_edge_forecast_evaluation(
        self,
        evaluation: EdgeForecastEvaluationRecord,
        context: PipelineContext,
    ) -> None:
        self._write_edge_result("edge_forecast_evaluation", evaluation.to_record())

    def append_cloud_forecast_feature(
        self,
        forecast_feature: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> None:
        self._forecast_features_writer.writerow(forecast_feature.to_record())
        self._mark_write()

    def append_cloud_nilm_feature(
        self,
        nilm_feature: CloudNilmFeatureRecord,
        context: PipelineContext,
    ) -> None:
        self._nilm_features_writer.writerow(nilm_feature.to_record())
        self._mark_write()

    def append_cloud_cohort_feature(
        self,
        cohort_feature: CloudCohortFeatureRecord,
        context: PipelineContext,
    ) -> None:
        self._cohort_features_writer.writerow(cohort_feature.to_record())
        self._mark_write()

    def append_cloud_forecast_training_example(
        self,
        training_example: CloudForecastTrainingExample,
        context: PipelineContext,
    ) -> None:
        self._forecast_training_writer.writerow(training_example.to_record())
        self._mark_write()

    def append_latency_sample(
        self,
        latency_sample: LatencySample,
        context: PipelineContext,
    ) -> None:
        if self._latency_writer is None:
            return
        self._latency_cycle += 1
        self._latency_writer.writerow(
            {"cycle": self._latency_cycle, **latency_sample.to_record()}
        )
        self._mark_write()

    def close(
        self,
        summary: GatewayStreamRunSummary,
        context: PipelineContext,
    ) -> None:
        self._flush_open_handles()
        (context.output_dir / "run_summary.json").write_text(
            json.dumps(summary.to_record(), indent=2),
            encoding="utf-8",
        )
        (context.output_dir / "performance_metrics.json").write_text(
            json.dumps(
                {
                    "performance_summary": summary.performance_summary,
                    "latency_summary": summary.latency_summary,
                    "forecast_quality_summary": summary.forecast_quality_summary,
                    "model_result_summary": summary.model_result_summary,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        metadata = context.config.get("_runtime_cloud_forecast_metadata")
        if metadata is None and context.config.get("cloud_forecast", {}).get("enabled", True):
            metadata = FheForecastFeaturePreparer(context.config).metadata()
        if metadata is not None:
            (context.output_dir / "cloud_forecast_feature_contract.json").write_text(
                json.dumps(metadata, indent=2),
                encoding="utf-8",
            )
        contracts = context.config.get("_runtime_cloud_model_contracts")
        if contracts is None:
            contracts = {
                "forecast": metadata,
                "nilm": FheNilmFeaturePreparer(context.config).metadata(),
                "cohort": FheCohortFeaturePreparer(context.config).metadata(),
            }
        (context.output_dir / "cloud_model_feature_contracts.json").write_text(
            json.dumps(contracts, indent=2),
            encoding="utf-8",
        )
        for handle in (
            self._edge_results_handle,
            self._forecast_features_handle,
            self._nilm_features_handle,
            self._cohort_features_handle,
            self._forecast_training_handle,
            self._latency_handle,
        ):
            if handle is not None:
                handle.close()

    def _write_edge_result(self, record_type: str, payload: dict[str, Any]) -> None:
        if self._edge_results_handle is None:
            return
        self._edge_results_handle.write(
            json.dumps(
                {"edge_record_type": record_type, **payload},
                ensure_ascii=True,
            )
            + "\n"
        )
        self._mark_write()

    def _mark_write(self) -> None:
        self._dirty_record_count += 1
        if self._should_flush():
            self._flush_open_handles()

    def _should_flush(self) -> bool:
        if self._dirty_record_count <= 0:
            return False
        if int(self.flush_every_records or 1) <= 1:
            return True
        if self._dirty_record_count >= int(self.flush_every_records or 1):
            return True
        return (self.monotonic_fn() - self._last_flush_at) >= float(
            self.flush_interval_seconds or 0.0
        )

    def _flush_open_handles(self) -> None:
        for handle in (
            self._edge_results_handle,
            self._forecast_features_handle,
            self._nilm_features_handle,
            self._cohort_features_handle,
            self._forecast_training_handle,
            self._latency_handle,
        ):
            if handle is not None:
                handle.flush()
        self._dirty_record_count = 0
        self._last_flush_at = self.monotonic_fn()


class NullStreamingSink(StreamingSink):
    pass


def _latency_fieldnames() -> list[str]:
    return list(
        LatencySample(
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
        ).to_record()
    )
