from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .models import (
    CloudCohortFeatureRecord,
    CloudForecastFeatureRecord,
    CloudForecastTrainingExample,
    CloudNilmFeatureRecord,
    EdgeForecastEvaluationRecord,
    EdgeServiceResult,
    FeatureWindow,
    GatewayStreamRunSummary,
    LatencySample,
    LoadEventMarker,
    ModelInferenceResult,
    ModelInput,
    NormalizedEvent,
    RawTelemetryEvent,
    StreamingPipelineRuntime,
)


@dataclass(frozen=True)
class PipelineContext:
    input_path: Path | str
    output_dir: Path
    config: dict[str, Any]


class EdgeService(ABC):
    @abstractmethod
    def evaluate(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> list[EdgeServiceResult]:
        raise NotImplementedError


class EdgeEventDetector(ABC):
    @abstractmethod
    def evaluate(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> list[LoadEventMarker]:
        raise NotImplementedError


class EdgeModel(ABC):
    @abstractmethod
    def infer(
        self,
        model_input: ModelInput,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        raise NotImplementedError


class StreamingEventSource(ABC):
    @abstractmethod
    def stream(self, context: PipelineContext) -> Iterable[RawTelemetryEvent]:
        raise NotImplementedError


class StreamingObserver:
    def on_stream_start(self, context: PipelineContext) -> None:
        return None

    def on_stream_wait(
        self,
        delay_seconds: float,
        previous_event: RawTelemetryEvent,
        next_event: RawTelemetryEvent,
        context: PipelineContext,
    ) -> None:
        return None

    def on_event_group(self, events: list[RawTelemetryEvent], context: PipelineContext) -> None:
        return None

    def on_pipeline_step(
        self,
        step: str,
        detail: str,
        context: PipelineContext,
    ) -> None:
        return None

    def on_normalized_event(
        self,
        raw_event: RawTelemetryEvent,
        normalized_event: NormalizedEvent,
        context: PipelineContext,
    ) -> None:
        return None

    def on_quality_alert(self, alert: dict[str, Any], context: PipelineContext) -> None:
        return None

    def on_snapshot(self, snapshot: dict[str, Any], context: PipelineContext) -> None:
        return None

    def on_feature_window(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> None:
        return None

    def on_cloud_forecast_feature(
        self,
        forecast_feature: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> None:
        return None

    def on_cloud_nilm_feature(
        self,
        nilm_feature: CloudNilmFeatureRecord,
        context: PipelineContext,
    ) -> None:
        return None

    def on_cloud_cohort_feature(
        self,
        cohort_feature: CloudCohortFeatureRecord,
        context: PipelineContext,
    ) -> None:
        return None

    def on_cloud_forecast_training_example(
        self,
        training_example: CloudForecastTrainingExample,
        context: PipelineContext,
    ) -> None:
        return None

    def on_service_result(
        self,
        service_result: EdgeServiceResult,
        context: PipelineContext,
    ) -> None:
        return None

    def on_load_event_marker(
        self,
        marker: LoadEventMarker,
        context: PipelineContext,
    ) -> None:
        return None

    def on_model_result(
        self,
        model_result: ModelInferenceResult,
        context: PipelineContext,
    ) -> None:
        return None

    def on_stream_complete(
        self,
        summary: GatewayStreamRunSummary,
        context: PipelineContext,
    ) -> None:
        return None


class StreamingSink:
    def open(self, context: PipelineContext) -> None:
        return None

    def append_normalized_event(
        self,
        raw_event: RawTelemetryEvent,
        normalized_event: NormalizedEvent,
        context: PipelineContext,
    ) -> None:
        return None

    def append_quality_alert(self, alert: dict[str, Any], context: PipelineContext) -> None:
        return None

    def append_snapshot(self, snapshot: dict[str, Any], context: PipelineContext) -> None:
        return None

    def append_feature_window(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> None:
        return None

    def append_cloud_forecast_feature(
        self,
        forecast_feature: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> None:
        return None

    def append_cloud_nilm_feature(
        self,
        nilm_feature: CloudNilmFeatureRecord,
        context: PipelineContext,
    ) -> None:
        return None

    def append_cloud_cohort_feature(
        self,
        cohort_feature: CloudCohortFeatureRecord,
        context: PipelineContext,
    ) -> None:
        return None

    def append_cloud_forecast_training_example(
        self,
        training_example: CloudForecastTrainingExample,
        context: PipelineContext,
    ) -> None:
        return None

    def append_service_result(
        self,
        service_result: EdgeServiceResult,
        context: PipelineContext,
    ) -> None:
        return None

    def append_load_event_marker(
        self,
        marker: LoadEventMarker,
        context: PipelineContext,
    ) -> None:
        return None

    def append_model_input(
        self,
        model_input: ModelInput,
        context: PipelineContext,
    ) -> None:
        return None

    def append_model_result(
        self,
        model_result: ModelInferenceResult,
        context: PipelineContext,
    ) -> None:
        return None

    def append_edge_forecast_evaluation(
        self,
        evaluation: EdgeForecastEvaluationRecord,
        context: PipelineContext,
    ) -> None:
        return None

    def append_latency_sample(
        self,
        latency_sample: LatencySample,
        context: PipelineContext,
    ) -> None:
        return None

    def close(
        self,
        summary: GatewayStreamRunSummary,
        context: PipelineContext,
    ) -> None:
        return None


class StreamingPipelineStage(ABC):
    def on_start(
        self,
        runtime: StreamingPipelineRuntime,
        context: PipelineContext,
        sink: StreamingSink,
        observer: StreamingObserver,
    ) -> None:
        return None

    def on_event_group(
        self,
        events: list[RawTelemetryEvent],
        runtime: StreamingPipelineRuntime,
        context: PipelineContext,
        sink: StreamingSink,
        observer: StreamingObserver,
    ) -> None:
        return None

    def on_tick(
        self,
        tick_timestamp,
        runtime: StreamingPipelineRuntime,
        context: PipelineContext,
        sink: StreamingSink,
        observer: StreamingObserver,
    ) -> None:
        return None

    def on_complete(
        self,
        runtime: StreamingPipelineRuntime,
        context: PipelineContext,
        sink: StreamingSink,
        observer: StreamingObserver,
    ) -> None:
        return None

