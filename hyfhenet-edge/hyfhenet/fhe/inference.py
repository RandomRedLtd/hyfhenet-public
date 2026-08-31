from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import timedelta
from typing import Any, Callable, Sequence

from ..core.interfaces import PipelineContext
from ..core.models import (
    CloudCohortFeatureRecord,
    CloudForecastFeatureRecord,
    CloudNilmFeatureRecord,
    ModelInferenceResult,
)
from .client import DEFAULT_MODEL_NAME, FheClientConfig, HyfhenetFheClient
from .features import (
    COHORT_INPUT_COLUMNS,
    FORECAST_INPUT_COLUMNS,
    NILM_INPUT_COLUMNS,
)

FHE_INPUT_CONTRACT_VERSION = "1.0"
FHE_OUTPUT_CONTRACT_VERSION = "1.0"


class FheInferenceRunner:
    def __init__(self, tasks: Sequence["FheInferenceTask"]) -> None:
        self.tasks = list(tasks)

    def infer_forecast_feature(
        self,
        forecast_feature: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        return self.infer_cloud_features(forecast_feature, None, None, context)

    def infer_cloud_features(
        self,
        forecast_feature: CloudForecastFeatureRecord | None,
        nilm_feature: CloudNilmFeatureRecord | None,
        cohort_feature: CloudCohortFeatureRecord | None,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        results: list[ModelInferenceResult] = []
        for task in self.tasks:
            results.extend(
                task.infer_cloud_features(
                    forecast_feature,
                    nilm_feature,
                    cohort_feature,
                    context,
                )
            )
        return results

    def task_ids(self) -> list[str]:
        return [getattr(task, "TASK_ID", task.__class__.__name__) for task in self.tasks]


class FheInferenceTask:
    TASK_ID = "fhe_inference_task"
    MODEL_NAME = ""

    def infer_forecast_feature(
        self,
        forecast_feature: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        return self.infer_cloud_features(forecast_feature, None, None, context)

    def infer_cloud_features(
        self,
        forecast_feature: CloudForecastFeatureRecord | None,
        nilm_feature: CloudNilmFeatureRecord | None,
        cohort_feature: CloudCohortFeatureRecord | None,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        feature_by_model = {
            "forecast": forecast_feature,
            "nilm": nilm_feature,
            "cohort": cohort_feature,
        }
        feature = feature_by_model.get(self.MODEL_NAME)
        if feature is None:
            return []
        return self.infer_feature(feature, context)

    def infer_feature(
        self,
        feature_record: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        raise NotImplementedError


class RemoteFheTask(FheInferenceTask):
    INPUT_COLUMNS: list[str] = []
    CLIENT_METHOD = "infer"
    SUCCESS_LABEL = "fhe_remote_result"
    UNAVAILABLE_LABEL = "fhe_remote_unavailable"

    def __init__(
        self,
        config: dict[str, Any],
        client_factory: Callable[[FheClientConfig], Any] | None = None,
    ) -> None:
        self.cloud_config = config["fhe_cloud"]
        self.config = _task_config(self.cloud_config, self.MODEL_NAME)
        self.client_factory = client_factory or HyfhenetFheClient
        self.client = None
        self._static_unavailable_details: str | None = None

    def infer_feature(
        self,
        feature_record: CloudForecastFeatureRecord,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        started_at = time.perf_counter()
        if self._static_unavailable_details is not None:
            return [
                self._result(
                    feature_record,
                    status="unavailable",
                    label=self.UNAVAILABLE_LABEL,
                    prediction_score=None,
                    load_score=None,
                    details=self._static_unavailable_details,
                )
            ]
        try:
            client = self._get_client()
            feature_values = {
                column: feature_record.values.get(column, 0.0)
                for column in self.INPUT_COLUMNS
            }
            infer_method = getattr(client, self.CLIENT_METHOD)
            prediction_values = infer_method(
                feature_values,
                model_name=str(self.config.get("model_name", self.MODEL_NAME)),
            )
            if not prediction_values:
                raise RuntimeError("FHE inference returned no numeric prediction.")
            result = self._success_result(feature_record, prediction_values)
            return [self._with_timing(result, client, started_at)]
        except Exception as exc:
            details = f"{exc.__class__.__name__}: {exc}"
            if _cacheable_unavailable_exception(exc):
                self._static_unavailable_details = details
            return [
                self._result(
                    feature_record,
                    status="unavailable",
                    label=self.UNAVAILABLE_LABEL,
                    prediction_score=None,
                    load_score=None,
                    details=details,
                )
            ]

    def _success_result(
        self,
        feature_record: CloudForecastFeatureRecord,
        prediction_values: list[float],
    ) -> ModelInferenceResult:
        prediction = _as_float(prediction_values[0])
        if prediction is None:
            raise RuntimeError("FHE inference returned a non-numeric prediction.")
        details = json.dumps(
            {
                "feature_set_id": feature_record.feature_set_id,
                "feature_schema_version": feature_record.schema_version,
                "model_name": self.config.get("model_name", self.MODEL_NAME),
                "prediction_values": [round(float(value), 4) for value in prediction_values],
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        return self._result(
            feature_record,
            status="ok",
            label=self.SUCCESS_LABEL,
            prediction_score=round(prediction, 4),
            load_score=round(prediction, 4),
            details=details,
        )

    def _with_timing(
        self,
        result: ModelInferenceResult,
        client,
        started_at: float,
    ) -> ModelInferenceResult:
        timing_ms = {
            key: round(float(value), 3)
            for key, value in (getattr(client, "last_timing_ms", {}) or {}).items()
        }
        timing_ms.setdefault(
            "fhe_task_total_ms",
            round(max(time.perf_counter() - started_at, 0.0) * 1000.0, 3),
        )
        try:
            details = json.loads(result.details)
            if not isinstance(details, dict):
                details = {"details": result.details}
        except (TypeError, json.JSONDecodeError):
            details = {"details": result.details}
        details["architecture"] = getattr(client, "architecture", None)
        details["timing_ms"] = timing_ms
        return replace(
            result,
            details=json.dumps(details, ensure_ascii=True, sort_keys=True),
        )

    def _get_client(self):
        if self.client is None:
            self.client = self.client_factory(
                FheClientConfig(
                    api_url=self.cloud_config.get("api_url"),
                    api_key=self.cloud_config.get("api_key"),
                    cache_dir=self.cloud_config.get("cache_dir"),
                    client_cert_path=self.cloud_config.get("client_cert_path"),
                    client_key_path=self.cloud_config.get("client_key_path"),
                    ca_bundle_path=self.cloud_config.get("ca_bundle_path"),
                    architecture=self.cloud_config.get("architecture"),
                    request_timeout_seconds=float(
                        self.cloud_config.get("request_timeout_seconds", 60.0)
                    ),
                    allow_insecure_http=bool(
                        self.cloud_config.get("allow_insecure_http", False)
                    ),
                )
            )
        return self.client

    def _result(
        self,
        feature_record: CloudForecastFeatureRecord,
        status: str,
        label: str,
        prediction_score: float | None,
        load_score: float | None,
        details: str,
    ) -> ModelInferenceResult:
        return ModelInferenceResult(
            timestamp=feature_record.timestamp,
            model_id=self.TASK_ID,
            model_version=str(self.config.get("model_version", "1.0")),
            backend=str(self.config.get("backend", "concrete_ml_remote_fhe")),
            input_contract_version=FHE_INPUT_CONTRACT_VERSION,
            output_contract_version=FHE_OUTPUT_CONTRACT_VERSION,
            inference_status=status,
            prediction_label=label,
            prediction_score=prediction_score,
            anomaly_score=None,
            load_score=load_score,
            details=details,
        )


class FheLongTermLoadForecastTask(RemoteFheTask):
    TASK_ID = "fhe_long_term_load_forecast"
    MODEL_NAME = "forecast"
    INPUT_COLUMNS = FORECAST_INPUT_COLUMNS
    CLIENT_METHOD = "forecast"
    SUCCESS_LABEL = "fhe_long_term_load_forecast"
    UNAVAILABLE_LABEL = "fhe_long_term_forecast_unavailable"

    def _success_result(
        self,
        feature_record: CloudForecastFeatureRecord,
        prediction_values: list[float],
    ) -> ModelInferenceResult:
        predicted_w = _as_float(prediction_values[0])
        if predicted_w is None:
            raise RuntimeError("FHE inference returned no numeric forecast.")
        horizon_minutes = int(self.config.get("horizon_minutes", 1))
        target_timestamp = feature_record.timestamp + timedelta(minutes=horizon_minutes)
        details = json.dumps(
            {
                "feature_set_id": feature_record.feature_set_id,
                "feature_schema_version": feature_record.schema_version,
                "horizon_minutes": horizon_minutes,
                "model_name": self.config.get("model_name", DEFAULT_MODEL_NAME),
                "target_timestamp": target_timestamp.isoformat(),
                "predicted_household_power_w": round(predicted_w, 4),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        return self._result(
            feature_record,
            status="ok",
            label=self.SUCCESS_LABEL,
            prediction_score=round(predicted_w, 4),
            load_score=round(predicted_w, 4),
            details=details,
        )


class FheNilmDisaggregationTask(RemoteFheTask):
    TASK_ID = "fhe_nilm_disaggregation"
    MODEL_NAME = "nilm"
    INPUT_COLUMNS = NILM_INPUT_COLUMNS
    CLIENT_METHOD = "nilm"
    SUCCESS_LABEL = "fhe_nilm_disaggregation"
    UNAVAILABLE_LABEL = "fhe_nilm_unavailable"

    def _success_result(
        self,
        feature_record: CloudNilmFeatureRecord,
        prediction_values: list[float],
    ) -> ModelInferenceResult:
        output_names = [
            "plug_1_power_w",
            "plug_2_power_w",
            "hvac_power_w",
            "baseload_power_w",
            "other_power_w",
        ]
        component_values = {
            name: round(float(prediction_values[index]), 4)
            for index, name in enumerate(output_names)
            if index < len(prediction_values)
        }
        total_w = round(sum(component_values.values()), 4)
        details = json.dumps(
            {
                "feature_set_id": feature_record.feature_set_id,
                "feature_schema_version": feature_record.schema_version,
                "model_name": self.config.get("model_name", self.MODEL_NAME),
                "predicted_components_w": component_values,
                "predicted_total_w": total_w,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        return self._result(
            feature_record,
            status="ok",
            label=self.SUCCESS_LABEL,
            prediction_score=total_w,
            load_score=total_w,
            details=details,
        )


class FheCohortBenchmarkTask(RemoteFheTask):
    TASK_ID = "fhe_cohort_benchmark"
    MODEL_NAME = "cohort"
    INPUT_COLUMNS = COHORT_INPUT_COLUMNS
    CLIENT_METHOD = "cohort"
    SUCCESS_LABEL = "fhe_cohort_benchmark"
    UNAVAILABLE_LABEL = "fhe_cohort_unavailable"

    def _success_result(
        self,
        feature_record: CloudCohortFeatureRecord,
        prediction_values: list[float],
    ) -> ModelInferenceResult:
        cohort_code = _as_float(prediction_values[0])
        if cohort_code is None:
            raise RuntimeError("FHE inference returned no numeric cohort code.")
        rounded_code = round(cohort_code)
        details = json.dumps(
            {
                "feature_set_id": feature_record.feature_set_id,
                "feature_schema_version": feature_record.schema_version,
                "model_name": self.config.get("model_name", self.MODEL_NAME),
                "predicted_cohort_code": rounded_code,
                "raw_prediction": round(cohort_code, 4),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        return self._result(
            feature_record,
            status="ok",
            label=self.SUCCESS_LABEL,
            prediction_score=float(rounded_code),
            load_score=None,
            details=details,
        )


def build_default_fhe_inference_tasks(config: dict[str, Any]) -> list[FheInferenceTask]:
    cloud_config = config.get("fhe_cloud", {})
    if not cloud_config.get("enabled", True):
        return []
    task_classes = [
        FheLongTermLoadForecastTask,
        FheNilmDisaggregationTask,
        FheCohortBenchmarkTask,
    ]
    tasks: list[FheInferenceTask] = []
    for task_class in task_classes:
        task_config = _task_config(cloud_config, task_class.MODEL_NAME)
        if task_config.get("enabled", True):
            tasks.append(task_class(config))
    return tasks


def _task_config(
    cloud_config: dict[str, Any],
    model_name: str,
    *aliases: str,
) -> dict[str, Any]:
    tasks = cloud_config.get("tasks", {})
    for key in (model_name, *aliases):
        if key in tasks:
            return tasks[key]
    return {}


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _cacheable_unavailable_exception(exc: Exception) -> bool:
    if not isinstance(exc, RuntimeError):
        return False
    message = str(exc)
    return message.startswith(
        (
            "FHE inference requires platform FHE dependencies.",
            "FHE API URL is not configured.",
            "FHE API key is not configured.",
            "FHE API URL must use HTTPS.",
        )
    )
