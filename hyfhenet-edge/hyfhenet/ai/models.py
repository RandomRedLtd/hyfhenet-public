from __future__ import annotations

import math
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Sequence

from ..core.interfaces import EdgeModel, PipelineContext
from ..core.models import ModelInferenceResult, ModelInput

MODEL_INPUT_CONTRACT_VERSION = "1.0"
MODEL_OUTPUT_CONTRACT_VERSION = "1.0"

MODEL_RESULT_COLUMNS = [
    "timestamp",
    "model_id",
    "model_version",
    "backend",
    "input_contract_version",
    "output_contract_version",
    "inference_status",
    "prediction_label",
    "prediction_score",
    "anomaly_score",
    "load_score",
    "details",
]


@dataclass
class RunningBaseline:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def zscore(self, value: float) -> float:
        if self.count < 2:
            return 0.0
        variance = self.m2 / max(self.count - 1, 1)
        std = math.sqrt(max(variance, 1e-6))
        return (value - self.mean) / max(std, 1e-3)

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2


class EdgeModelRunner:
    def __init__(self, models: Sequence[EdgeModel]) -> None:
        self.models = list(models)

    def infer(
        self,
        model_input: ModelInput,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        results: list[ModelInferenceResult] = []
        for model in self.models:
            results.extend(model.infer(model_input, context))
        return results

    def model_ids(self) -> list[str]:
        return [getattr(model, "MODEL_ID", model.__class__.__name__) for model in self.models]


class EnergyAnomalyMonitorModel(EdgeModel):
    MODEL_ID = "energy_anomaly_monitor"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config["edge_models"]["energy_anomaly_monitor"]
        self.power_baseline = RunningBaseline()
        self.ratio_baseline = RunningBaseline()

    def infer(
        self,
        model_input: ModelInput,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        plug_mean_1m = _as_float(model_input.feature_window.get("plug_power_mean_1m_w"))
        plug_mean_5m = _as_float(model_input.feature_window.get("plug_power_mean_5m_w"))
        plug_delta_1m = _as_float(model_input.feature_window.get("plug_power_delta_1m_w")) or 0.0
        ratio = _as_float(model_input.feature_window.get("device_to_household_ratio"))
        on_fraction_5m = _as_float(model_input.feature_window.get("plug_on_fraction_5m")) or 0.0
        stale_flag = int(model_input.feature_window.get("plug_stale_flag") or 0)

        if plug_mean_1m is None:
            return [
                self._result(
                    model_input,
                    inference_status="insufficient_data",
                    prediction_label="insufficient_data",
                    prediction_score=0.0,
                    anomaly_score=0.0,
                    load_score=0.0,
                    details="missing plug_power_mean_1m_w",
                )
            ]

        load_score = min(
            1.0,
            max(0.0, plug_mean_1m / max(float(self.config["active_power_reference_w"]), 1.0)),
        )

        if stale_flag:
            return [
                self._result(
                    model_input,
                    inference_status="degraded",
                    prediction_label="degraded_signal",
                    prediction_score=0.75,
                    anomaly_score=round(float(self.config["stale_penalty"]), 4),
                    load_score=round(load_score, 4),
                    details="stale plug signal",
                )
            ]

        history_count = self.power_baseline.count
        if history_count < int(self.config["warmup_windows"]):
            self._update_baselines(plug_mean_1m, ratio)
            return [
                self._result(
                    model_input,
                    inference_status="warmup",
                    prediction_label="warming_up",
                    prediction_score=0.5,
                    anomaly_score=0.0,
                    load_score=round(load_score, 4),
                    details=f"warmup history_count={history_count + 1}",
                )
            ]

        z_power = abs(self.power_baseline.zscore(plug_mean_1m))
        z_ratio = abs(self.ratio_baseline.zscore(ratio)) if ratio is not None else 0.0
        sustained_threshold = float(self.config["sustained_load_threshold_w"])
        dominant_share_min = float(self.config["dominant_share_min"])

        anomaly_score = min(
            1.0,
            (0.6 * min(z_power / max(float(self.config["power_zscore_threshold"]), 1.0), 1.0))
            + (0.4 * min(z_ratio / max(float(self.config["ratio_zscore_threshold"]), 1.0), 1.0)),
        )

        sustained_share_min = float(self.config["sustained_share_min"])
        sustained_zscore_threshold = float(self.config["sustained_zscore_threshold"])

        if z_power >= float(self.config["power_zscore_threshold"]) and plug_delta_1m > 0:
            label = "power_spike"
            status = "alert"
        elif ratio is not None and z_ratio >= float(self.config["ratio_zscore_threshold"]) and ratio >= dominant_share_min:
            label = "dominant_load_outlier"
            status = "alert"
        elif (
            (plug_mean_5m or plug_mean_1m) >= sustained_threshold
            and on_fraction_5m >= float(self.config["sustained_on_fraction_threshold"])
            and ratio is not None
            and ratio >= sustained_share_min
            and z_power >= sustained_zscore_threshold
        ):
            label = "sustained_high_load"
            status = "alert"
            anomaly_score = max(anomaly_score, 0.7)
        else:
            label = "normal"
            status = "ok"
            anomaly_score = min(anomaly_score, 0.49)

        prediction_score = round(anomaly_score if status == "alert" else max(0.5, 1.0 - anomaly_score), 4)
        details = (
            f"z_power={z_power:.4f} z_ratio={z_ratio:.4f} "
            f"plug_mean_1m={plug_mean_1m:.4f} "
            f"plug_mean_5m={(plug_mean_5m or plug_mean_1m):.4f} "
            f"ratio={(ratio if ratio is not None else 0.0):.4f}"
        )

        self._update_baselines(plug_mean_1m, ratio)
        return [
            self._result(
                model_input,
                inference_status=status,
                prediction_label=label,
                prediction_score=prediction_score,
                anomaly_score=round(anomaly_score, 4),
                load_score=round(load_score, 4),
                details=details,
            )
        ]

    def _update_baselines(self, plug_mean_1m: float, ratio: float | None) -> None:
        self.power_baseline.update(plug_mean_1m)
        if ratio is not None:
            self.ratio_baseline.update(ratio)

    def _result(
        self,
        model_input: ModelInput,
        inference_status: str,
        prediction_label: str,
        prediction_score: float,
        anomaly_score: float,
        load_score: float,
        details: str,
    ) -> ModelInferenceResult:
        return ModelInferenceResult(
            timestamp=model_input.timestamp,
            model_id=self.MODEL_ID,
            model_version=str(self.config["model_version"]),
            backend=str(self.config["backend"]),
            input_contract_version=model_input.contract_version,
            output_contract_version=MODEL_OUTPUT_CONTRACT_VERSION,
            inference_status=inference_status,
            prediction_label=prediction_label,
            prediction_score=prediction_score,
            anomaly_score=anomaly_score,
            load_score=load_score,
            details=details,
        )


class EdgeShortTermLoadForecastModel(EdgeModel):
    MODEL_ID = "edge_short_term_load_forecast"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config["edge_models"]["short_term_load_forecast"]
        self.model_path = Path(self.config.get("model_path", "models/edge_load_forecast_ridge.json"))
        self.artifact: dict[str, Any] | None = None

    def infer(
        self,
        model_input: ModelInput,
        context: PipelineContext,
    ) -> list[ModelInferenceResult]:
        if model_input.forecast_feature_record is None:
            return [
                self._result(
                    model_input,
                    status="insufficient_data",
                    label="forecast_unavailable",
                    predicted_w=None,
                    details="missing cloud forecast feature record",
                )
            ]

        artifact = self._load_artifact()
        if artifact is None:
            return [
                self._result(
                    model_input,
                    status="unavailable",
                    label="forecast_model_missing",
                    predicted_w=None,
                    details=f"model_path={self.model_path}",
                )
            ]

        horizon_minutes = int(artifact.get("horizon_minutes", self.config.get("horizon_minutes", 1)))
        input_columns = artifact["input_columns"]
        feature_values = _feature_values_for_horizon(
            model_input.forecast_feature_record.values,
            model_input.timestamp,
            horizon_minutes,
        )
        vector = [_as_float(feature_values.get(column)) or 0.0 for column in input_columns]
        prediction = _ridge_predict(vector, artifact)
        target_timestamp = model_input.timestamp + timedelta(minutes=horizon_minutes)
        details = json.dumps(
            {
                "feature_set_id": artifact.get("feature_set_id"),
                "horizon_minutes": horizon_minutes,
                "target_timestamp": target_timestamp.isoformat(),
                "predicted_household_power_w": round(prediction, 4),
                "training_metrics": artifact.get("metrics", {}),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        return [
            self._result(
                model_input,
                status="ok",
                label="short_term_load_forecast",
                predicted_w=round(prediction, 4),
                details=details,
            )
        ]

    def _load_artifact(self) -> dict[str, Any] | None:
        if self.artifact is not None:
            return self.artifact
        if not self.model_path.exists():
            return None
        self.artifact = json.loads(self.model_path.read_text(encoding="utf-8"))
        return self.artifact

    def _result(
        self,
        model_input: ModelInput,
        status: str,
        label: str,
        predicted_w: float | None,
        details: str,
    ) -> ModelInferenceResult:
        return ModelInferenceResult(
            timestamp=model_input.timestamp,
            model_id=self.MODEL_ID,
            model_version=str(self.config.get("model_version", "1.0")),
            backend=str(self.config.get("backend", "ridge_regression")),
            input_contract_version=model_input.contract_version,
            output_contract_version=MODEL_OUTPUT_CONTRACT_VERSION,
            inference_status=status,
            prediction_label=label,
            prediction_score=predicted_w,
            anomaly_score=None,
            load_score=predicted_w,
            details=details,
        )


def build_model_input(feature_window, forecast_feature_record=None) -> ModelInput:
    return ModelInput(
        timestamp=feature_window.timestamp,
        contract_version=MODEL_INPUT_CONTRACT_VERSION,
        feature_window=feature_window,
        forecast_feature_record=forecast_feature_record,
    )


def build_default_edge_models(config: dict[str, Any]) -> list[EdgeModel]:
    models: list[EdgeModel] = []
    model_config = config.get("edge_models", {}).get("energy_anomaly_monitor", {})
    if model_config.get("enabled", True):
        models.append(EnergyAnomalyMonitorModel(config))
    forecast_config = config.get("edge_models", {}).get("short_term_load_forecast", {})
    if forecast_config.get("enabled", False):
        models.append(EdgeShortTermLoadForecastModel(config))
    return models


def _ridge_predict(vector: list[float], artifact: dict[str, Any]) -> float:
    means = artifact["feature_means"]
    scales = artifact["feature_scales"]
    coefficients = artifact["coefficients"]
    total = float(artifact["intercept"])
    for value, mean, scale, coefficient in zip(vector, means, scales, coefficients):
        standardized = (float(value) - float(mean)) / max(float(scale), 1e-12)
        total += standardized * float(coefficient)
    return total


def _feature_values_for_horizon(
    values: dict[str, Any],
    timestamp,
    horizon_minutes: int,
) -> dict[str, Any]:
    feature_values = dict(values)
    target_timestamp = timestamp + timedelta(minutes=horizon_minutes)
    target_minute = target_timestamp.hour * 60 + target_timestamp.minute
    feature_values.update(
        {
            "horizon_minutes": horizon_minutes,
            "target_minute_of_day": target_minute,
            "target_day_of_week": target_timestamp.weekday(),
            "target_is_weekend": 1 if target_timestamp.weekday() >= 5 else 0,
            "target_minute_sin": math.sin(2.0 * math.pi * target_minute / 1440.0),
            "target_minute_cos": math.cos(2.0 * math.pi * target_minute / 1440.0),
            "target_dow_sin": math.sin(2.0 * math.pi * target_timestamp.weekday() / 7.0),
            "target_dow_cos": math.cos(2.0 * math.pi * target_timestamp.weekday() / 7.0),
        }
    )
    return feature_values


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            return None
    return None


