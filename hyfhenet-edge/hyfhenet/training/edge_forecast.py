from __future__ import annotations

import csv
import json
import math
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..fhe.features import FORECAST_NUMERIC_FEATURE_COLUMNS
from ..runtime.stream import (
    build_gateway_stream_context,
    build_gateway_stream_pipeline_for_context,
)
from .linear import fit_ridge_regression, predict_ridge_regression

FEATURE_DESCRIPTIONS = {
    "horizon_minutes": "Forecast horizon encoded in minutes.",
    "snapshot_interval_s": "Aligned snapshot interval in seconds.",
    "minute_of_day": "Minute index from midnight at feature timestamp.",
    "day_of_week": "Python weekday number, Monday=0.",
    "is_weekend": "Binary weekend flag.",
    "minute_sin": "Sine encoding of minute-of-day.",
    "minute_cos": "Cosine encoding of minute-of-day.",
    "dow_sin": "Sine encoding of day-of-week.",
    "dow_cos": "Cosine encoding of day-of-week.",
    "target_minute_of_day": "Minute index from midnight at target timestamp.",
    "target_day_of_week": "Python weekday number at target timestamp, Monday=0.",
    "target_is_weekend": "Binary weekend flag at target timestamp.",
    "target_minute_sin": "Sine encoding of target minute-of-day.",
    "target_minute_cos": "Cosine encoding of target minute-of-day.",
    "target_dow_sin": "Sine encoding of target day-of-week.",
    "target_dow_cos": "Cosine encoding of target day-of-week.",
    "linky_household_power_w": "Current household active power, from Linky/TIC when present or the edge plug-sum fallback.",
    "linky_household_power_w_missing": "Missing indicator for real household meter power; plug-sum fallback sets this to 1.",
    "linky_household_power_mean_1h_w": "One-hour mean household active power.",
    "linky_household_power_std_1h_w": "One-hour household active power standard deviation.",
    "linky_household_power_delta_1h_w": "Change in household active power over the observed one-hour window.",
    "linky_household_power_mean_24h_w": "Twenty-four-hour mean household active power over observed history.",
    "linky_household_power_std_24h_w": "Twenty-four-hour household active power standard deviation over observed history.",
    "household_power_w": "Current monitored household active power, derived as the sum of the two configured smart plugs.",
    "household_power_w_missing": "Missing indicator for current household power.",
    "household_power_mean_1m_w": "Rolling 1-minute mean household power.",
    "household_power_mean_5m_w": "Rolling 5-minute mean household power.",
    "household_power_std_1m_w": "Rolling 1-minute household power standard deviation.",
    "household_power_std_5m_w": "Rolling 5-minute household power standard deviation.",
    "household_power_delta_1m_w": "Change in household power over the 1-minute window.",
    "household_power_delta_5m_w": "Change in household power over the 5-minute window.",
    "plug_1_power_w": "Current active power from configured smart plug 1.",
    "plug_1_power_w_missing": "Missing indicator for smart plug 1 power.",
    "plug_1_power_mean_1h_w": "One-hour mean active power from configured smart plug 1.",
    "plug_1_power_mean_1m_w": "Rolling 1-minute mean smart plug 1 power.",
    "plug_1_power_mean_5m_w": "Rolling 5-minute mean smart plug 1 power.",
    "plug_1_power_delta_1m_w": "Change in smart plug 1 power over the 1-minute window.",
    "plug_1_power_delta_5m_w": "Change in smart plug 1 power over the 5-minute window.",
    "plug_2_power_w": "Current active power from configured smart plug 2.",
    "plug_2_power_w_missing": "Missing indicator for smart plug 2 power.",
    "plug_2_power_mean_1h_w": "One-hour mean active power from configured smart plug 2.",
    "plug_2_power_mean_1m_w": "Rolling 1-minute mean smart plug 2 power.",
    "plug_2_power_mean_5m_w": "Rolling 5-minute mean smart plug 2 power.",
    "plug_2_power_delta_1m_w": "Change in smart plug 2 power over the 1-minute window.",
    "plug_2_power_delta_5m_w": "Change in smart plug 2 power over the 5-minute window.",
    "plug_power_w": "Current total monitored smart-plug power.",
    "plug_power_w_missing": "Missing indicator for total monitored plug power.",
    "plug_power_mean_1m_w": "Rolling 1-minute mean total monitored plug power.",
    "plug_power_mean_5m_w": "Rolling 5-minute mean total monitored plug power.",
    "plug_power_delta_1m_w": "Change in total monitored plug power over the 1-minute window.",
    "plug_power_delta_5m_w": "Change in total monitored plug power over the 5-minute window.",
    "indoor_temperature_c": "Current indoor temperature.",
    "indoor_temperature_c_missing": "Missing indicator for indoor temperature.",
    "indoor_humidity_pct": "Current indoor humidity.",
    "monitored_plug_share": "Share of current household power represented by the configured smart plugs.",
    "environment_missing_flag": "Binary flag showing environment data is stale or unavailable.",
    "temperature_delta_1h_c": "Change in indoor temperature over the observed one-hour window.",
    "indoor_humidity_pct_missing": "Missing indicator for indoor humidity.",
    "device_to_household_ratio": "Current monitored plug power divided by derived household power; normally 1.0 for the current sensor-only setup.",
    "device_to_household_ratio_missing": "Missing indicator for device-to-household ratio.",
    "plug_stale_flag": "Binary flag showing plug data is stale or unavailable.",
    "environment_stale_flag": "Binary flag showing environment data is stale or unavailable.",
    "household_stale_flag": "Binary flag showing derived household power is stale or unavailable.",
    "load_event_count": "Number of load event markers emitted for this feature tick.",
    "load_event_type_code": "Integer code for the latest load event type.",
    "load_event_direction_code": "Integer code for latest load direction.",
}


def train_edge_short_term_load_forecast(
        input_path: Path | str,
        config_path: Path | str | None,
        output_model_path: Path | str,
        output_dataset_path: Path | str | None = None,
        output_report_path: Path | str | None = None,
        horizon_minutes: int = 1,
        alpha: float = 10.0,
        train_fraction: float = 0.7,
) -> dict[str, Any]:
    output_model_path = Path(output_model_path)
    output_dataset_path = Path(output_dataset_path) if output_dataset_path is not None else None
    output_report_path = Path(output_report_path) if output_report_path is not None else None
    rows = _build_training_rows(input_path, config_path, horizon_minutes)
    if len(rows) < 3:
        raise ValueError(
            "Not enough supervised forecast examples. "
            "Use a shorter horizon or a longer replay capture."
        )

    rows.sort(key=lambda row: row["timestamp"])
    split_index = int(len(rows) * train_fraction)
    split_index = min(max(split_index, 2), len(rows) - 1)
    train_rows = rows[:split_index]
    test_rows = rows[split_index:]

    model = fit_ridge_regression(
        x=[_feature_vector(row) for row in train_rows],
        y=[float(row["target_household_power_w"]) for row in train_rows],
        alpha=alpha,
        round_digits=10,
    )
    predictions = [predict_ridge_regression(_feature_vector(row), model) for row in test_rows]
    targets = [float(row["target_household_power_w"]) for row in test_rows]
    test_metrics = _evaluate(predictions, targets)
    train_predictions = [predict_ridge_regression(_feature_vector(row), model) for row in train_rows]
    train_targets = [float(row["target_household_power_w"]) for row in train_rows]
    train_metrics = _evaluate(train_predictions, train_targets)
    dataset_rows = _build_dataset_rows(rows, model, split_index)
    all_metrics = _evaluate(
        [float(row["predicted_household_power_w"]) for row in dataset_rows],
        [float(row["target_household_power_w"]) for row in dataset_rows],
    )

    artifact = {
        "model_id": "edge_short_term_load_forecast",
        "model_version": "1.0",
        "backend": "ridge_regression",
        "feature_set_id": rows[0]["feature_set_id"],
        "schema_version": rows[0]["schema_version"],
        "horizon_minutes": horizon_minutes,
        "target_column": "target_household_power_w",
        "input_columns": FORECAST_NUMERIC_FEATURE_COLUMNS,
        "alpha": alpha,
        "intercept": model["intercept"],
        "coefficients": model["coefficients"],
        "feature_means": model["feature_means"],
        "feature_scales": model["feature_scales"],
        "metrics": test_metrics,
        "metrics_by_split": {
            "train": train_metrics,
            "test": test_metrics,
            "all": all_metrics,
        },
        "training_summary": {
            "source": str(input_path),
            "example_count": len(rows),
            "train_count": len(train_rows),
            "test_count": len(test_rows),
            "train_fraction": train_fraction,
            "household_target_source": "sum_of_configured_smart_plugs",
            "dataset_path": str(output_dataset_path) if output_dataset_path else None,
            "report_path": str(output_report_path) if output_report_path else None,
            "model_path": str(output_model_path),
        },
        "trained_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
    }

    output_model_path.parent.mkdir(parents=True, exist_ok=True)
    output_model_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    if output_dataset_path is not None:
        _write_dataset(Path(output_dataset_path), dataset_rows)
    if output_report_path is not None:
        _write_report(Path(output_report_path), artifact)
    return artifact


def _build_training_rows(
        input_path: Path | str,
        config_path: Path | str | None,
        horizon_minutes: int,
) -> list[dict[str, str]]:
    with tempfile.TemporaryDirectory() as tmpdir:
        context = build_gateway_stream_context(
            input_path=input_path,
            output_dir=Path(tmpdir),
            config_path=config_path,
            interval_seconds=30,
            zigbee_source="replay",
            follow_event_timing=False,
            console_log_level="none",
        )
        context.config["cloud_forecast"]["horizons_minutes"] = [horizon_minutes]
        context.config.setdefault("fhe_cloud", {})["enabled"] = False
        context.config["edge_models"]["short_term_load_forecast"]["enabled"] = False
        build_gateway_stream_pipeline_for_context(context, write_artifacts=True).run(context)
        path = Path(tmpdir) / "cloud_forecast_training_examples.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))


def _build_dataset_rows(
        rows: list[dict[str, str]],
        model: dict[str, Any],
        split_index: int,
) -> list[dict[str, Any]]:
    dataset_rows = []
    for index, row in enumerate(rows):
        target = float(row["target_household_power_w"])
        prediction = predict_ridge_regression(_feature_vector(row), model)
        error = prediction - target
        dataset_rows.append(
            {
                "split": "train" if index < split_index else "test",
                **row,
                "predicted_household_power_w": round(prediction, 4),
                "forecast_error_w": round(error, 4),
                "absolute_error_w": round(abs(error), 4),
            }
        )
    return dataset_rows


def _write_dataset(path: Path, rows: list[dict[str, Any]]) -> None:
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
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_report(path: Path, artifact: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = artifact["training_summary"]
    metrics = artifact["metrics_by_split"]
    lines = [
        "# Edge Short-Term Load Forecast Training Report",
        "",
        "## Model",
        "",
        f"- Model id: `{artifact['model_id']}`",
        f"- Backend: `{artifact['backend']}`",
        f"- Horizon: `{artifact['horizon_minutes']}` minute(s)",
        f"- Regularization alpha: `{artifact['alpha']}`",
        f"- Feature set: `{artifact['feature_set_id']}`",
        f"- Input feature count: `{len(artifact['input_columns'])}`",
        f"- Trained at: `{artifact['trained_at']}`",
        "",
        "## Data",
        "",
        f"- Source replay: `{summary['source']}`",
        f"- Supervised examples: `{summary['example_count']}`",
        f"- Train examples: `{summary['train_count']}`",
        f"- Test examples: `{summary['test_count']}`",
        f"- Train fraction: `{summary['train_fraction']}`",
        f"- Household target source: `{summary['household_target_source']}`",
        f"- Saved dataset: `{summary['dataset_path']}`",
        f"- Saved model: `{summary['model_path']}`",
        "",
        "## Target Variable",
        "",
        "- Target column: `target_household_power_w`",
        f"- Target horizon: `{artifact['horizon_minutes']}` minute(s)",
        "- Definition: monitored household active power observed at `target_timestamp = timestamp + horizon_minutes`.",
        "- Stage 2 source: `plug_1_power_w + plug_2_power_w` from the two configured Zigbee smart plugs.",
        "- Rows are emitted only when the future derived household-power target is observed.",
        "",
        "## Evaluation",
        "",
        "| Split | MAE W | RMSE W | R2 |",
        "| --- | ---: | ---: | ---: |",
        f"| Train | {metrics['train']['mae_w']} | {metrics['train']['rmse_w']} | {metrics['train']['r2']} |",
        f"| Test | {metrics['test']['mae_w']} | {metrics['test']['rmse_w']} | {metrics['test']['r2']} |",
        f"| All | {metrics['all']['mae_w']} | {metrics['all']['rmse_w']} | {metrics['all']['r2']} |",
        "",
        "## Features",
        "",
        "| Feature | Description |",
        "| --- | --- |",
        *[
            f"| `{column}` | {FEATURE_DESCRIPTIONS.get(column, 'Model input feature.')} |"
            for column in artifact["input_columns"]
        ],
        "",
        "## Notes",
        "",
        "The model is trained once and saved. Runtime streaming loads the saved JSON model and performs inference only.",
        "During a stream, matured prediction-vs-actual rows are written to `edge_results.jsonl` with `edge_record_type=edge_forecast_evaluation`.",
        "The current Stage 2 capture is short, so the 1-minute horizon is used for local validation. Longer horizons need longer live data.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _feature_vector(row: dict[str, str]) -> list[float]:
    return [_as_float(row.get(column)) for column in FORECAST_NUMERIC_FEATURE_COLUMNS]


def _as_float(value: str | None) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _evaluate(predictions: list[float], targets: list[float]) -> dict[str, float]:
    errors = [prediction - target for prediction, target in zip(predictions, targets)]
    mae = sum(abs(error) for error in errors) / len(errors)
    rmse = math.sqrt(sum(error * error for error in errors) / len(errors))
    target_mean = sum(targets) / len(targets)
    total_sum_squares = sum((target - target_mean) ** 2 for target in targets)
    residual_sum_squares = sum(error * error for error in errors)
    r2 = 0.0 if total_sum_squares <= 1e-12 else 1.0 - (residual_sum_squares / total_sum_squares)
    return {
        "mae_w": round(mae, 4),
        "rmse_w": round(rmse, 4),
        "r2": round(r2, 4),
    }
