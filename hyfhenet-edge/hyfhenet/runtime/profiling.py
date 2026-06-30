from __future__ import annotations

import math
from typing import Any

from ..core.models import (
    EdgeForecastEvaluationRecord,
    LatencySample,
    ModelInferenceResult,
    StreamingPipelineRuntime,
)


def build_latency_summary(samples: list[LatencySample]) -> dict[str, Any] | None:
    if not samples:
        return None

    total_tick_ms = _metric_values(samples, "total_tick_ms")
    event_to_model_ms = _metric_values(samples, "event_to_model_ms")
    event_gate_stage_ms = _metric_values(samples, "event_gate_stage_ms")
    forecast_prep_stage_ms = _metric_values(samples, "forecast_prep_stage_ms")
    fhe_cloud_samples = [sample for sample in samples if sample.fhe_cloud_sampled]
    fhe_cloud_stage_ms = _metric_values(fhe_cloud_samples, "fhe_cloud_stage_ms")
    preprocessing_stage_ms = _metric_values(samples, "snapshot_stage_ms")
    feature_stage_ms = _metric_values(samples, "feature_stage_ms")
    model_stage_ms = _metric_values(samples, "model_stage_ms")
    service_stage_ms = _metric_values(samples, "service_stage_ms")

    return {
        "sample_count": len(samples),
        "fhe_cloud_sample_count": len(fhe_cloud_samples),
        **_latency_metric("total_tick_ms", total_tick_ms),
        **_latency_metric("event_to_model_ms", event_to_model_ms),
        **_latency_metric("preprocessing_stage_ms", preprocessing_stage_ms),
        **_latency_metric("feature_stage_ms", feature_stage_ms),
        **_latency_metric("event_gate_stage_ms", event_gate_stage_ms),
        **_latency_metric("forecast_prep_stage_ms", forecast_prep_stage_ms),
        **_latency_metric("fhe_cloud_stage_ms", fhe_cloud_stage_ms),
        **_latency_metric("service_stage_ms", service_stage_ms),
        **_latency_metric("model_stage_ms", model_stage_ms),
    }


def build_performance_summary(
        runtime: StreamingPipelineRuntime,
        wall_clock_elapsed_seconds: float,
) -> dict[str, Any]:
    event_seconds = None
    if runtime.first_event_timestamp is not None and runtime.last_event_timestamp is not None:
        event_seconds = max(
            (runtime.last_event_timestamp - runtime.first_event_timestamp).total_seconds(),
            0.0,
        )
    elapsed = max(float(wall_clock_elapsed_seconds), 1e-9)
    return {
        "wall_clock_elapsed_seconds": _round(float(wall_clock_elapsed_seconds), 6),
        "event_time_coverage_seconds": _round(event_seconds, 6),
        "tick_interval_seconds": runtime.tick_interval_seconds,
        "raw_events_per_wall_second": _round(runtime.raw_event_count / elapsed, 6),
        "normalized_events_per_wall_second": _round(
            runtime.normalized_event_count / elapsed,
            6,
            ),
        "snapshots_per_wall_second": _round(runtime.snapshot_count / elapsed, 6),
        "model_results_per_wall_second": _round(runtime.model_result_count / elapsed, 6),
        "avg_raw_events_per_snapshot": _round(
            _safe_div(runtime.raw_event_count, runtime.snapshot_count),
            6,
        ),
        "avg_model_results_per_snapshot": _round(
            _safe_div(runtime.model_result_count, runtime.snapshot_count),
            6,
        ),
    }


def build_forecast_quality_summary(
        evaluations: list[EdgeForecastEvaluationRecord],
) -> dict[str, Any] | None:
    if not evaluations:
        return None
    groups: dict[str, list[EdgeForecastEvaluationRecord]] = {"overall": evaluations}
    for evaluation in evaluations:
        key = f"{evaluation.model_id}:horizon_{evaluation.horizon_minutes}m"
        groups.setdefault(key, []).append(evaluation)
    return {
        key: _forecast_quality_metrics(group)
        for key, group in sorted(groups.items())
    }


def build_model_result_summary(
        results: list[ModelInferenceResult],
) -> dict[str, Any] | None:
    if not results:
        return None
    by_model: dict[str, dict[str, Any]] = {}
    for result in results:
        model_summary = by_model.setdefault(
            result.model_id,
            {
                "count": 0,
                "status_counts": {},
                "label_counts": {},
            },
        )
        model_summary["count"] += 1
        _increment_count(model_summary["status_counts"], result.inference_status)
        _increment_count(model_summary["label_counts"], result.prediction_label)

    for model_summary in by_model.values():
        count = int(model_summary["count"])
        ok_count = int(model_summary["status_counts"].get("ok", 0))
        model_summary["ok_rate"] = _round(_safe_div(ok_count, count), 6)

    overall_status_counts: dict[str, int] = {}
    for result in results:
        _increment_count(overall_status_counts, result.inference_status)
    return {
        "total_count": len(results),
        "overall_status_counts": overall_status_counts,
        "by_model": by_model,
    }


def _metric_values(samples: list[LatencySample], field_name: str) -> list[float]:
    values: list[float] = []
    for sample in samples:
        value = getattr(sample, field_name)
        if value is not None:
            values.append(float(value))
    return values


def _latency_metric(prefix: str, values: list[float]) -> dict[str, float | None]:
    return {
        f"avg_{prefix}": _round(_average(values)),
        f"p50_{prefix}": _round(_percentile(values, 0.50)),
        f"p95_{prefix}": _round(_percentile(values, 0.95)),
        f"p99_{prefix}": _round(_percentile(values, 0.99)),
        f"max_{prefix}": _round(max(values) if values else None),
    }


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = round((len(ordered) - 1) * fraction)
    return ordered[index]


def _forecast_quality_metrics(
        evaluations: list[EdgeForecastEvaluationRecord],
) -> dict[str, Any]:
    predictions = [float(evaluation.predicted_household_power_w) for evaluation in evaluations]
    targets = [float(evaluation.target_household_power_w) for evaluation in evaluations]
    errors = [prediction - target for prediction, target in zip(predictions, targets)]
    absolute_errors = [abs(error) for error in errors]
    squared_errors = [error * error for error in errors]
    target_mean = _average(targets) or 0.0
    total_sum_squares = sum((target - target_mean) ** 2 for target in targets)
    residual_sum_squares = sum(squared_errors)
    r2 = None
    if total_sum_squares > 1e-12:
        r2 = 1.0 - residual_sum_squares / total_sum_squares
    return {
        "sample_count": len(evaluations),
        "mae_w": _round(_average(absolute_errors), 4),
        "rmse_w": _round(math.sqrt(_average(squared_errors) or 0.0), 4),
        "mean_error_w": _round(_average(errors), 4),
        "p50_absolute_error_w": _round(_percentile(absolute_errors, 0.50), 4),
        "p95_absolute_error_w": _round(_percentile(absolute_errors, 0.95), 4),
        "max_absolute_error_w": _round(max(absolute_errors), 4),
        "r2": _round(r2, 4),
        "target_mean_w": _round(target_mean, 4),
    }


def _increment_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _safe_div(numerator: float, denominator: float) -> float | None:
    if abs(float(denominator)) <= 1e-12:
        return None
    return float(numerator) / float(denominator)


def _round(value: float | None, digits: int = 3) -> float | None:
    if value is None:
        return None
    return round(value, digits)
