from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any

from .stream import run_gateway_stream_replay


BENCHMARK_COLUMNS = [
    "run_index",
    "output_dir",
    "elapsed_seconds",
    "raw_event_count",
    "snapshot_count",
    "cloud_forecast_feature_count",
    "model_input_count",
    "model_result_count",
    "edge_forecast_evaluation_count",
    "raw_events_per_wall_second",
    "snapshots_per_wall_second",
    "model_results_per_wall_second",
    "avg_total_tick_ms",
    "p50_total_tick_ms",
    "p95_total_tick_ms",
    "p99_total_tick_ms",
    "avg_event_to_model_ms",
    "p95_event_to_model_ms",
    "avg_edge_local_operations_ms",
    "p95_edge_local_operations_ms",
    "avg_cloud_fhe_operations_ms",
    "p95_cloud_fhe_operations_ms",
    "avg_fhe_cloud_stage_ms",
    "p95_fhe_cloud_stage_ms",
    "cloud_fhe_ok_count",
    "cloud_fhe_unavailable_count",
    "cloud_fhe_ok_rate",
    "forecast_quality_sample_count",
    "forecast_mae_w",
    "forecast_rmse_w",
    "forecast_r2",
    "forecast_p95_absolute_error_w",
]


def run_gateway_replay_benchmark(
    input_path: Path | str,
    output_dir: Path | str,
    config_path: Path | str | None = None,
    runs: int = 4,
    interval_seconds: int | None = 30,
    console_log_level: str = "none",
    fhe_cloud_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    benchmark_dir = Path(output_dir)
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    run_count = max(int(runs), 1)
    for index in range(1, run_count + 1):
        run_output_dir = benchmark_dir / f"run_{index:03d}"
        started = time.monotonic()
        summary = run_gateway_stream_replay(
            input_path=input_path,
            output_dir=run_output_dir,
            config_path=config_path,
            interval_seconds=interval_seconds,
            console_log_level=console_log_level,
            follow_event_timing=False,
            fhe_cloud_overrides=fhe_cloud_overrides,
        )
        elapsed_seconds = round(time.monotonic() - started, 6)
        latency = summary.get("latency_summary") or {}
        performance = summary.get("performance_summary") or {}
        forecast_quality = (summary.get("forecast_quality_summary") or {}).get("overall", {})
        cloud_fhe_counts = _cloud_fhe_status_counts(summary)
        rows.append(
            {
                "run_index": index,
                "output_dir": str(run_output_dir),
                "elapsed_seconds": elapsed_seconds,
                "raw_event_count": summary.get("raw_event_count", 0),
                "snapshot_count": summary.get("snapshot_count", 0),
                "cloud_forecast_feature_count": summary.get(
                    "cloud_forecast_feature_count", 0
                ),
                "model_input_count": summary.get("model_input_count", 0),
                "model_result_count": summary.get("model_result_count", 0),
                "edge_forecast_evaluation_count": summary.get(
                    "edge_forecast_evaluation_count", 0
                ),
                "raw_events_per_wall_second": performance.get("raw_events_per_wall_second"),
                "snapshots_per_wall_second": performance.get("snapshots_per_wall_second"),
                "model_results_per_wall_second": performance.get(
                    "model_results_per_wall_second"
                ),
                "avg_total_tick_ms": latency.get("avg_total_tick_ms"),
                "p50_total_tick_ms": latency.get("p50_total_tick_ms"),
                "p95_total_tick_ms": latency.get("p95_total_tick_ms"),
                "p99_total_tick_ms": latency.get("p99_total_tick_ms"),
                "avg_event_to_model_ms": latency.get("avg_event_to_model_ms"),
                "p95_event_to_model_ms": latency.get("p95_event_to_model_ms"),
                "avg_edge_local_operations_ms": latency.get(
                    "avg_edge_local_operations_ms"
                ),
                "p95_edge_local_operations_ms": latency.get(
                    "p95_edge_local_operations_ms"
                ),
                "avg_cloud_fhe_operations_ms": latency.get(
                    "avg_cloud_fhe_operations_ms"
                ),
                "p95_cloud_fhe_operations_ms": latency.get(
                    "p95_cloud_fhe_operations_ms"
                ),
                "avg_fhe_cloud_stage_ms": latency.get("avg_fhe_cloud_stage_ms"),
                "p95_fhe_cloud_stage_ms": latency.get("p95_fhe_cloud_stage_ms"),
                "cloud_fhe_ok_count": cloud_fhe_counts["ok"],
                "cloud_fhe_unavailable_count": cloud_fhe_counts["unavailable"],
                "cloud_fhe_ok_rate": _ok_rate(cloud_fhe_counts),
                "forecast_quality_sample_count": forecast_quality.get("sample_count", 0),
                "forecast_mae_w": forecast_quality.get("mae_w"),
                "forecast_rmse_w": forecast_quality.get("rmse_w"),
                "forecast_r2": forecast_quality.get("r2"),
                "forecast_p95_absolute_error_w": forecast_quality.get(
                    "p95_absolute_error_w"
                ),
            }
        )

    csv_path = benchmark_dir / "benchmark_summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=BENCHMARK_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    aggregate = _aggregate_rows(rows)
    summary_record = {
        "input_path": str(Path(input_path)),
        "config_path": str(Path(config_path)) if config_path else None,
        "output_dir": str(benchmark_dir.resolve()),
        "run_count": run_count,
        "interval_seconds": interval_seconds,
        "benchmark_csv": str(csv_path),
        "totals": aggregate,
        "runs": rows,
    }
    json_path = benchmark_dir / "benchmark_summary.json"
    json_path.write_text(
        json.dumps(summary_record, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_record["benchmark_json"] = str(json_path)
    return summary_record


def _aggregate_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    totals = {
        "raw_event_count": sum(int(row["raw_event_count"]) for row in rows),
        "snapshot_count": sum(int(row["snapshot_count"]) for row in rows),
        "cloud_forecast_feature_count": sum(
            int(row["cloud_forecast_feature_count"]) for row in rows
        ),
        "model_input_count": sum(int(row["model_input_count"]) for row in rows),
        "model_result_count": sum(int(row["model_result_count"]) for row in rows),
        "edge_forecast_evaluation_count": sum(
            int(row["edge_forecast_evaluation_count"]) for row in rows
        ),
    }
    totals["elapsed_seconds"] = round(
        sum(float(row["elapsed_seconds"]) for row in rows),
        6,
    )
    totals["mean_avg_total_tick_ms"] = _mean_numeric(rows, "avg_total_tick_ms")
    totals["mean_p95_total_tick_ms"] = _mean_numeric(rows, "p95_total_tick_ms")
    totals["mean_avg_edge_local_operations_ms"] = _mean_numeric(
        rows,
        "avg_edge_local_operations_ms",
    )
    totals["mean_p95_edge_local_operations_ms"] = _mean_numeric(
        rows,
        "p95_edge_local_operations_ms",
    )
    totals["mean_avg_cloud_fhe_operations_ms"] = _mean_numeric(
        rows,
        "avg_cloud_fhe_operations_ms",
    )
    totals["mean_p95_cloud_fhe_operations_ms"] = _mean_numeric(
        rows,
        "p95_cloud_fhe_operations_ms",
    )
    totals["mean_avg_fhe_cloud_stage_ms"] = _mean_numeric(
        rows,
        "avg_fhe_cloud_stage_ms",
    )
    totals["mean_raw_events_per_wall_second"] = _mean_numeric(
        rows,
        "raw_events_per_wall_second",
    )
    totals["mean_forecast_mae_w"] = _mean_numeric(rows, "forecast_mae_w")
    totals["mean_forecast_rmse_w"] = _mean_numeric(rows, "forecast_rmse_w")
    totals["mean_forecast_r2"] = _mean_numeric(rows, "forecast_r2")
    totals["cloud_fhe_ok_count"] = sum(int(row["cloud_fhe_ok_count"]) for row in rows)
    totals["cloud_fhe_unavailable_count"] = sum(
        int(row["cloud_fhe_unavailable_count"]) for row in rows
    )
    totals["cloud_fhe_ok_rate"] = _ok_rate(totals)
    return totals


def _cloud_fhe_status_counts(summary: dict[str, Any]) -> dict[str, int]:
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


def _ok_rate(counts: dict[str, Any]) -> float | None:
    ok_count = int(counts.get("ok", counts.get("cloud_fhe_ok_count", 0)) or 0)
    unavailable_count = int(
        counts.get("unavailable", counts.get("cloud_fhe_unavailable_count", 0)) or 0
    )
    total = ok_count + unavailable_count
    return round(ok_count / total, 6) if total else None


def _mean_numeric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 6)
