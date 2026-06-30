from __future__ import annotations

import json
import math
from collections import deque
from datetime import datetime
from typing import Any

from ..core.models import FeatureWindow, NormalizedEvent

FEATURE_SCHEMA_VERSION = "1.0"


class DataQualityValidator:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config["edge_processing"]

    def validate_event(self, event: NormalizedEvent) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        timestamp = event.timestamp.isoformat()
        if event.field in {"plug_power_w", "household_power_w"} and isinstance(event.value, (int, float)):
            if float(event.value) < 0:
                alerts.append(_alert(timestamp, event.device_id, event.field, "negative_power", event.value))
        if event.field == "indoor_temperature_c" and isinstance(event.value, (int, float)):
            if not (self.config["temperature_min_c"] <= float(event.value) <= self.config["temperature_max_c"]):
                alerts.append(_alert(timestamp, event.device_id, event.field, "temperature_out_of_range", event.value))
        if event.field == "indoor_humidity_pct" and isinstance(event.value, (int, float)):
            if not (self.config["humidity_min_pct"] <= float(event.value) <= self.config["humidity_max_pct"]):
                alerts.append(_alert(timestamp, event.device_id, event.field, "humidity_out_of_range", event.value))
        if event.field in {"plug_energy_wh", "household_energy_wh"} and isinstance(event.value, (int, float)):
            if float(event.value) < 0:
                alerts.append(_alert(timestamp, event.device_id, event.field, "negative_energy", event.value))
        return alerts

    def validate_snapshot(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        timestamp = snapshot["timestamp"]
        plug_age = snapshot.get("plug_data_age_s")
        if isinstance(plug_age, int) and plug_age > int(self.config["max_plug_staleness_seconds"]):
            alerts.append(_alert(timestamp, "gateway", "plug_data_age_s", "plug_data_stale", plug_age))
        env_age = snapshot.get("environment_data_age_s")
        if isinstance(env_age, int) and env_age > int(self.config["max_environment_staleness_seconds"]):
            alerts.append(_alert(timestamp, "gateway", "environment_data_age_s", "environment_data_stale", env_age))
        household_age = snapshot.get("household_data_age_s")
        if isinstance(household_age, int) and household_age > int(self.config["max_household_staleness_seconds"]):
            alerts.append(_alert(timestamp, "gateway", "household_data_age_s", "household_data_stale", household_age))
        return alerts


class RollingFeatureEngine:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config["edge_processing"]
        self.windows = [int(window) for window in self.config["feature_windows_seconds"]]
        self.max_window = max(self.windows, default=0)
        self.history: deque[tuple[datetime, dict[str, Any]]] = deque()

    def build_feature_window(self, snapshot: dict[str, Any]) -> FeatureWindow:
        timestamp = datetime.fromisoformat(snapshot["timestamp"])
        self.history.append((timestamp, dict(snapshot)))
        cutoff = timestamp.timestamp() - self.max_window
        while self.history and self.history[0][0].timestamp() < cutoff:
            self.history.popleft()

        values: dict[str, Any] = {
            "plug_1_power_latest_w": snapshot.get("plug_1_power_w"),
            "plug_2_power_latest_w": snapshot.get("plug_2_power_w"),
            "plug_power_latest_w": snapshot.get("plug_power_w"),
            "household_power_latest_w": snapshot.get("household_power_w"),
            "indoor_temperature_latest_c": snapshot.get("indoor_temperature_c"),
            "indoor_humidity_latest_pct": snapshot.get("indoor_humidity_pct"),
            "plug_stale_flag": _stale_flag(
                snapshot.get("plug_data_age_s"),
                self.config["max_plug_staleness_seconds"],
            ),
            "environment_stale_flag": _stale_flag(
                snapshot.get("environment_data_age_s"),
                self.config["max_environment_staleness_seconds"],
            ),
            "household_stale_flag": _stale_flag(
                snapshot.get("household_data_age_s"),
                self.config["max_household_staleness_seconds"],
            ),
            "device_to_household_ratio": _safe_ratio(snapshot.get("plug_power_w"), snapshot.get("household_power_w")),
            "plug_load_band": _load_band(snapshot.get("plug_power_w"), self.config),
            "household_load_band": _load_band(snapshot.get("household_power_w"), self.config),
        }

        for window_seconds in self.windows:
            label = _window_label(window_seconds)
            rows = [row for ts, row in self.history if ts.timestamp() >= timestamp.timestamp() - window_seconds]
            plug_1_series = _numeric_series(rows, "plug_1_power_w")
            plug_2_series = _numeric_series(rows, "plug_2_power_w")
            plug_series = _numeric_series(rows, "plug_power_w")
            household_series = _numeric_series(rows, "household_power_w")
            temp_series = _numeric_series(rows, "indoor_temperature_c")
            state_series = [row.get("plug_state") for row in rows if row.get("plug_state") is not None]

            values[f"plug_1_power_mean_{label}_w"] = _mean(plug_1_series)
            values[f"plug_1_power_delta_{label}_w"] = _delta(plug_1_series)
            values[f"plug_2_power_mean_{label}_w"] = _mean(plug_2_series)
            values[f"plug_2_power_delta_{label}_w"] = _delta(plug_2_series)
            values[f"plug_power_mean_{label}_w"] = _mean(plug_series)
            values[f"plug_power_min_{label}_w"] = min(plug_series) if plug_series else None
            values[f"plug_power_max_{label}_w"] = max(plug_series) if plug_series else None
            values[f"plug_power_std_{label}_w"] = _std(plug_series)
            values[f"plug_power_delta_{label}_w"] = _delta(plug_series)
            values[f"household_power_mean_{label}_w"] = _mean(household_series)
            values[f"household_power_min_{label}_w"] = min(household_series) if household_series else None
            values[f"household_power_max_{label}_w"] = max(household_series) if household_series else None
            values[f"household_power_std_{label}_w"] = _std(household_series)
            values[f"household_power_delta_{label}_w"] = _delta(household_series)
            values[f"temperature_delta_{label}_c"] = _delta(temp_series)
            values[f"plug_on_fraction_{label}"] = _on_fraction(state_series)

        return FeatureWindow(
            timestamp=timestamp,
            schema_version=FEATURE_SCHEMA_VERSION,
            values=values,
        )


def build_feature_columns(config: dict[str, Any]) -> list[str]:
    windows = [int(window) for window in config["edge_processing"]["feature_windows_seconds"]]
    columns = [
        "timestamp",
        "schema_version",
        "plug_1_power_latest_w",
        "plug_2_power_latest_w",
        "plug_power_latest_w",
        "household_power_latest_w",
        "indoor_temperature_latest_c",
        "indoor_humidity_latest_pct",
        "plug_stale_flag",
        "environment_stale_flag",
        "household_stale_flag",
        "device_to_household_ratio",
        "plug_load_band",
        "household_load_band",
    ]
    for window_seconds in windows:
        label = _window_label(window_seconds)
        columns.extend(
            [
                f"plug_power_mean_{label}_w",
                f"plug_1_power_mean_{label}_w",
                f"plug_1_power_delta_{label}_w",
                f"plug_2_power_mean_{label}_w",
                f"plug_2_power_delta_{label}_w",
                f"plug_power_min_{label}_w",
                f"plug_power_max_{label}_w",
                f"plug_power_std_{label}_w",
                f"plug_power_delta_{label}_w",
                f"household_power_mean_{label}_w",
                f"household_power_min_{label}_w",
                f"household_power_max_{label}_w",
                f"household_power_std_{label}_w",
                f"household_power_delta_{label}_w",
                f"temperature_delta_{label}_c",
                f"plug_on_fraction_{label}",
            ]
        )
    return columns


def _alert(timestamp: str, device_id: str, field: str, code: str, value: Any) -> dict[str, Any]:
    return {
        "timestamp": timestamp,
        "device_id": device_id,
        "field": field,
        "code": code,
        "severity": "warning",
        "value": value,
    }


def _window_label(window_seconds: int) -> str:
    if window_seconds % 60 == 0:
        return f"{window_seconds // 60}m"
    return f"{window_seconds}s"


def _numeric_series(rows: list[dict[str, Any]], key: str) -> list[float]:
    values = []
    for row in rows:
        value = row.get(key)
        if isinstance(value, (int, float)):
            values.append(float(value))
    return values


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _std(values: list[float]) -> float | None:
    if len(values) < 2:
        return 0.0 if values else None
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / len(values)
    return round(math.sqrt(variance), 4)


def _delta(values: list[float]) -> float | None:
    if len(values) < 2:
        return 0.0 if values else None
    return round(values[-1] - values[0], 4)


def _on_fraction(states: list[str]) -> float | None:
    if not states:
        return None
    on_count = sum(1 for state in states if state == "ON")
    return round(on_count / len(states), 4)


def _safe_ratio(part, total):
    if not isinstance(part, (int, float)) or not isinstance(total, (int, float)) or float(total) <= 0:
        return None
    return round(float(part) / float(total), 4)


def _stale_flag(age, threshold) -> int:
    if not isinstance(age, int):
        return 1
    return 1 if age > int(threshold) else 0


def _load_band(value, config: dict[str, Any]) -> str | None:
    if not isinstance(value, (int, float)):
        return None
    if value < config["load_band_medium_w"]:
        return "low"
    if value < config["load_band_high_w"]:
        return "medium"
    return "high"


