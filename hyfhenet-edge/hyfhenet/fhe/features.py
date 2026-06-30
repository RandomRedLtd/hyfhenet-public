from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from ..core.models import (
    CloudCohortFeatureRecord,
    CloudForecastFeatureRecord,
    CloudForecastTrainingExample,
    CloudNilmFeatureRecord,
    FeatureWindow,
    LoadEventMarker,
)

FORECAST_SCHEMA_VERSION = "1.0"
FORECAST_MODEL_NAME = "forecast"
FORECAST_FEATURE_SET_ID = "long_term_load_forecast_v1"
DEFAULT_LONG_TERM_FORECAST_HORIZONS_MINUTES = [120, 1440]
NILM_SCHEMA_VERSION = "1.0"
NILM_MODEL_NAME = "nilm"
NILM_FEATURE_SET_ID = "nilm_disaggregation_v1"
COHORT_SCHEMA_VERSION = "1.0"
COHORT_MODEL_NAME = "cohort"
COHORT_FEATURE_SET_ID = "cohort_benchmark_v1"

# Runtime input columns expected by the finished hyfhenet-fhe forecast package.
FORECAST_INPUT_COLUMNS = [
    "horizon_minutes",
    "minute_of_day",
    "day_of_week",
    "is_weekend",
    "minute_sin",
    "minute_cos",
    "dow_sin",
    "dow_cos",
    "target_minute_of_day",
    "target_day_of_week",
    "target_is_weekend",
    "target_minute_sin",
    "target_minute_cos",
    "target_dow_sin",
    "target_dow_cos",
    "linky_household_power_w",
    "linky_household_power_w_missing",
    "linky_household_power_mean_1h_w",
    "linky_household_power_std_1h_w",
    "linky_household_power_delta_1h_w",
    "linky_household_power_mean_24h_w",
    "linky_household_power_std_24h_w",
    "plug_1_power_w",
    "plug_1_power_w_missing",
    "plug_1_power_mean_1h_w",
    "plug_2_power_w",
    "plug_2_power_w_missing",
    "plug_2_power_mean_1h_w",
    "monitored_plug_share",
    "indoor_temperature_c",
    "indoor_humidity_pct",
    "environment_missing_flag",
    "temperature_delta_1h_c",
    "load_event_type_code",
    "load_event_direction_code",
]

# Backward-compatible name used by the local edge forecast trainer.
FORECAST_NUMERIC_FEATURE_COLUMNS = FORECAST_INPUT_COLUMNS

NILM_INPUT_COLUMNS = [
    "minute_of_day",
    "day_of_week",
    "is_weekend",
    "minute_sin",
    "minute_cos",
    "dow_sin",
    "dow_cos",
    "linky_household_power_w",
    "linky_household_power_w_missing",
    "linky_household_power_mean_1h_w",
    "linky_household_power_std_1h_w",
    "linky_household_power_delta_1h_w",
    "linky_household_power_mean_24h_w",
    "linky_household_power_std_24h_w",
    "plug_1_power_w",
    "plug_1_power_w_missing",
    "plug_1_power_mean_1h_w",
    "plug_2_power_w",
    "plug_2_power_w_missing",
    "plug_2_power_mean_1h_w",
    "monitored_plug_share",
    "indoor_temperature_c",
    "indoor_humidity_pct",
    "environment_missing_flag",
    "temperature_delta_1h_c",
    "load_event_type_code",
    "load_event_direction_code",
]

COHORT_INPUT_COLUMNS = [
    "day_index",
    "total_energy_kwh",
    "mean_power_w",
    "peak_power_w",
    "p95_power_w",
    "min_power_w",
    "load_factor",
    "peak_to_mean_ratio",
    "morning_energy_share",
    "evening_energy_share",
    "overnight_energy_share",
    "weekend_flag",
    "temperature_mean_c",
    "temperature_sensitivity_w_per_c",
    "monitored_plug_energy_share",
    "hvac_energy_share",
    "baseload_mean_w",
    "event_rate_per_day",
    "flexibility_score",
]

CLOUD_MODEL_INPUT_COLUMNS = {
    FORECAST_MODEL_NAME: FORECAST_INPUT_COLUMNS,
    NILM_MODEL_NAME: NILM_INPUT_COLUMNS,
    COHORT_MODEL_NAME: COHORT_INPUT_COLUMNS,
}

FORECAST_FEATURE_COLUMNS = [
    "timestamp",
    "schema_version",
    "feature_set_id",
    *FORECAST_INPUT_COLUMNS,
]

NILM_FEATURE_COLUMNS = [
    "timestamp",
    "schema_version",
    "feature_set_id",
    *NILM_INPUT_COLUMNS,
]

COHORT_FEATURE_COLUMNS = [
    "timestamp",
    "schema_version",
    "feature_set_id",
    *COHORT_INPUT_COLUMNS,
]

FORECAST_TRAINING_COLUMNS = [
    "timestamp",
    "schema_version",
    "feature_set_id",
    "target_timestamp",
    "target_household_power_w",
    *FORECAST_INPUT_COLUMNS,
]

EVENT_TYPE_CODES = {
    "none": 0,
    "initial_state": 1,
    "switch_on": 2,
    "switch_off": 3,
    "ramp_up": 4,
    "ramp_down": 5,
}

EVENT_DIRECTION_CODES = {
    "none": 0,
    "increase": 1,
    "decrease": -1,
}


@dataclass(frozen=True)
class PendingForecastLabel:
    feature_record: CloudForecastFeatureRecord
    horizon_minutes: int
    target_due_at: datetime


@dataclass(frozen=True)
class ForecastHistorySample:
    timestamp: datetime
    linky_household_power_w: float | None
    plug_1_power_w: float | None
    plug_2_power_w: float | None
    indoor_temperature_c: float | None


@dataclass(frozen=True)
class CohortHistorySample:
    timestamp: datetime
    household_power_w: float | None
    plug_power_w: float | None
    indoor_temperature_c: float | None
    load_event_count: int


class FheForecastFeaturePreparer:
    """Builds the edge-side plaintext row that is encrypted by the FHE client."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config["cloud_forecast"]
        self.feature_set_id = str(
            self.config.get("feature_set_id", FORECAST_FEATURE_SET_ID)
        )
        self.schema_version = str(self.config.get("schema_version", FORECAST_SCHEMA_VERSION))
        self.horizons_minutes = [
            int(value)
            for value in self.config.get(
                "horizons_minutes",
                DEFAULT_LONG_TERM_FORECAST_HORIZONS_MINUTES,
            )
        ]
        self.runtime_horizon_minutes = int(
            self.config.get("runtime_horizon_minutes") or self.horizons_minutes[0]
        )
        self.pending_labels: deque[PendingForecastLabel] = deque()
        self.history: deque[ForecastHistorySample] = deque(maxlen=10000)

    def build_feature_record(
        self,
        snapshot: dict[str, Any],
        feature_window: FeatureWindow,
        load_event_markers: list[LoadEventMarker],
        horizon_minutes: int | None = None,
    ) -> CloudForecastFeatureRecord:
        timestamp = feature_window.timestamp
        horizon_minutes = int(horizon_minutes or self.runtime_horizon_minutes)
        target_timestamp = timestamp + timedelta(minutes=horizon_minutes)
        target_minute_of_day = target_timestamp.hour * 60 + target_timestamp.minute
        values = _build_stream_feature_values(
            snapshot,
            feature_window,
            load_event_markers,
            self.history,
        )
        values.update(
            {
                "horizon_minutes": horizon_minutes,
                "target_minute_of_day": target_minute_of_day,
                "target_day_of_week": target_timestamp.weekday(),
                "target_is_weekend": 1 if target_timestamp.weekday() >= 5 else 0,
                "target_minute_sin": _sin_cycle(target_minute_of_day, 1440),
                "target_minute_cos": _cos_cycle(target_minute_of_day, 1440),
                "target_dow_sin": _sin_cycle(target_timestamp.weekday(), 7),
                "target_dow_cos": _cos_cycle(target_timestamp.weekday(), 7),
            }
        )
        return CloudForecastFeatureRecord(
            timestamp=timestamp,
            schema_version=self.schema_version,
            feature_set_id=self.feature_set_id,
            values={key: _round_feature(values[key]) for key in FORECAST_INPUT_COLUMNS},
        )

    def observe(
        self,
        snapshot: dict[str, Any],
        feature_window: FeatureWindow,
        load_event_markers: list[LoadEventMarker],
    ) -> tuple[CloudForecastFeatureRecord, list[CloudForecastTrainingExample]]:
        feature_record = self.build_feature_record(snapshot, feature_window, load_event_markers)
        training_examples = self._resolve_due_labels(feature_window.timestamp, snapshot)
        self._enqueue_labels(snapshot, feature_window, load_event_markers)
        return feature_record, training_examples

    def metadata(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "feature_set_id": self.feature_set_id,
            "purpose": "long_term_load_forecasting",
            "model_endpoint": FORECAST_MODEL_NAME,
            "fhe_backend_target": "zama_concrete_ml",
            "edge_boundary": "runtime_input_contract_only",
            "numeric_feature_columns": FORECAST_INPUT_COLUMNS,
            "horizons_minutes": self.horizons_minutes,
            "runtime_horizon_minutes": self.runtime_horizon_minutes,
            "missing_value_policy": "numeric_missing_values_imputed_to_zero_with_missing_indicator",
            "categorical_policy": "categorical_values_encoded_as_fixed_integer_codes",
            "source": "edge_gateway_stream",
            "notes": (
                "The edge builds only the plaintext feature row, encrypts it with the "
                "FHE client, and sends it to the remote forecast endpoint. Server "
                "models, training datasets, and FHE compilation artifacts are cloud-side."
            ),
        }

    def _enqueue_labels(
        self,
        snapshot: dict[str, Any],
        feature_window: FeatureWindow,
        load_event_markers: list[LoadEventMarker],
    ) -> None:
        for horizon_minutes in self.horizons_minutes:
            feature_record = self.build_feature_record(
                snapshot,
                feature_window,
                load_event_markers,
                horizon_minutes=horizon_minutes,
            )
            self.pending_labels.append(
                PendingForecastLabel(
                    feature_record=feature_record,
                    horizon_minutes=horizon_minutes,
                    target_due_at=feature_record.timestamp + timedelta(minutes=horizon_minutes),
                )
            )

    def _resolve_due_labels(
        self,
        current_timestamp: datetime,
        snapshot: dict[str, Any],
    ) -> list[CloudForecastTrainingExample]:
        target_power = _as_float(snapshot.get("household_power_w"))
        if target_power is None:
            return []

        examples: list[CloudForecastTrainingExample] = []
        while self.pending_labels and self.pending_labels[0].target_due_at <= current_timestamp:
            pending = self.pending_labels.popleft()
            examples.append(
                CloudForecastTrainingExample(
                    timestamp=pending.feature_record.timestamp,
                    schema_version=pending.feature_record.schema_version,
                    feature_set_id=pending.feature_record.feature_set_id,
                    horizon_minutes=pending.horizon_minutes,
                    target_timestamp=pending.target_due_at,
                    target_household_power_w=round(target_power, 4),
                    values=pending.feature_record.values,
                )
            )
        return examples


CloudLoadForecastFeaturePreparer = FheForecastFeaturePreparer


class FheNilmFeaturePreparer:
    """Builds the NILM disaggregation input row used by the cloud/FHE NILM model."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config.get("cloud_nilm", {})
        self.feature_set_id = str(
            self.config.get("feature_set_id", NILM_FEATURE_SET_ID)
        )
        self.schema_version = str(self.config.get("schema_version", NILM_SCHEMA_VERSION))
        self.history: deque[ForecastHistorySample] = deque(maxlen=10000)

    def build_feature_record(
        self,
        snapshot: dict[str, Any],
        feature_window: FeatureWindow,
        load_event_markers: list[LoadEventMarker],
    ) -> CloudNilmFeatureRecord:
        values = _build_stream_feature_values(
            snapshot,
            feature_window,
            load_event_markers,
            self.history,
        )
        return CloudNilmFeatureRecord(
            timestamp=feature_window.timestamp,
            schema_version=self.schema_version,
            feature_set_id=self.feature_set_id,
            values={key: _round_feature(values[key]) for key in NILM_INPUT_COLUMNS},
        )

    def metadata(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "feature_set_id": self.feature_set_id,
            "purpose": "nilm_disaggregation",
            "model_endpoint": NILM_MODEL_NAME,
            "fhe_backend_target": "zama_concrete_ml",
            "edge_boundary": "runtime_input_contract_only",
            "numeric_feature_columns": NILM_INPUT_COLUMNS,
            "target_columns": [
                "target_plug_1_power_w",
                "target_plug_2_power_w",
                "target_hvac_power_w",
                "target_baseload_power_w",
                "target_other_power_w",
            ],
            "source": "edge_gateway_stream",
        }


class FheCohortFeaturePreparer:
    """Builds the cohort benchmarking input row from edge-side daily aggregates."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config.get("cloud_cohort", {})
        self.pipeline_config = config.get("pipeline", {})
        self.feature_set_id = str(
            self.config.get("feature_set_id", COHORT_FEATURE_SET_ID)
        )
        self.schema_version = str(self.config.get("schema_version", COHORT_SCHEMA_VERSION))
        self.history: deque[CohortHistorySample] = deque(maxlen=20000)

    def build_feature_record(
        self,
        snapshot: dict[str, Any],
        feature_window: FeatureWindow,
        load_event_markers: list[LoadEventMarker],
    ) -> CloudCohortFeatureRecord:
        timestamp = feature_window.timestamp
        sample = CohortHistorySample(
            timestamp=timestamp,
            household_power_w=_as_float(snapshot.get("household_power_w")),
            plug_power_w=_as_float(snapshot.get("plug_power_w")),
            indoor_temperature_c=_as_float(snapshot.get("indoor_temperature_c")),
            load_event_count=sum(1 for marker in load_event_markers if marker.should_forward),
        )
        if not self.history or self.history[-1].timestamp != timestamp:
            self.history.append(sample)
        day_samples = [
            item for item in self.history if item.timestamp.date() == timestamp.date()
        ] or list(self.history)
        interval_seconds = int(self.pipeline_config.get("interval_seconds", 30))
        values = _build_cohort_values(timestamp, day_samples, interval_seconds)
        return CloudCohortFeatureRecord(
            timestamp=timestamp,
            schema_version=self.schema_version,
            feature_set_id=self.feature_set_id,
            values={key: _round_feature(values[key]) for key in COHORT_INPUT_COLUMNS},
        )

    def metadata(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "feature_set_id": self.feature_set_id,
            "purpose": "cohort_benchmarking",
            "model_endpoint": COHORT_MODEL_NAME,
            "fhe_backend_target": "zama_concrete_ml",
            "edge_boundary": "runtime_input_contract_only",
            "numeric_feature_columns": COHORT_INPUT_COLUMNS,
            "target_columns": ["cohort_label_code"],
            "excluded_columns": ["cohort_label"],
            "source": "edge_gateway_daily_aggregate",
        }


def _value_and_missing(name: str, value: Any) -> dict[str, float | int]:
    converted = _as_float(value)
    if converted is None:
        return {name: 0.0, f"{name}_missing": 1}
    return {name: converted, f"{name}_missing": 0}


def _float_or_zero(value: Any) -> float:
    converted = _as_float(value)
    return converted if converted is not None else 0.0


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _sin_cycle(value: int, period: int) -> float:
    return math.sin(2.0 * math.pi * value / period)


def _cos_cycle(value: int, period: int) -> float:
    return math.cos(2.0 * math.pi * value / period)


def _round_feature(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    return value


def _samples_since(
    samples: deque[ForecastHistorySample],
    timestamp: datetime,
    window: timedelta,
) -> list[ForecastHistorySample]:
    cutoff = timestamp - window
    return [sample for sample in samples if sample.timestamp >= cutoff]


def _sample_values(samples: list[ForecastHistorySample], attr: str) -> list[float]:
    values: list[float] = []
    for sample in samples:
        value = getattr(sample, attr)
        if value is not None:
            values.append(float(value))
    return values


def _mean_sample_attr(samples: list[ForecastHistorySample], attr: str) -> float:
    values = _sample_values(samples, attr)
    return sum(values) / len(values) if values else 0.0


def _std_sample_attr(samples: list[ForecastHistorySample], attr: str) -> float:
    values = _sample_values(samples, attr)
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _delta_sample_attr(samples: list[ForecastHistorySample], attr: str) -> float:
    values = _sample_values(samples, attr)
    if len(values) < 2:
        return 0.0
    return values[-1] - values[0]


def _safe_ratio(numerator: float | None, denominator: float | None) -> float:
    if numerator is None or denominator is None or abs(denominator) <= 1e-9:
        return 0.0
    return float(numerator) / float(denominator)


def _sum_present(values: list[float | None]) -> float | None:
    present = [float(value) for value in values if value is not None]
    if not present:
        return None
    return sum(present)


def _build_stream_feature_values(
    snapshot: dict[str, Any],
    feature_window: FeatureWindow,
    load_event_markers: list[LoadEventMarker],
    history: deque[ForecastHistorySample],
) -> dict[str, Any]:
    timestamp = feature_window.timestamp
    latest_event = load_event_markers[-1] if load_event_markers else None
    minute_of_day = timestamp.hour * 60 + timestamp.minute
    linky_power = _as_float(snapshot.get("household_power_w"))
    linky_missing = 0 if snapshot.get("household_power_source") == "household_meter" else 1
    current_sample = ForecastHistorySample(
        timestamp=timestamp,
        linky_household_power_w=linky_power,
        plug_1_power_w=_as_float(snapshot.get("plug_1_power_w")),
        plug_2_power_w=_as_float(snapshot.get("plug_2_power_w")),
        indoor_temperature_c=_as_float(snapshot.get("indoor_temperature_c")),
    )
    if not history or history[-1].timestamp != timestamp:
        history.append(current_sample)

    hour_samples = _samples_since(history, timestamp, timedelta(hours=1))
    day_samples = _samples_since(history, timestamp, timedelta(hours=24))
    plug_1_power = _as_float(snapshot.get("plug_1_power_w"))
    plug_2_power = _as_float(snapshot.get("plug_2_power_w"))
    monitored_plug_power = _sum_present([plug_1_power, plug_2_power])
    return {
        "minute_of_day": minute_of_day,
        "day_of_week": timestamp.weekday(),
        "is_weekend": 1 if timestamp.weekday() >= 5 else 0,
        "minute_sin": _sin_cycle(minute_of_day, 1440),
        "minute_cos": _cos_cycle(minute_of_day, 1440),
        "dow_sin": _sin_cycle(timestamp.weekday(), 7),
        "dow_cos": _cos_cycle(timestamp.weekday(), 7),
        "linky_household_power_w": _float_or_zero(linky_power),
        "linky_household_power_w_missing": linky_missing,
        "linky_household_power_mean_1h_w": _mean_sample_attr(hour_samples, "linky_household_power_w"),
        "linky_household_power_std_1h_w": _std_sample_attr(hour_samples, "linky_household_power_w"),
        "linky_household_power_delta_1h_w": _delta_sample_attr(hour_samples, "linky_household_power_w"),
        "linky_household_power_mean_24h_w": _mean_sample_attr(day_samples, "linky_household_power_w"),
        "linky_household_power_std_24h_w": _std_sample_attr(day_samples, "linky_household_power_w"),
        **_value_and_missing("plug_1_power_w", snapshot.get("plug_1_power_w")),
        "plug_1_power_mean_1h_w": _mean_sample_attr(hour_samples, "plug_1_power_w"),
        **_value_and_missing("plug_2_power_w", snapshot.get("plug_2_power_w")),
        "plug_2_power_mean_1h_w": _mean_sample_attr(hour_samples, "plug_2_power_w"),
        "monitored_plug_share": _safe_ratio(monitored_plug_power, linky_power),
        "indoor_temperature_c": _float_or_zero(snapshot.get("indoor_temperature_c")),
        "indoor_humidity_pct": _float_or_zero(snapshot.get("indoor_humidity_pct")),
        "environment_missing_flag": int(feature_window.get("environment_stale_flag") or 0),
        "temperature_delta_1h_c": _delta_sample_attr(hour_samples, "indoor_temperature_c"),
        "load_event_type_code": EVENT_TYPE_CODES.get(
            latest_event.event_type if latest_event else "none",
            0,
        ),
        "load_event_direction_code": EVENT_DIRECTION_CODES.get(
            latest_event.direction if latest_event else "none",
            0,
        ),
    }


def _build_cohort_values(
    timestamp: datetime,
    samples: list[CohortHistorySample],
    interval_seconds: int,
) -> dict[str, Any]:
    power_values = [value for value in (sample.household_power_w for sample in samples) if value is not None]
    plug_values = [value for value in (sample.plug_power_w for sample in samples) if value is not None]
    temp_values = [value for value in (sample.indoor_temperature_c for sample in samples) if value is not None]
    mean_power = _mean_values(power_values)
    peak_power = max(power_values) if power_values else 0.0
    min_power = min(power_values) if power_values else 0.0
    p95_power = _percentile(power_values, 0.95)
    total_energy_kwh = _energy_kwh(power_values, interval_seconds)
    plug_energy_kwh = _energy_kwh(plug_values, interval_seconds)
    load_factor = _safe_ratio(mean_power, peak_power)
    monitored_share = _safe_ratio(plug_energy_kwh, total_energy_kwh)
    baseload_mean = min_power
    baseload_share = _safe_ratio(baseload_mean, mean_power)
    event_count = sum(sample.load_event_count for sample in samples)
    elapsed_days = max(
        ((samples[-1].timestamp - samples[0].timestamp).total_seconds() + interval_seconds)
        / 86400.0,
        interval_seconds / 86400.0,
    ) if samples else interval_seconds / 86400.0
    hvac_share = max(0.0, min(1.0, 1.0 - monitored_share - baseload_share))
    return {
        "day_index": timestamp.timetuple().tm_yday - 1,
        "total_energy_kwh": total_energy_kwh,
        "mean_power_w": mean_power,
        "peak_power_w": peak_power,
        "p95_power_w": p95_power,
        "min_power_w": min_power,
        "load_factor": load_factor,
        "peak_to_mean_ratio": _safe_ratio(peak_power, mean_power),
        "morning_energy_share": _energy_share_by_hour(samples, interval_seconds, 6, 12),
        "evening_energy_share": _energy_share_by_hour(samples, interval_seconds, 17, 22),
        "overnight_energy_share": _overnight_energy_share(samples, interval_seconds),
        "weekend_flag": 1 if timestamp.weekday() >= 5 else 0,
        "temperature_mean_c": _mean_values(temp_values),
        "temperature_sensitivity_w_per_c": _slope(temp_values, power_values),
        "monitored_plug_energy_share": monitored_share,
        "hvac_energy_share": hvac_share,
        "baseload_mean_w": baseload_mean,
        "event_rate_per_day": event_count / elapsed_days,
        "flexibility_score": max(0.0, min(1.0, (1.0 - load_factor) * 0.6 + monitored_share * 0.4)),
    }


def _mean_values(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def _energy_kwh(values: list[float], interval_seconds: int) -> float:
    return sum(values) * max(interval_seconds, 1) / 3600.0 / 1000.0


def _energy_share_by_hour(
    samples: list[CohortHistorySample],
    interval_seconds: int,
    start_hour: int,
    end_hour: int,
) -> float:
    total = _energy_kwh(
        [sample.household_power_w for sample in samples if sample.household_power_w is not None],
        interval_seconds,
    )
    if total <= 0:
        return 0.0
    bucket = _energy_kwh(
        [
            sample.household_power_w
            for sample in samples
            if sample.household_power_w is not None
            and start_hour <= sample.timestamp.hour < end_hour
        ],
        interval_seconds,
    )
    return bucket / total


def _overnight_energy_share(
    samples: list[CohortHistorySample],
    interval_seconds: int,
) -> float:
    total = _energy_kwh(
        [sample.household_power_w for sample in samples if sample.household_power_w is not None],
        interval_seconds,
    )
    if total <= 0:
        return 0.0
    bucket = _energy_kwh(
        [
            sample.household_power_w
            for sample in samples
            if sample.household_power_w is not None
            and (sample.timestamp.hour < 6 or sample.timestamp.hour >= 22)
        ],
        interval_seconds,
    )
    return bucket / total


def _slope(x_values: list[float], y_values: list[float]) -> float:
    count = min(len(x_values), len(y_values))
    if count < 2:
        return 0.0
    x = x_values[:count]
    y = y_values[:count]
    x_mean = _mean_values(x)
    y_mean = _mean_values(y)
    variance = sum((value - x_mean) ** 2 for value in x)
    if variance <= 1e-9:
        return 0.0
    covariance = sum((x_value - x_mean) * (y_value - y_mean) for x_value, y_value in zip(x, y))
    return covariance / variance
