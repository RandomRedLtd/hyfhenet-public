from __future__ import annotations

from datetime import datetime
from typing import Any

from ..core.models import NormalizedEvent, RawTelemetryEvent

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

ROLE_FIELD_MAPS: dict[str, dict[str, tuple[str, str | None]]] = {
    "smart_plug": {
        "state": ("plug_state", None),
        "power": ("plug_power_w", "W"),
        "voltage": ("plug_voltage_v", "V"),
        "current": ("plug_current_a", "A"),
        "energy": ("plug_energy_wh", "Wh"),
        "energy_today": ("plug_energy_today_wh", "Wh"),
        "energy_yesterday": ("plug_energy_yesterday_wh", "Wh"),
        "energy_month": ("plug_energy_month_wh", "Wh"),
        "linkquality": ("plug_link_quality", "lqi"),
    },
    "environment_sensor": {
        "temperature": ("indoor_temperature_c", "degC"),
        "humidity": ("indoor_humidity_pct", "pct"),
        "battery": ("sensor_battery_pct", "pct"),
        "linkquality": ("sensor_link_quality", "lqi"),
    },
    "household_meter": {
        "power": ("household_power_w", "W"),
        "active_power": ("household_power_w", "W"),
        "active_power_w": ("household_power_w", "W"),
        "meter_power_w": ("household_power_w", "W"),
        "household_power_w": ("household_power_w", "W"),
        "PAPP": ("household_power_w", "VA"),
        "SINSTS": ("household_power_w", "VA"),
        "energy": ("household_energy_wh", "Wh"),
        "energy_wh": ("household_energy_wh", "Wh"),
        "household_energy_wh": ("household_energy_wh", "Wh"),
        "BASE": ("household_energy_wh", "Wh"),
        "EAST": ("household_energy_wh", "Wh"),
        "EASF01": ("household_energy_wh", "Wh"),
        "voltage": ("household_voltage_v", "V"),
        "voltage_v": ("household_voltage_v", "V"),
        "current": ("household_current_a", "A"),
        "current_a": ("household_current_a", "A"),
        "linkquality": ("household_link_quality", "lqi"),
    },
}

FALLBACK_FIELD_MAP: dict[str, tuple[str, str | None]] = {
    "state": ("plug_state", None),
    "power": ("plug_power_w", "W"),
    "voltage": ("plug_voltage_v", "V"),
    "current": ("plug_current_a", "A"),
    "energy": ("plug_energy_wh", "Wh"),
    "energy_today": ("plug_energy_today_wh", "Wh"),
    "energy_yesterday": ("plug_energy_yesterday_wh", "Wh"),
    "energy_month": ("plug_energy_month_wh", "Wh"),
    "temperature": ("indoor_temperature_c", "degC"),
    "humidity": ("indoor_humidity_pct", "pct"),
    "battery": ("sensor_battery_pct", "pct"),
    "linkquality": ("signal_link_quality", "lqi"),
    "household_power_w": ("household_power_w", "W"),
    "meter_power_w": ("household_power_w", "W"),
    "active_power": ("household_power_w", "W"),
    "active_power_w": ("household_power_w", "W"),
    "PAPP": ("household_power_w", "VA"),
    "SINSTS": ("household_power_w", "VA"),
    "household_energy_wh": ("household_energy_wh", "Wh"),
    "energy_wh": ("household_energy_wh", "Wh"),
    "BASE": ("household_energy_wh", "Wh"),
    "EAST": ("household_energy_wh", "Wh"),
    "EASF01": ("household_energy_wh", "Wh"),
}


def normalize_raw_row(
    raw_row: RawTelemetryEvent | dict[str, str], devices: dict[str, dict[str, Any]]
) -> NormalizedEvent:
    raw_event = ensure_raw_event(raw_row)
    device_config = devices.get(raw_event.device, {})
    device_role = device_config.get("role", "unknown_device")
    canonical_field, unit = resolve_field_mapping(device_role, raw_event.field)
    return NormalizedEvent(
        timestamp=raw_event.timestamp,
        device_id=raw_event.device,
        device_role=device_role,
        device_name=device_config.get("name", raw_event.device),
        field=canonical_field,
        value=parse_raw_value(raw_event.field, raw_event.value),
        unit=unit,
        source=raw_event.source,
    )


def ensure_raw_event(raw_row: RawTelemetryEvent | dict[str, str]) -> RawTelemetryEvent:
    if isinstance(raw_row, RawTelemetryEvent):
        return raw_row
    metadata = dict(raw_row.get("metadata") or {})
    if "device_name" in raw_row:
        metadata.setdefault("device_name", raw_row["device_name"])
    if "raw_payload" in raw_row:
        metadata.setdefault("raw_payload", raw_row["raw_payload"])
    if "mqtt_topic" in raw_row:
        metadata.setdefault("mqtt_topic", raw_row["mqtt_topic"])
    if "edf_stream" in raw_row:
        metadata.setdefault("edf_stream", raw_row["edf_stream"])
    return RawTelemetryEvent(
        timestamp=datetime.strptime(raw_row["timestamp"], TIMESTAMP_FORMAT),
        device=raw_row["device"],
        field=raw_row["field"],
        value=raw_row["value"],
        source=raw_row.get("source", "zigbee"),
        metadata=metadata,
    )


def resolve_field_mapping(device_role: str, raw_field: str) -> tuple[str, str | None]:
    role_map = ROLE_FIELD_MAPS.get(device_role, {})
    if raw_field in role_map:
        return role_map[raw_field]
    return FALLBACK_FIELD_MAP.get(raw_field, (raw_field, None))


def parse_raw_value(field: str, raw_value: str) -> str | float | int:
    if field == "state":
        return raw_value.strip().upper()

    numeric_fields = {
        "power",
        "voltage",
        "current",
        "energy",
        "energy_today",
        "energy_yesterday",
        "energy_month",
        "temperature",
        "humidity",
        "battery",
        "linkquality",
        "active_power",
        "active_power_w",
        "meter_power_w",
        "household_power_w",
        "PAPP",
        "SINSTS",
        "energy_wh",
        "household_energy_wh",
        "voltage_v",
        "current_a",
        "BASE",
        "EAST",
        "EASF01",
    }
    if field not in numeric_fields:
        return raw_value

    numeric = float(raw_value)
    if field in {"battery", "linkquality"} and numeric.is_integer():
        return int(numeric)
    return numeric


def apply_normalized_event_to_state(
    event: NormalizedEvent,
    state: dict[str, Any],
    last_update: dict[str, datetime],
) -> None:
    if event.field in {
        "household_power_w",
        "household_energy_wh",
        "household_voltage_v",
        "household_current_a",
        "household_link_quality",
    }:
        state[event.field] = event.value
        last_update[event.field] = event.timestamp
        if event.field == "household_power_w":
            state["_household_meter_power_observed"] = True
            state["household_power_source"] = "household_meter"
            last_update["household_power_source"] = event.timestamp
        elif event.field == "household_energy_wh":
            state["_household_meter_energy_observed"] = True
        return

    if event.device_role == "smart_plug" and event.field in {
        "plug_state",
        "plug_power_w",
        "plug_voltage_v",
        "plug_current_a",
        "plug_energy_wh",
        "plug_energy_today_wh",
        "plug_energy_yesterday_wh",
        "plug_energy_month_wh",
        "plug_link_quality",
    }:
        plug_state = state.setdefault("_smart_plugs", {})
        plug_last_update = last_update.setdefault("_smart_plugs", {})
        device_values = plug_state.setdefault(event.device_id, {})
        device_last_update = plug_last_update.setdefault(event.device_id, {})
        device_values[event.field] = event.value
        device_last_update[event.field] = event.timestamp
        _refresh_smart_plug_aggregates(state, last_update)
        return

    if event.device_role == "household_meter":
        state[event.field] = event.value
        last_update[event.field] = event.timestamp
        if event.field == "household_power_w":
            state["_household_meter_power_observed"] = True
            state["household_power_source"] = "household_meter"
            last_update["household_power_source"] = event.timestamp
        elif event.field == "household_energy_wh":
            state["_household_meter_energy_observed"] = True
        return

    state[event.field] = event.value
    last_update[event.field] = event.timestamp


def build_snapshot(
    current_time: datetime,
    interval_seconds: int,
    state: dict[str, Any],
    last_update: dict[str, datetime],
    devices: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    smart_plug_ids = _smart_plug_ids(state, devices)
    plug_1 = _plug_snapshot_values(smart_plug_ids, state, last_update, 0)
    plug_2 = _plug_snapshot_values(smart_plug_ids, state, last_update, 1)
    household_source = state.get("household_power_source")
    return {
        "timestamp": current_time.isoformat(),
        "interval_seconds": interval_seconds,
        "plug_1_device_id": plug_1["device_id"],
        "plug_1_state": plug_1["state"],
        "plug_1_power_w": plug_1["power_w"],
        "plug_1_voltage_v": plug_1["voltage_v"],
        "plug_1_current_a": plug_1["current_a"],
        "plug_1_energy_wh": plug_1["energy_wh"],
        "plug_1_link_quality": plug_1["link_quality"],
        "plug_1_data_age_s": age_in_seconds(current_time, plug_1["latest_timestamp"]),
        "plug_2_device_id": plug_2["device_id"],
        "plug_2_state": plug_2["state"],
        "plug_2_power_w": plug_2["power_w"],
        "plug_2_voltage_v": plug_2["voltage_v"],
        "plug_2_current_a": plug_2["current_a"],
        "plug_2_energy_wh": plug_2["energy_wh"],
        "plug_2_link_quality": plug_2["link_quality"],
        "plug_2_data_age_s": age_in_seconds(current_time, plug_2["latest_timestamp"]),
        "plug_state": state.get("plug_state"),
        "plug_power_w": state.get("plug_power_w"),
        "plug_voltage_v": state.get("plug_voltage_v"),
        "plug_current_a": state.get("plug_current_a"),
        "plug_energy_wh": state.get("plug_energy_wh"),
        "plug_energy_today_wh": state.get("plug_energy_today_wh"),
        "plug_energy_yesterday_wh": state.get("plug_energy_yesterday_wh"),
        "plug_energy_month_wh": state.get("plug_energy_month_wh"),
        "plug_link_quality": state.get("plug_link_quality"),
        "plug_data_age_s": age_in_seconds(
            current_time,
            latest_timestamp(
                last_update,
                [
                    "plug_power_w",
                    "plug_voltage_v",
                    "plug_current_a",
                    "plug_energy_wh",
                ],
            ),
        ),
        "indoor_temperature_c": state.get("indoor_temperature_c"),
        "indoor_humidity_pct": state.get("indoor_humidity_pct"),
        "sensor_battery_pct": state.get("sensor_battery_pct"),
        "sensor_link_quality": state.get("sensor_link_quality"),
        "environment_data_age_s": age_in_seconds(
            current_time,
            latest_timestamp(
                last_update,
                [
                    "indoor_temperature_c",
                    "indoor_humidity_pct",
                    "sensor_battery_pct",
                ],
            ),
        ),
        "household_power_w": state.get("household_power_w"),
        "household_energy_wh": state.get("household_energy_wh"),
        "household_link_quality": state.get("household_link_quality"),
        "household_data_age_s": age_in_seconds(
            current_time,
            latest_timestamp(last_update, ["household_power_w", "household_energy_wh"]),
        ),
        "household_power_source": household_source,
    }


def _refresh_smart_plug_aggregates(
    state: dict[str, Any],
    last_update: dict[str, datetime],
) -> None:
    plug_values = state.get("_smart_plugs", {})
    plug_updates = last_update.get("_smart_plugs", {})

    powers = _numeric_device_values(plug_values, "plug_power_w")
    currents = _numeric_device_values(plug_values, "plug_current_a")
    voltages = _numeric_device_values(plug_values, "plug_voltage_v")
    energies = _numeric_device_values(plug_values, "plug_energy_wh")
    link_qualities = _numeric_device_values(plug_values, "plug_link_quality")
    states = [
        values.get("plug_state")
        for values in plug_values.values()
        if values.get("plug_state") is not None
    ]

    if powers:
        total_power = round(sum(powers), 4)
        state["plug_power_w"] = total_power
        last_update["plug_power_w"] = _latest_device_timestamp(plug_updates, "plug_power_w")
        if not state.get("_household_meter_power_observed"):
            state["household_power_w"] = total_power
            state["household_power_source"] = "sum_of_configured_smart_plugs"
            last_update["household_power_w"] = last_update["plug_power_w"]
            last_update["household_power_source"] = last_update["plug_power_w"]
    if currents:
        state["plug_current_a"] = round(sum(currents), 4)
        last_update["plug_current_a"] = _latest_device_timestamp(plug_updates, "plug_current_a")
    if voltages:
        state["plug_voltage_v"] = round(sum(voltages) / len(voltages), 4)
        last_update["plug_voltage_v"] = _latest_device_timestamp(plug_updates, "plug_voltage_v")
    if energies:
        state["plug_energy_wh"] = round(sum(energies), 4)
        last_update["plug_energy_wh"] = _latest_device_timestamp(plug_updates, "plug_energy_wh")
        if not state.get("_household_meter_energy_observed"):
            state["household_energy_wh"] = state["plug_energy_wh"]
            last_update["household_energy_wh"] = last_update["plug_energy_wh"]
    if link_qualities:
        state["plug_link_quality"] = round(sum(link_qualities) / len(link_qualities), 4)
        last_update["plug_link_quality"] = _latest_device_timestamp(plug_updates, "plug_link_quality")
    if states:
        state["plug_state"] = "ON" if any(value == "ON" for value in states) else "OFF"
        last_update["plug_state"] = _latest_device_timestamp(plug_updates, "plug_state")


def _smart_plug_ids(
    state: dict[str, Any],
    devices: dict[str, dict[str, Any]] | None,
) -> list[str]:
    configured = [
        device_id
        for device_id, config in (devices or {}).items()
        if config.get("role") == "smart_plug"
    ]
    observed = list((state.get("_smart_plugs") or {}).keys())
    return sorted(dict.fromkeys([*configured, *observed]))


def _plug_snapshot_values(
    smart_plug_ids: list[str],
    state: dict[str, Any],
    last_update: dict[str, datetime],
    index: int,
) -> dict[str, Any]:
    device_id = smart_plug_ids[index] if index < len(smart_plug_ids) else None
    values = (state.get("_smart_plugs") or {}).get(device_id, {}) if device_id else {}
    updates = (last_update.get("_smart_plugs") or {}).get(device_id, {}) if device_id else {}
    return {
        "device_id": device_id,
        "state": values.get("plug_state"),
        "power_w": values.get("plug_power_w"),
        "voltage_v": values.get("plug_voltage_v"),
        "current_a": values.get("plug_current_a"),
        "energy_wh": values.get("plug_energy_wh"),
        "link_quality": values.get("plug_link_quality"),
        "latest_timestamp": latest_timestamp(
            updates,
            ["plug_power_w", "plug_voltage_v", "plug_current_a", "plug_energy_wh", "plug_state"],
        ),
    }


def _numeric_device_values(
    plug_values: dict[str, dict[str, Any]],
    field: str,
) -> list[float]:
    values = []
    for device_values in plug_values.values():
        value = device_values.get(field)
        if isinstance(value, (int, float)):
            values.append(float(value))
    return values


def _latest_device_timestamp(
    plug_updates: dict[str, dict[str, datetime]],
    field: str,
) -> datetime:
    timestamps = [
        updates[field]
        for updates in plug_updates.values()
        if field in updates
    ]
    return max(timestamps)


def latest_timestamp(
    last_update: dict[str, datetime], keys: list[str]
) -> datetime | None:
    candidates = [last_update[key] for key in keys if key in last_update]
    if not candidates:
        return None
    return max(candidates)


def age_in_seconds(current_time: datetime, seen_at: datetime | None) -> int | None:
    if seen_at is None:
        return None
    return int((current_time - seen_at).total_seconds())


