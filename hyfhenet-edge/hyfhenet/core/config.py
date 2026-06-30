from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "input_source": "csv",
    "pipeline": {
        "interval_seconds": 30,
    },
    "edge_processing": {
        "feature_windows_seconds": [60, 300],
        "max_plug_staleness_seconds": 120,
        "max_environment_staleness_seconds": 600,
        "max_household_staleness_seconds": 180,
        "temperature_min_c": -20.0,
        "temperature_max_c": 60.0,
        "humidity_min_pct": 0.0,
        "humidity_max_pct": 100.0,
        "load_band_medium_w": 150.0,
        "load_band_high_w": 500.0,
    },
    "edge_services": {
        "energy_profile": {
            "enabled": True,
            "version": "1.0",
            "standby_power_threshold_w": 5.0,
            "active_power_threshold_w": 20.0,
            "transition_on_fraction_threshold": 0.5,
            "active_on_fraction_threshold": 0.8,
            "dominant_share_threshold": 0.6,
        }
    },
    "edge_event_gates": {
        "load_event_gate": {
            "enabled": True,
            "version": "1.0",
            "power_on_threshold_w": 20.0,
            "power_off_threshold_w": 5.0,
            "min_event_delta_w": 15.0,
            "min_ramp_delta_w": 25.0,
            "cooldown_windows": 1,
            "min_forward_confidence": 0.5,
            "emit_initial_state_marker": True,
            "emit_initial_inactive_marker": False,
            "forward_event_types": [
                "initial_state",
                "switch_on",
                "switch_off",
                "ramp_up",
                "ramp_down",
            ],
            "suppress_when_plug_stale": True,
        }
    },
    "edge_models": {
        "energy_anomaly_monitor": {
            "enabled": True,
            "model_version": "1.0",
            "backend": "online_statistical_detector",
            "active_power_reference_w": 100.0,
            "warmup_windows": 5,
            "power_zscore_threshold": 3.0,
            "ratio_zscore_threshold": 2.5,
            "dominant_share_min": 0.6,
            "sustained_load_threshold_w": 120.0,
            "sustained_on_fraction_threshold": 0.95,
            "sustained_share_min": 0.35,
            "sustained_zscore_threshold": 1.5,
            "stale_penalty": 0.35,
        },
        "short_term_load_forecast": {
            "enabled": True,
            "model_version": "1.0",
            "backend": "ridge_regression",
            "model_path": "models/edge_load_forecast_ridge.json",
            "horizon_minutes": 1,
        }
    },
    "cloud_forecast": {
        "enabled": True,
        "schema_version": "1.0",
        "feature_set_id": "long_term_load_forecast_v1",
        "horizons_minutes": [120, 1440],
        "runtime_horizon_minutes": 120,
        "recommended_quantization_bits": 8,
    },
    "cloud_nilm": {
        "enabled": True,
        "schema_version": "1.0",
        "feature_set_id": "nilm_disaggregation_v1",
        "recommended_quantization_bits": 8,
    },
    "cloud_cohort": {
        "enabled": True,
        "schema_version": "1.0",
        "feature_set_id": "cohort_benchmark_v1",
        "recommended_quantization_bits": 8,
    },
    "fhe_cloud": {
        "enabled": True,
        "api_url": None,
        "api_key": None,
        "cache_dir": None,
        "client_cert_path": None,
        "client_key_path": None,
        "ca_bundle_path": None,
        "request_timeout_seconds": 60.0,
        "allow_insecure_http": False,
        "tasks": {
            "forecast": {
                "enabled": True,
                "model_version": "1.0",
                "backend": "concrete_ml_remote_fhe",
                "model_name": "forecast",
                "horizon_minutes": 120,
                "feature_set_id": "long_term_load_forecast_v1",
            },
            "nilm": {
                "enabled": True,
                "model_version": "1.0",
                "backend": "concrete_ml_remote_fhe",
                "model_name": "nilm",
                "feature_set_id": "nilm_disaggregation_v1",
            },
            "cohort": {
                "enabled": True,
                "model_version": "1.0",
                "backend": "concrete_ml_remote_fhe",
                "model_name": "cohort",
                "feature_set_id": "cohort_benchmark_v1",
            }
        },
    },
    "devices": {
        "0xa4c1380538efffff": {
            "role": "smart_plug",
            "name": "zigbee_smart_plug_1",
            "location": "participant_home",
        },
        "0xa4c138057801ffff": {
            "role": "smart_plug",
            "name": "zigbee_smart_plug_2",
            "location": "participant_home",
        },
        "0xd44867fffe098162": {
            "role": "environment_sensor",
            "name": "zigbee_room_climate_sensor",
            "location": "participant_home",
        },
    },
    "gateway_stream": {
        "stage_order": [
            "preprocessing",
            "feature_engineering",
            "event_gating",
            "cloud_forecast_prep",
            "fhe_cloud_inference",
            "service_inference",
            "ai_modeling",
        ],
        "follow_event_timing": False,
        "replay_speed_multiplier": 1.0,
        "max_replay_sleep_seconds": None,
        "profiling_enabled": True,
        "idle_poll_seconds": 0.1,
        "group_flush_seconds": 0.2,
        "flush_every_records": 10,
        "flush_interval_seconds": 2.0,
        "console_log_level": "none",
        "replay_delay_ms": 0,
    },
    "zigbee_gateway": {
        "host": "localhost",
        "port": 1883,
        "topic": "zigbee2mqtt/#",
        "topic_prefix": "zigbee2mqtt/",
        "listen_seconds": 30,
        "keepalive_seconds": 60,
        "connect_timeout_seconds": 10,
        "reconnect_min_delay_seconds": 1,
        "reconnect_max_delay_seconds": 30,
        "debug_bridge_messages": False,
        "include_unsupported_fields": False,
        "supported_fields": [
            "temperature",
            "humidity",
            "battery",
            "state",
            "power",
            "voltage",
            "current",
            "energy",
            "energy_today",
            "energy_yesterday",
            "energy_month",
            "linkquality",
            "active_power",
            "active_power_w",
            "meter_power_w",
            "household_power_w",
            "energy_wh",
            "household_energy_wh",
            "PAPP",
            "SINSTS",
            "BASE",
            "EAST",
            "EASF01",
        ],
        "client_id": "hyfhenet-zigbee-gateway",
        "username": None,
        "password": None,
        "transport": "tcp",
        "tls_enabled": False,
        "ca_cert_path": None,
        "client_cert_path": None,
        "client_key_path": None,
        "tls_insecure": False,
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_pipeline_config(path: Path | str | None) -> dict[str, Any]:
    if path is None:
        config = deepcopy(DEFAULT_CONFIG)
        _apply_environment_overrides(config)
        return config

    path = Path(path)
    if not path.exists():
        config = deepcopy(DEFAULT_CONFIG)
        _apply_environment_overrides(config)
        return config

    loaded = json.loads(path.read_text(encoding="utf-8-sig"))
    config = _deep_merge(DEFAULT_CONFIG, loaded)
    _apply_environment_overrides(config)
    return config


def _apply_environment_overrides(config: dict[str, Any]) -> None:
    _update_from_env(
        config.setdefault("zigbee_gateway", {}),
        {
            "host": ("HYFHENET_ZIGBEE_HOST", str),
            "port": ("HYFHENET_ZIGBEE_PORT", int),
            "topic": ("HYFHENET_ZIGBEE_TOPIC", str),
            "topic_prefix": ("HYFHENET_ZIGBEE_TOPIC_PREFIX", str),
            "listen_seconds": ("HYFHENET_ZIGBEE_LISTEN_SECONDS", int),
            "username": ("HYFHENET_ZIGBEE_USERNAME", str),
            "password": ("HYFHENET_ZIGBEE_PASSWORD", str),
            "client_id": ("HYFHENET_ZIGBEE_CLIENT_ID", str),
            "tls_enabled": ("HYFHENET_ZIGBEE_TLS", _bool_env),
            "ca_cert_path": ("HYFHENET_ZIGBEE_CA_CERT", str),
            "client_cert_path": ("HYFHENET_ZIGBEE_CLIENT_CERT", str),
            "client_key_path": ("HYFHENET_ZIGBEE_CLIENT_KEY", str),
            "tls_insecure": ("HYFHENET_ZIGBEE_TLS_INSECURE", _bool_env),
        },
    )
    _update_from_env(
        config.setdefault("fhe_cloud", {}),
        {
            "api_url": ("HYFHENET_FHE_API_URL", str),
            "api_key": ("HYFHENET_FHE_API_KEY", str),
            "cache_dir": ("HYFHENET_FHE_CACHE_DIR", str),
            "client_cert_path": ("HYFHENET_FHE_CLIENT_CERT", str),
            "client_key_path": ("HYFHENET_FHE_CLIENT_KEY", str),
            "ca_bundle_path": ("HYFHENET_FHE_CA_BUNDLE", str),
            "allow_insecure_http": ("HYFHENET_FHE_ALLOW_INSECURE_HTTP", _bool_env),
        },
    )


def _update_from_env(
    target: dict[str, Any],
    mapping: dict[str, tuple[str, Any]],
) -> None:
    for key, (env_name, parser) in mapping.items():
        raw_value = os.getenv(env_name)
        if raw_value is None or raw_value == "":
            continue
        target[key] = parser(raw_value)


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}

