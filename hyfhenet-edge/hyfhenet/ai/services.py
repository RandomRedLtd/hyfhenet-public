from __future__ import annotations

from typing import Any, Sequence

from ..core.interfaces import EdgeService, PipelineContext
from ..core.models import EdgeServiceResult, FeatureWindow


class EdgeServiceRunner:
    def __init__(self, services: Sequence[EdgeService]) -> None:
        self.services = list(services)

    def evaluate_feature_window(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> list[EdgeServiceResult]:
        results: list[EdgeServiceResult] = []
        for service in self.services:
            results.extend(service.evaluate(feature_window, context))
        return results

    def service_ids(self) -> list[str]:
        return [getattr(service, "SERVICE_ID", service.__class__.__name__) for service in self.services]


class EnergyProfileService(EdgeService):
    SERVICE_ID = "energy_profile"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config["edge_services"]["energy_profile"]

    def evaluate(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> list[EdgeServiceResult]:
        timestamp = feature_window.timestamp
        plug_mean_1m = _as_float(feature_window.get("plug_power_mean_1m_w"))
        plug_mean_5m = _as_float(feature_window.get("plug_power_mean_5m_w"))
        household_mean_1m = _as_float(feature_window.get("household_power_mean_1m_w"))
        household_mean_5m = _as_float(feature_window.get("household_power_mean_5m_w"))
        plug_latest = _as_float(feature_window.get("plug_power_latest_w"))
        ratio = _as_float(feature_window.get("device_to_household_ratio"))
        on_fraction_1m = _as_float(feature_window.get("plug_on_fraction_1m"))
        household_context = feature_window.get("household_load_band")
        reasons: list[str] = []

        plug_stale = int(feature_window.get("plug_stale_flag") or 0) == 1
        household_stale = int(feature_window.get("household_stale_flag") or 0) == 1

        if plug_stale:
            reasons.append("plug_stale")
        if household_stale:
            reasons.append("household_stale")
        if plug_mean_1m is None:
            reasons.append("missing_plug_window")
        if household_mean_1m is None:
            reasons.append("missing_household_window")

        service_status = "ok"
        data_quality = "good"
        if plug_mean_1m is None:
            service_status = "insufficient_data"
            data_quality = "insufficient"
        elif plug_stale or household_stale:
            service_status = "degraded"
            data_quality = "degraded"

        activity_state = self._activity_state(
            plug_latest=plug_latest,
            plug_mean_1m=plug_mean_1m,
            on_fraction_1m=on_fraction_1m,
        )
        dominant_load_flag = 1 if self._is_dominant_load(ratio, activity_state) else 0
        profile_state = self._profile_state(
            service_status=service_status,
            activity_state=activity_state,
            dominant_load_flag=dominant_load_flag,
            plug_latest=plug_latest,
            household_context=household_context,
        )

        return [
            EdgeServiceResult(
                timestamp=timestamp,
                service_id=self.SERVICE_ID,
                service_version=str(self.config["version"]),
                service_status=service_status,
                profile_state=profile_state,
                activity_state=activity_state,
                household_context=household_context,
                data_quality=data_quality,
                dominant_load_flag=dominant_load_flag,
                appliance_share_pct=round(ratio * 100.0, 2) if ratio is not None else None,
                plug_power_mean_1m_w=plug_mean_1m,
                plug_power_mean_5m_w=plug_mean_5m,
                household_power_mean_1m_w=household_mean_1m,
                household_power_mean_5m_w=household_mean_5m,
                reasons=";".join(reasons) if reasons else "none",
            )
        ]

    def _activity_state(
        self,
        plug_latest: float | None,
        plug_mean_1m: float | None,
        on_fraction_1m: float | None,
    ) -> str:
        standby_threshold = float(self.config["standby_power_threshold_w"])
        active_threshold = float(self.config["active_power_threshold_w"])
        active_fraction_threshold = float(self.config["active_on_fraction_threshold"])
        transition_fraction_threshold = float(self.config["transition_on_fraction_threshold"])

        if plug_latest is None and plug_mean_1m is None:
            return "unknown"
        if (plug_latest or 0.0) <= standby_threshold and (on_fraction_1m or 0.0) <= 0.2:
            return "idle"
        if (plug_mean_1m or 0.0) >= active_threshold and (on_fraction_1m or 0.0) >= active_fraction_threshold:
            return "active"
        if (plug_mean_1m or 0.0) >= active_threshold or (on_fraction_1m or 0.0) >= transition_fraction_threshold:
            return "transition"
        return "standby"

    def _is_dominant_load(self, ratio: float | None, activity_state: str) -> bool:
        if ratio is None:
            return False
        return ratio >= float(self.config["dominant_share_threshold"]) and activity_state in {
            "active",
            "transition",
        }

    @staticmethod
    def _profile_state(
        service_status: str,
        activity_state: str,
        dominant_load_flag: int,
        plug_latest: float | None,
        household_context: str | None,
    ) -> str:
        if service_status == "insufficient_data":
            return "insufficient_data"
        if service_status == "degraded":
            return "degraded_profile"
        if dominant_load_flag:
            return "dominant_appliance_load"
        if activity_state == "active":
            return "active_appliance_load"
        if activity_state == "transition":
            return "transient_appliance_load"
        if household_context in {"medium", "high"} and (plug_latest or 0.0) <= 5.0:
            return "background_household_load"
        return "standby_appliance_load"


def build_default_edge_services(config: dict[str, Any]) -> list[EdgeService]:
    services: list[EdgeService] = []
    energy_profile_config = config.get("edge_services", {}).get("energy_profile", {})
    if energy_profile_config.get("enabled", True):
        services.append(EnergyProfileService(config))
    return services


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            return None
    return None


