from __future__ import annotations

from typing import Any, Sequence

from ..core.interfaces import EdgeEventDetector, PipelineContext
from ..core.models import FeatureWindow, LoadEventMarker

LOAD_EVENT_MARKER_COLUMNS = [
    "timestamp",
    "detector_id",
    "detector_version",
    "detector_status",
    "event_type",
    "event_state",
    "signal_name",
    "direction",
    "magnitude_w",
    "baseline_w",
    "current_w",
    "threshold_w",
    "appliance_share_pct",
    "confidence",
    "should_forward",
    "reasons",
]


class EdgeEventGateRunner:
    def __init__(self, detectors: Sequence[EdgeEventDetector]) -> None:
        self.detectors = list(detectors)

    def evaluate_feature_window(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> list[LoadEventMarker]:
        markers: list[LoadEventMarker] = []
        for detector in self.detectors:
            markers.extend(detector.evaluate(feature_window, context))
        return markers

    def detector_ids(self) -> list[str]:
        return [
            getattr(detector, "DETECTOR_ID", detector.__class__.__name__)
            for detector in self.detectors
        ]


class LoadEventGateDetector(EdgeEventDetector):
    DETECTOR_ID = "load_event_gate"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config["edge_event_gates"]["load_event_gate"]
        self.previous_power_w: float | None = None
        self.previous_active_state: bool | None = None
        self.cooldown_remaining = 0

    def evaluate(
        self,
        feature_window: FeatureWindow,
        context: PipelineContext,
    ) -> list[LoadEventMarker]:
        current_power = _first_float(
            feature_window.get("plug_power_latest_w"),
            feature_window.get("plug_power_mean_1m_w"),
        )
        ratio = _as_float(feature_window.get("device_to_household_ratio"))
        plug_stale = int(feature_window.get("plug_stale_flag") or 0) == 1
        household_stale = int(feature_window.get("household_stale_flag") or 0) == 1

        if current_power is None:
            return []
        if plug_stale and bool(self.config.get("suppress_when_plug_stale", True)):
            return []

        active_state = self._active_state(current_power)
        previous_power = self.previous_power_w
        previous_active_state = self.previous_active_state
        marker: LoadEventMarker | None = None

        if previous_power is None:
            marker = self._build_initial_marker(
                feature_window=feature_window,
                current_power=current_power,
                active_state=active_state,
                ratio=ratio,
                plug_stale=plug_stale,
                household_stale=household_stale,
            )
        else:
            delta = current_power - previous_power
            magnitude = abs(delta)
            marker = self._build_marker_if_event(
                feature_window=feature_window,
                previous_power=previous_power,
                current_power=current_power,
                previous_active_state=previous_active_state,
                active_state=active_state,
                delta=delta,
                magnitude=magnitude,
                ratio=ratio,
                plug_stale=plug_stale,
                household_stale=household_stale,
            )

        self.previous_power_w = current_power
        self.previous_active_state = active_state
        if marker is not None:
            self.cooldown_remaining = int(self.config.get("cooldown_windows", 1))
            return [marker]
        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1
        return []

    def _build_initial_marker(
        self,
        feature_window: FeatureWindow,
        current_power: float,
        active_state: bool,
        ratio: float | None,
        plug_stale: bool,
        household_stale: bool,
    ) -> LoadEventMarker | None:
        if not bool(self.config.get("emit_initial_state_marker", True)):
            return None
        if not active_state and not bool(self.config.get("emit_initial_inactive_marker", False)):
            return None

        reasons = ["initial_state_observed"]
        if plug_stale:
            reasons.append("plug_stale")
        if household_stale:
            reasons.append("household_stale")
        if ratio is None:
            reasons.append("missing_household_share")

        configured_forward_events = set(self.config.get("forward_event_types", []))
        forwardable_type = not configured_forward_events or "initial_state" in configured_forward_events
        confidence = 0.75 if active_state else 0.5
        if plug_stale or household_stale:
            confidence *= 0.75

        return LoadEventMarker(
            timestamp=feature_window.timestamp,
            detector_id=self.DETECTOR_ID,
            detector_version=str(self.config["version"]),
            detector_status="degraded" if plug_stale or household_stale else "ok",
            event_type="initial_state",
            event_state="active" if active_state else "inactive",
            signal_name="plug_power_w",
            direction="none",
            magnitude_w=0.0,
            baseline_w=None,
            current_w=round(current_power, 4),
            threshold_w=round(float(self.config["power_on_threshold_w"]), 4),
            appliance_share_pct=round(ratio * 100.0, 2) if ratio is not None else None,
            confidence=round(confidence, 4),
            should_forward=1 if forwardable_type else 0,
            reasons=";".join(reasons),
        )

    def _build_marker_if_event(
        self,
        feature_window: FeatureWindow,
        previous_power: float,
        current_power: float,
        previous_active_state: bool | None,
        active_state: bool,
        delta: float,
        magnitude: float,
        ratio: float | None,
        plug_stale: bool,
        household_stale: bool,
    ) -> LoadEventMarker | None:
        min_event_delta = float(self.config["min_event_delta_w"])
        min_ramp_delta = float(self.config["min_ramp_delta_w"])
        event_type: str | None = None
        event_state: str | None = None
        threshold = min_event_delta
        reasons: list[str] = []

        if previous_active_state is not None and previous_active_state != active_state and magnitude >= min_event_delta:
            event_type = "switch_on" if active_state else "switch_off"
            event_state = "active" if active_state else "inactive"
            reasons.append("hysteresis_state_transition")
        elif magnitude >= min_ramp_delta and self.cooldown_remaining <= 0:
            event_type = "ramp_up" if delta > 0 else "ramp_down"
            event_state = "active" if active_state else "inactive"
            threshold = min_ramp_delta
            reasons.append("power_delta_threshold")

        if event_type is None or event_state is None:
            return None

        direction = "increase" if delta > 0 else "decrease"
        if plug_stale:
            reasons.append("plug_stale")
        if household_stale:
            reasons.append("household_stale")
        if ratio is None:
            reasons.append("missing_household_share")

        confidence = self._confidence(magnitude, threshold, ratio, plug_stale or household_stale)
        configured_forward_events = set(self.config.get("forward_event_types", []))
        forwardable_type = not configured_forward_events or event_type in configured_forward_events
        min_confidence = float(self.config.get("min_forward_confidence", 0.5))
        should_forward = 1 if forwardable_type and confidence >= min_confidence else 0

        return LoadEventMarker(
            timestamp=feature_window.timestamp,
            detector_id=self.DETECTOR_ID,
            detector_version=str(self.config["version"]),
            detector_status="degraded" if plug_stale or household_stale else "ok",
            event_type=event_type,
            event_state=event_state,
            signal_name="plug_power_w",
            direction=direction,
            magnitude_w=round(magnitude, 4),
            baseline_w=round(previous_power, 4),
            current_w=round(current_power, 4),
            threshold_w=round(threshold, 4),
            appliance_share_pct=round(ratio * 100.0, 2) if ratio is not None else None,
            confidence=confidence,
            should_forward=should_forward,
            reasons=";".join(reasons) if reasons else "none",
        )

    def _active_state(self, current_power: float) -> bool:
        on_threshold = float(self.config["power_on_threshold_w"])
        off_threshold = float(self.config["power_off_threshold_w"])
        if self.previous_active_state is True:
            return current_power > off_threshold
        return current_power >= on_threshold

    @staticmethod
    def _confidence(
        magnitude: float,
        threshold: float,
        ratio: float | None,
        degraded: bool,
    ) -> float:
        magnitude_score = min(1.0, magnitude / max(threshold, 1.0))
        ratio_score = 0.5 if ratio is None else min(1.0, max(ratio, 0.0) / 0.25)
        confidence = (0.75 * magnitude_score) + (0.25 * ratio_score)
        if degraded:
            confidence *= 0.75
        return round(min(1.0, max(0.0, confidence)), 4)


def build_default_event_gates(config: dict[str, Any]) -> list[EdgeEventDetector]:
    detectors: list[EdgeEventDetector] = []
    gate_config = config.get("edge_event_gates", {}).get("load_event_gate", {})
    if gate_config.get("enabled", True):
        detectors.append(LoadEventGateDetector(config))
    return detectors


def _first_float(*values: Any) -> float | None:
    for value in values:
        converted = _as_float(value)
        if converted is not None:
            return converted
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            return None
    return None
