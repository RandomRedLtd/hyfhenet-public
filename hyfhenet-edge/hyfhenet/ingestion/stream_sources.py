from __future__ import annotations

import csv
import json
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Literal

from ..runtime.observers import NullStreamingObserver
from ..core.interfaces import PipelineContext, StreamingEventSource, StreamingObserver
from ..core.models import RawTelemetryEvent
from .normalization import (
    TIMESTAMP_FORMAT,
    ensure_raw_event,
)

RAW_TELEMETRY_COLUMNS = ["timestamp", "device", "field", "value", "source"]
DEFAULT_ZIGBEE_TELEMETRY_FIELDS = {
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
}


def decode_mqtt_message(
    topic: str,
    payload: bytes | str,
    received_at: datetime,
    topic_prefix: str,
    allow_bridge_debug: bool = False,
    debug_log_fn: Callable[[str], None] | None = None,
    supported_fields: set[str] | None = None,
    include_unsupported_fields: bool = False,
) -> list[dict[str, str]]:
    payload_text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    try:
        message = json.loads(payload_text)
    except json.JSONDecodeError:
        return []

    if not isinstance(message, dict):
        return []

    device_id = _device_id_from_topic(topic, topic_prefix)
    if device_id == "bridge":
        if allow_bridge_debug and debug_log_fn is not None:
            debug_log_fn(f"[zigbee_mqtt] ignoring bridge topic {topic}: {payload_text}")
        return []
    if not device_id:
        return []

    timestamp = _coerce_timestamp(message.get("timestamp"), received_at)
    rows: list[dict[str, str]] = []
    telemetry_fields = supported_fields or DEFAULT_ZIGBEE_TELEMETRY_FIELDS
    for field, value in message.items():
        if field == "timestamp" or isinstance(value, (dict, list)):
            continue
        if field not in telemetry_fields and not include_unsupported_fields:
            continue
        rows.append(
            {
                "timestamp": timestamp,
                "device": device_id,
                "device_name": device_id,
                "field": field,
                "value": _scalar_to_string(value),
                "raw_payload": payload_text,
                "mqtt_topic": topic,
            }
        )
    return rows


class ZigbeeCsvReplaySource(StreamingEventSource):
    def __init__(self, encoding: str = "utf-8-sig") -> None:
        self.encoding = encoding

    def stream(self, context: PipelineContext):
        input_path = Path(context.input_path)
        with input_path.open(newline="", encoding=self.encoding) as handle:
            for row in csv.DictReader(handle):
                yield ensure_raw_event({**row, "source": row.get("source", "zigbee")})


class CaptureRawEventSource(StreamingEventSource):
    def __init__(
        self,
        source: StreamingEventSource,
        capture_path: Path | str,
        append: bool = False,
        flush_every_records: int = 1,
    ) -> None:
        self.source = source
        self.capture_path = Path(capture_path)
        self.append = append
        self.flush_every_records = max(int(flush_every_records), 1)

    def stream(self, context: PipelineContext):
        handle = None
        try:
            writer = None
            dirty_count = 0
            for event in self.source.stream(context):
                if handle is None:
                    self.capture_path.parent.mkdir(parents=True, exist_ok=True)
                    mode = "a" if self.append else "w"
                    write_header = (
                        not self.append
                        or not self.capture_path.exists()
                        or self.capture_path.stat().st_size == 0
                    )
                    handle = self.capture_path.open(mode, newline="", encoding="utf-8")
                    writer = csv.DictWriter(handle, fieldnames=RAW_TELEMETRY_COLUMNS)
                    if write_header:
                        writer.writeheader()
                if writer is None:
                    raise RuntimeError("Capture writer was not initialized.")
                writer.writerow(event.to_row())
                dirty_count += 1
                if dirty_count >= self.flush_every_records:
                    handle.flush()
                    dirty_count = 0
                yield event
            if handle is not None:
                handle.flush()
        finally:
            if handle is not None:
                handle.close()


class ZigbeeMqttSensorSource(StreamingEventSource):
    def __init__(
        self,
        client_factory=None,
        now_provider=None,
        sleep_fn=None,
        event_callback: Callable[[RawTelemetryEvent], None] | None = None,
        debug_log_fn: Callable[[str], None] | None = None,
    ) -> None:
        self.client_factory = client_factory or self._build_client
        self.now_provider = now_provider or __import__("datetime").datetime.now
        self.sleep_fn = sleep_fn or time.sleep
        self.event_callback = event_callback
        self.debug_log_fn = debug_log_fn

    def stream(self, context: PipelineContext):
        config = context.config["zigbee_gateway"]
        queue: Queue[RawTelemetryEvent] = Queue()
        client = self.client_factory(config)
        configure_mqtt_client_security(client, config)
        connected = {"value": False}
        connection_failed = {"value": None}

        def on_connect(client_obj, userdata, flags, reason_code, properties=None):
            if not _mqtt_reason_code_is_success(reason_code):
                connection_failed["value"] = RuntimeError(
                    f"MQTT connection failed with reason code {_format_mqtt_reason_code(reason_code)}."
                )
                return
            client_obj.subscribe(config["topic"])
            connected["value"] = True

        def on_disconnect(client_obj, userdata, disconnect_flags, reason_code, properties=None):
            connected["value"] = False
            if not _mqtt_reason_code_is_success(reason_code) and self.debug_log_fn is not None:
                self.debug_log_fn(
                    "[zigbee_mqtt] disconnected from broker; reconnecting "
                    f"(reason_code={_format_mqtt_reason_code(reason_code)})"
                )

        def on_message(client_obj, userdata, message):
            try:
                rows = decode_mqtt_message(
                    topic=message.topic,
                    payload=message.payload,
                    received_at=self.now_provider(),
                    topic_prefix=config["topic_prefix"],
                    allow_bridge_debug=bool(config.get("debug_bridge_messages", False)),
                    debug_log_fn=self.debug_log_fn,
                    supported_fields=set(
                        config.get("supported_fields", DEFAULT_ZIGBEE_TELEMETRY_FIELDS)
                    ),
                    include_unsupported_fields=bool(
                        config.get("include_unsupported_fields", False)
                    ),
                )
            except Exception as exc:
                if self.debug_log_fn is not None:
                    self.debug_log_fn(
                        f"[zigbee_mqtt] failed to decode topic {message.topic}: {exc}"
                    )
                return

            for row in rows:
                event = ensure_raw_event({**row, "source": "zigbee_mqtt"})
                if self.event_callback is not None:
                    self.event_callback(event)
                queue.put(event)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        reconnect_delay_set = getattr(client, "reconnect_delay_set", None)
        if callable(reconnect_delay_set):
            reconnect_delay_set(
                min_delay=max(1, int(config.get("reconnect_min_delay_seconds", 1))),
                max_delay=max(1, int(config.get("reconnect_max_delay_seconds", 30))),
            )
        client.connect(
            config["host"],
            int(config["port"]),
            int(config.get("keepalive_seconds", 60)),
        )
        client.loop_start()
        listen_seconds = int(config["listen_seconds"])
        deadline = (
            None
            if listen_seconds < 0
            else time.monotonic() + max(0, listen_seconds)
        )
        connect_deadline = time.monotonic() + max(
            1, int(config.get("connect_timeout_seconds", 10))
        )
        try:
            while not connected["value"] and time.monotonic() < connect_deadline:
                if connection_failed["value"] is not None:
                    raise connection_failed["value"]
                self.sleep_fn(0.05)
            if not connected["value"]:
                raise TimeoutError(
                    f"Timed out connecting to MQTT broker {config['host']}:{config['port']}."
                )
            while deadline is None or time.monotonic() < deadline:
                try:
                    yield queue.get(timeout=0.1)
                except Empty:
                    continue
            while not queue.empty():
                yield queue.get_nowait()
        finally:
            client.loop_stop()
            client.disconnect()

    @staticmethod
    def _build_client(config: dict[str, Any]):
        try:
            from paho.mqtt import client as mqtt
        except ImportError as exc:
            raise RuntimeError(
                "paho-mqtt is required for the local Zigbee MQTT gateway adapter."
            ) from exc

        return mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=config.get("client_id") or "hyfhenet-zigbee-gateway",
            transport=config.get("transport", "tcp"),
        )

class ZigbeeMqttControlClient:
    def __init__(self, client_factory=None) -> None:
        self.client_factory = client_factory or ZigbeeMqttSensorSource._build_client

    def publish_json(
        self,
        config: dict[str, Any],
        topic: str,
        payload: dict[str, Any],
    ) -> None:
        client = self.client_factory(config)
        configure_mqtt_client_security(client, config)
        client.connect(
            config["host"],
            int(config["port"]),
            int(config.get("keepalive_seconds", 60)),
        )
        client.loop_start()
        try:
            client.publish(topic, json.dumps(payload))
        finally:
            client.loop_stop()
            client.disconnect()


def set_plug_state(
    device_name: str,
    state: Literal["ON", "OFF"],
    config: dict[str, Any] | None = None,
    client_factory=None,
) -> None:
    normalized_state = state.upper()
    if normalized_state not in {"ON", "OFF"}:
        raise ValueError("Plug state must be 'ON' or 'OFF'.")
    mqtt_config = dict(config or {})
    topic_prefix = mqtt_config.get("topic_prefix", "zigbee2mqtt/")
    topic = f"{topic_prefix}{device_name}/set"
    ZigbeeMqttControlClient(client_factory=client_factory).publish_json(
        {
            "host": mqtt_config.get("host", "localhost"),
            "port": mqtt_config.get("port", 1883),
            "username": mqtt_config.get("username"),
            "password": mqtt_config.get("password"),
            "transport": mqtt_config.get("transport", "tcp"),
            "client_id": mqtt_config.get("client_id") or "hyfhenet-zigbee-control",
            "keepalive_seconds": mqtt_config.get("keepalive_seconds", 60),
            "tls_enabled": mqtt_config.get("tls_enabled", False),
            "ca_cert_path": mqtt_config.get("ca_cert_path"),
            "client_cert_path": mqtt_config.get("client_cert_path"),
            "client_key_path": mqtt_config.get("client_key_path"),
            "tls_insecure": mqtt_config.get("tls_insecure", False),
        },
        topic,
        {"state": normalized_state},
    )


def permit_join(
    enabled: bool,
    duration_seconds: int = 120,
    config: dict[str, Any] | None = None,
    client_factory=None,
) -> None:
    mqtt_config = dict(config or {})
    topic_prefix = mqtt_config.get("topic_prefix", "zigbee2mqtt/")
    topic = f"{topic_prefix}bridge/request/permit_join"
    payload = {"value": bool(enabled), "time": max(int(duration_seconds), 0)}
    ZigbeeMqttControlClient(client_factory=client_factory).publish_json(
        {
            "host": mqtt_config.get("host", "localhost"),
            "port": mqtt_config.get("port", 1883),
            "username": mqtt_config.get("username"),
            "password": mqtt_config.get("password"),
            "transport": mqtt_config.get("transport", "tcp"),
            "client_id": mqtt_config.get("client_id") or "hyfhenet-zigbee-control",
            "keepalive_seconds": mqtt_config.get("keepalive_seconds", 60),
            "tls_enabled": mqtt_config.get("tls_enabled", False),
            "ca_cert_path": mqtt_config.get("ca_cert_path"),
            "client_cert_path": mqtt_config.get("client_cert_path"),
            "client_key_path": mqtt_config.get("client_key_path"),
            "tls_insecure": mqtt_config.get("tls_insecure", False),
        },
        topic,
        payload,
    )


def configure_mqtt_client_security(client, config: dict[str, Any]) -> None:
    if config.get("username"):
        client.username_pw_set(config["username"], config.get("password"))
    if not bool(config.get("tls_enabled", False)):
        return

    tls_set = getattr(client, "tls_set", None)
    if not callable(tls_set):
        raise RuntimeError("Configured MQTTS but the MQTT client does not support TLS.")
    tls_set(
        ca_certs=config.get("ca_cert_path") or None,
        certfile=config.get("client_cert_path") or None,
        keyfile=config.get("client_key_path") or None,
    )
    tls_insecure_set = getattr(client, "tls_insecure_set", None)
    if callable(tls_insecure_set):
        tls_insecure_set(bool(config.get("tls_insecure", False)))


class TimestampPacedStreamSource(StreamingEventSource):
    def __init__(
        self,
        source: StreamingEventSource,
        observer: StreamingObserver | None = None,
        sleep_fn=None,
    ) -> None:
        self.source = source
        self.observer = observer or NullStreamingObserver()
        self.sleep_fn = sleep_fn or time.sleep

    def stream(self, context: PipelineContext):
        config = context.config["gateway_stream"]
        follow_event_timing = bool(config.get("follow_event_timing", False))
        speed_multiplier = float(config.get("replay_speed_multiplier", 1.0) or 1.0)
        max_sleep = config.get("max_replay_sleep_seconds")
        previous_timestamp = None
        previous_event = None

        for event in self.source.stream(context):
            if follow_event_timing and previous_timestamp is not None:
                delay_seconds = (event.timestamp - previous_timestamp).total_seconds()
                if delay_seconds > 0:
                    adjusted_delay = delay_seconds / max(speed_multiplier, 0.0001)
                    if max_sleep is not None:
                        adjusted_delay = min(adjusted_delay, float(max_sleep))
                    if adjusted_delay > 0:
                        self.observer.on_stream_wait(
                            adjusted_delay,
                            previous_event,
                            event,
                            context,
                        )
                        self.sleep_fn(adjusted_delay)
            yield event
            previous_timestamp = event.timestamp
            previous_event = event


StreamSourceFactory = Callable[[], StreamingEventSource]


def get_zigbee_stream_source_factories() -> dict[str, StreamSourceFactory]:
    return {
        "replay": ZigbeeCsvReplaySource,
        "zigbee_mqtt": ZigbeeMqttSensorSource,
    }


def build_zigbee_stream_source(mode: str) -> StreamingEventSource:
    factories = get_zigbee_stream_source_factories()
    factory = factories.get(mode)
    if factory is None:
        supported = ", ".join(sorted(factories))
        raise ValueError(
            f"Unsupported zigbee source mode '{mode}'. Supported modes: {supported}"
        )
    return factory()

def _device_id_from_topic(topic: str, topic_prefix: str) -> str | None:
    if topic_prefix and topic.startswith(topic_prefix):
        remainder = topic[len(topic_prefix) :]
    else:
        parts = topic.split("/", 1)
        remainder = parts[1] if len(parts) == 2 else parts[0]
    device_id = remainder.split("/", 1)[0].strip()
    return device_id or None


def _coerce_timestamp(raw_timestamp: Any, received_at: datetime) -> str:
    if isinstance(raw_timestamp, str) and raw_timestamp.strip():
        candidate = raw_timestamp.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed.strftime(TIMESTAMP_FORMAT)
        except ValueError:
            for fmt in (TIMESTAMP_FORMAT, "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(candidate, fmt).strftime(TIMESTAMP_FORMAT)
                except ValueError:
                    continue
    return received_at.strftime(TIMESTAMP_FORMAT)


def _scalar_to_string(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _mqtt_reason_code_is_success(reason_code: Any) -> bool:
    is_failure = getattr(reason_code, "is_failure", None)
    if isinstance(is_failure, bool):
        return not is_failure
    value = getattr(reason_code, "value", reason_code)
    try:
        return int(value) == 0
    except (TypeError, ValueError):
        return str(reason_code).lower() in {"success", "normal disconnection"}


def _format_mqtt_reason_code(reason_code: Any) -> str:
    name = getattr(reason_code, "name", None)
    value = getattr(reason_code, "value", None)
    if name is not None and value is not None:
        return f"{name}({value})"
    return str(reason_code)
