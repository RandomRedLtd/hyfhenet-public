from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hyfhenet.ingestion.normalization import normalize_raw_row
from hyfhenet.runtime.stream import (
    build_gateway_event_source,
    build_gateway_stream_context,
    run_gateway_zigbee_mqtt_live,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read normalized Zigbee2MQTT measurements from the local broker."
    )
    parser.add_argument("--config", type=Path, default=Path("configs/pilot_1_2_edge.json"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/read_zigbee"))
    parser.add_argument("--zigbee-host", default="localhost")
    parser.add_argument("--zigbee-port", type=int, default=1883)
    parser.add_argument("--zigbee-topic", default="zigbee2mqtt/#")
    parser.add_argument("--zigbee-topic-prefix", default="zigbee2mqtt/")
    parser.add_argument("--zigbee-listen-seconds", type=int, default=60)
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Read until interrupted instead of stopping after --zigbee-listen-seconds.",
    )
    parser.add_argument("--debug-bridge", action="store_true")
    parser.add_argument(
        "--run-pipeline",
        action="store_true",
        help="Forward the live MQTT stream into the existing edge preprocessing and inference pipeline.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.run_pipeline:
        summary = run_gateway_zigbee_mqtt_live(
            output_dir=args.output,
            config_path=args.config,
            zigbee_host=args.zigbee_host,
            zigbee_port=args.zigbee_port,
            zigbee_topic=args.zigbee_topic,
            zigbee_topic_prefix=args.zigbee_topic_prefix,
            zigbee_listen_seconds=-1 if args.continuous else args.zigbee_listen_seconds,
            console_log_level="progress",
        )
        print(json.dumps(summary, indent=2))
        return 0

    context = build_gateway_stream_context(
        input_path=f"zigbee-mqtt://{args.zigbee_host}",
        output_dir=args.output,
        config_path=args.config,
        input_source="zigbee_mqtt",
        zigbee_source="zigbee_mqtt",
        zigbee_gateway_overrides={
            "host": args.zigbee_host,
            "port": args.zigbee_port,
            "topic": args.zigbee_topic,
            "topic_prefix": args.zigbee_topic_prefix,
            "listen_seconds": -1 if args.continuous else args.zigbee_listen_seconds,
            "debug_bridge_messages": args.debug_bridge,
        },
    )

    for event in build_gateway_event_source(context).stream(context):
        normalized = normalize_raw_row(event, context.config["devices"])
        record = normalized.to_record()
        record["device_name"] = normalized.device_name
        record["mqtt_device_name"] = event.metadata.get("device_name", event.device)
        record["raw_payload"] = event.metadata.get("raw_payload")
        record["mqtt_topic"] = event.metadata.get("mqtt_topic")
        print(json.dumps(record, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
