# Deployment Guide

Use this guide when moving the gateway to a MiniPC first and then to a Raspberry Pi. The same Docker image supports the live gateway, MQTT capture, benchmark generation, and report regeneration.

## What Runs On The Edge Device

- `gateway`: continuous Zigbee2MQTT ingestion and edge pipeline execution
- `capture`: fixed-window MQTT capture plus an optional live pipeline run
- `report`: device-specific report regeneration from the capture CSV

The report is regenerated on the device that runs it. Hostname, platform, CPU count, configured device roles, latency, throughput, forecast quality, FHE status, and benchmark data all come from that device run.

## Prerequisites

- MiniPC with x86_64 Linux, or Raspberry Pi 4/5 with a 64-bit OS
- Docker Engine with the Docker Compose plugin
- Network access to the MQTT broker used by Zigbee2MQTT
- This repository copied or cloned onto the edge device
- Optional: remote FHE API URL/key and client certificates for encrypted cloud inference
- Optional for French virtual-datalogger runs: the private `service_sdk_python` package and the environment variables required by `DeviceApi.from_env()`

The default gateway image is the recommended first deployment target. It runs the full local gateway and records remote FHE outputs as `unavailable` until FHE credentials and client dependencies are installed. The report service uses a separate image name so optional FHE/report dependencies do not overwrite the lightweight live-gateway image.

Remote FHE API URLs must use HTTPS by default. Plain HTTP is rejected unless `HYFHENET_FHE_ALLOW_INSECURE_HTTP=true` is set for isolated lab testing.

## Quick New-Device Flow

1. Install Docker Engine and the Docker Compose plugin.
2. Clone the repo: `git clone https://github.com/RandomRedLtd/hyfhenet-public.git && cd hyfhenet-public/hyfhenet-edge`.
3. Create config: `cp .env.example .env`.
4. Edit `.env`: set `HYFHENET_ZIGBEE_HOST`, MQTT credentials/TLS if needed, `FHE_URL=https://hyfhe.net`, `FHE_API=<api-key>`, and `HYFHENET_REPORT_VIDEOS=false`.
5. Build the lightweight gateway: `docker compose build gateway`.
6. Start live gateway: `docker compose up -d gateway`.
7. Capture evidence: `docker compose --profile capture run --rm capture`.
8. Build full report image only if remote FHE dependencies are needed: `HYFHENET_REPORT_INSTALL_FHE_DEPS=true docker compose build report`.
9. Run report: `docker compose --profile report run --rm report`.
10. Review `artifacts/edge_report/README.md`, `manifest.json`, `showcase.html`, and `run/latency.csv`.

## 1. Prepare The Device

On the edge device:

```bash
git clone https://github.com/RandomRedLtd/hyfhenet-public.git
cd hyfhenet-public/hyfhenet-edge
cp .env.example .env
nano .env
```

If the repository is copied by USB or SCP instead of cloned, run the same `cp .env.example .env` step from the copied project directory.

Edit at least:

```text
HYFHENET_ZIGBEE_HOST=<mqtt-broker-host-or-ip>
HYFHENET_ZIGBEE_PORT=1883
HYFHENET_ZIGBEE_TOPIC=zigbee2mqtt/#
HYFHENET_ZIGBEE_TOPIC_PREFIX=zigbee2mqtt/
HYFHENET_ZIGBEE_LISTEN_SECONDS=600
```

MQTT host guidance:

- Broker on the same Docker Compose network: use the broker service name, for example `mosquitto`.
- Broker on the same Linux host: use the host LAN IP, or keep `host.docker.internal` with the `host-gateway` mapping already present in `docker-compose.yml`.
- Broker on another device: use that device's LAN IP or DNS name.
- Docker Desktop: `host.docker.internal` is usually correct.

## 2. Deploy On A MiniPC

Build and start the live gateway:

```bash
docker compose build gateway
docker compose up -d gateway
docker compose logs -f gateway
```

Check the output directory on the host:

```bash
ls -la artifacts/gateway_mqtt_live
```

Stop the live service:

```bash
docker compose stop gateway
```

## 3. Deploy On Raspberry Pi

Use a 64-bit OS. Confirm the architecture:

```bash
uname -m
```

Expected output is `aarch64` or `arm64`. If the device reports `armv7l`, install a 64-bit Raspberry Pi OS before deploying.

Install Docker if needed:

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"
newgrp docker
docker compose version
```

Then run the same Compose flow:

```bash
cp .env.example .env
nano .env
docker compose build gateway
docker compose up -d gateway
docker compose logs -f gateway
```

For bare-metal FHE without Docker, see [qemu_fhe_runbook.md](qemu_fhe_runbook.md).

The default Docker base image and Debian packages are multi-architecture. Start with the default image on Raspberry Pi; optional full FHE-client dependencies are platform-sensitive and should be validated separately on the target OS.

For remote FHE calls, the edge client automatically sends `X-Architecture: aarch64` on 64-bit Raspberry Pi and `X-Architecture: x86_64` on MiniPC Linux. Leave `HYFHENET_FHE_ARCHITECTURE` blank unless you are deliberately overriding a lab run.

## 4. EDF Service SDK Mode

Use this mode when the edge gateway must consume the French virtual datalogger through the EDF SDK instead of local MQTT/Zigbee devices:

```text
DATALOGGER_GATEWAY_URL=http://localhost:8080
DATALOGGER_GATEWAY_TOKEN=<application-or-service-token>
```

```bash
python main.py edf-sdk-live --output artifacts/gateway_edf_sdk_live --continuous --log-level progress
```

The default SDK streams are `TEMPERATURE,POWER,APPARENT_POWER,METER_INDEXES`. `POWER` is treated as plug power, while the wiki-documented Lixee meter streams `APPARENT_POWER` and `METER_INDEXES` are treated as household meter power and energy. If EDF confirms that `POWER` is aggregate meter consumption too, set:

```text
HYFHENET_EDF_SDK_STREAM_FIELD_MAP=POWER=household_power_w
HYFHENET_EDF_SDK_DEFAULT_DEVICE_ROLE=household_meter
```

For Docker runs, build the gateway with the private SDK only when this mode is needed. The SDK currently declares Python 3.13+, so set `HYFHENET_PYTHON_IMAGE=python:3.13-slim-bookworm` for EDF SDK gateway builds. Set `HYFHENET_EDF_SERVICE_SDK_PACKAGE` to the package name, private index package, or authenticated Git URL appropriate for your environment:

```bash
HYFHENET_PYTHON_IMAGE=python:3.13-slim-bookworm \
HYFHENET_GATEWAY_INSTALL_EDF_SERVICE_SDK=true docker compose build gateway
docker compose run --rm gateway edf-sdk-live --output artifacts/gateway_edf_sdk_live --continuous --log-level progress
```

## 5. Capture Device Evidence

Capture a fixed MQTT window and run the pipeline against the same data:

```bash
docker compose --profile capture run --rm capture
```

This writes:

```text
artifacts/zigbee_mqtt_capture.csv
artifacts/gateway_mqtt_capture/
```

Increase `HYFHENET_ZIGBEE_LISTEN_SECONDS` in `.env` when you need a longer evidence window.

## 6. Regenerate The Report On The Device

Run:

```bash
docker compose --profile report run --rm report
```

The default report replay interval is 5 seconds. Keep `HYFHENET_REPORT_INTERVAL_SECONDS=5` for short captures; increase it only when the capture window still leaves at least 100 latency samples.

When the full FHE client image is connected to the cloud, set `HYFHENET_REPORT_FHE_SAMPLE_INTERVAL_SECONDS` to bound remote FHE calls during report replay. `60` is a useful default for normal evidence runs; use a larger value such as `300` for first-device smoke reports or slow networks.

When a report only needs selected encrypted outputs, set `HYFHENET_REPORT_FHE_TASKS=forecast`, `forecast,nilm`, `cohort`, `all`, or `none`.

The report command now prints `[report] ...` progress lines and updates `artifacts/edge_report/_status.json` while it runs. On slower edge devices, skip MP4 rendering when you only need the evidence files:

```bash
HYFHENET_REPORT_VIDEOS=false docker compose --profile report run --rm report
```

Review the generated package:

```text
artifacts/edge_report/README.md
artifacts/edge_report/manifest.json
artifacts/edge_report/run/performance_metrics.json
artifacts/edge_report/run/cloud_forecast_features.csv
artifacts/edge_report/run/cloud_nilm_features.csv
artifacts/edge_report/run/cloud_cohort_features.csv
artifacts/edge_report/run/cloud_model_feature_contracts.json
artifacts/edge_report/benchmark/summary.json
artifacts/edge_report/model/model_comparison.csv
```

The report is generated output and is intentionally ignored by Git.

## 7. Deployment Evidence Targets

Use these as practical checks when generating MiniPC and Raspberry Pi evidence. They are review thresholds, not production SLAs.

| Check | Target | Evidence |
|---|---:|---|
| Inference cycles in a report run | at least 100 | `run/latency.csv`, `manifest.json` |
| MiniPC p95 total tick latency | 250 ms or lower | `run/latency_summary.json` |
| Raspberry Pi p95 total tick latency | 500 ms or lower | `run/latency_summary.json` |
| Local model p95 stage latency | 50 ms or lower | `run/latency_summary.json` |
| Cloud feature-prep p95 latency | 50 ms or lower | `run/latency_summary.json` |
| Replay benchmark throughput | at least 100 raw events/s | `benchmark/summary.json` |
| FHE transport | HTTPS by default; HTTP only with lab override | `.env`, `hyfhenet/fhe/client.py` |
| FHE model package handling | unsafe ZIP paths rejected | unit tests |

Generate final review evidence from a real capture with `HYFHENET_REPORT_SOURCE=input`. Use synthetic report data only for demonstrations.

## 8. Optional Full FHE Client Image

Set FHE API values in `.env`:

```text
HYFHENET_FHE_API_URL=https://fhe.example
HYFHENET_FHE_API_KEY=<device-api-key>
HYFHENET_FHE_CLIENT_CERT=
HYFHENET_FHE_CLIENT_KEY=
HYFHENET_FHE_CA_BUNDLE=
HYFHENET_FHE_ARCHITECTURE=
HYFHENET_FHE_ALLOW_INSECURE_HTTP=false
```

Build only the report image with optional FHE dependencies when encrypted cloud evidence is needed:

```bash
HYFHENET_REPORT_INSTALL_FHE_DEPS=true docker compose build report
```

On Windows PowerShell:

```powershell
$env:HYFHENET_REPORT_INSTALL_FHE_DEPS="true"
docker compose build report
```

For live MQTT runs against slow remote FHE, keep report mode synchronous but enable non-blocking dispatch on the gateway with `HYFHENET_FHE_ASYNC=true`. Use `HYFHENET_FHE_MAX_PENDING_REQUESTS=1` to prevent backlogs and `HYFHENET_FHE_SAMPLE_INTERVAL_SECONDS=60` to cap call frequency.

If the optional dependency build is not available for the target architecture, keep the default image and verify local gateway behavior, capture, report generation, and the explicit `unavailable` FHE status.

## 9. Useful Validation Commands

Replay the included sample without MQTT:

```bash
docker compose run --rm gateway stream-replay --input data/zigbee_mqtt_capture.csv --output artifacts/gateway_stream --as-fast-as-possible --log-level none
```

Run the benchmark:

```bash
docker compose run --rm gateway benchmark-pipeline --input data/zigbee_mqtt_capture.csv --output artifacts/edge_benchmark --runs 4 --log-level none
```

Run the Python tests outside Docker:

```bash
python -m unittest discover -s tests -v
```

## Troubleshooting

- Docker daemon unavailable: run `docker info` and restart Docker before building.
- MQTT connection refused: verify broker IP, port, topic prefix, username/password, TLS settings, and firewall rules.
- Empty capture: confirm Zigbee2MQTT is publishing under `HYFHENET_ZIGBEE_TOPIC`.
- Permission errors in mounted folders: run `mkdir -p artifacts .hyfhenet-cache` and adjust ownership with `sudo chown -R "$USER":"$USER" artifacts .hyfhenet-cache`.
- FHE status is `unavailable`: set FHE API credentials and build with optional dependencies, or treat this as the expected default-image behavior.
