# HyFHE-Net Edge

Pilot 1.2 edge gateway for residential energy analytics.

This repository contains the edge gateway runtime: Zigbee2MQTT/replay ingestion, normalization, snapshots, rolling features, event gates, local edge services, a one-minute local forecast, edge-side FHE client calls for `forecast`, `nilm`, and `cohort`, Docker deployment files, and report generation.

Cloud API serving, FHE model training, compiled server artifacts, model registries, and cloud datasets live in the separate cloud/FHE repositories.

The live gateway can also consume EDF service SDK `DeviceApi` events via `edf-sdk-live`. This mode uses `DeviceApi.from_env()`, listens to `TEMPERATURE,POWER,APPARENT_POWER,METER_INDEXES` by default, and feeds the same preprocessing and inference stages as the Zigbee2MQTT source. The private SDK is optional for normal replay/MQTT runs and must be installed only in environments that use this mode.

The FHE client auto-detects the local CPU architecture, normalizes it to the backend bundle names `x86_64` or `aarch64`, sends it as `X-Architecture`, and keeps downloaded client packages under an architecture-specific cache path. Set `HYFHENET_FHE_ARCHITECTURE` only when a test environment needs to override auto-detection.

## Quick Deploy

For MiniPC or Raspberry Pi deployment, use [documentation/deployment.md](documentation/deployment.md).

```bash
cp .env.example .env
nano .env
docker compose build gateway
docker compose up -d gateway
```

Capture device evidence and regenerate the report:

```bash
docker compose --profile capture run --rm capture
docker compose --profile report run --rm report
```

The Compose services use separate default image names for gateway/capture/report. Keep
`HYFHENET_GATEWAY_INSTALL_FHE_DEPS=false` for the lightweight live gateway, and set
`HYFHENET_REPORT_INSTALL_FHE_DEPS=true` only when the report image must run the full
Concrete/FHE client.

To generate processing/inference evidence without fitting report-local models:

```bash
HYFHENET_REPORT_SKIP_TRAINING=true docker compose --profile report run --rm report
```

## Local Checks

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe main.py stream-replay --input data\zigbee_mqtt_capture.csv --output artifacts\gateway_stream --as-fast-as-possible
.\.venv\Scripts\python.exe main.py edf-sdk-live --output artifacts\gateway_edf_sdk_live --edf-streams TEMPERATURE,POWER,APPARENT_POWER,METER_INDEXES
```

Install `requirements-fhe.txt` only when testing the optional full FHE client stack outside Docker.
For slow live cloud calls, set `HYFHENET_FHE_ASYNC=true` and optionally
`HYFHENET_FHE_SAMPLE_INTERVAL_SECONDS=60`. For report runs, keep async disabled and use
`HYFHENET_REPORT_FHE_SAMPLE_INTERVAL_SECONDS` plus `HYFHENET_REPORT_FHE_TASKS` when only
selected FHE tasks are needed.

## Key Paths

| Path | Purpose |
|---|---|
| `Dockerfile`, `docker-compose.yml`, `.env.example` | Edge deployment |
| `configs/pilot_1_2_edge.json` | Sensor roles, stage order, thresholds, FHE settings |
| `data/zigbee_mqtt_capture.csv` | Replay sample used by tests and smoke runs |
| `models/edge_load_forecast_ridge.json` | Local one-minute forecast model |
| `scripts/prepare_edge_report.py` | Device-specific report generator |
| `artifacts/` | Local generated runs/reports; ignored by Git |

## Docs

- [architecture.md](documentation/architecture.md): edge/cloud boundary and module map
- [deployment.md](documentation/deployment.md): MiniPC and Raspberry Pi deployment
- [qemu_fhe_runbook.md](documentation/qemu_fhe_runbook.md): bare-metal QEMU x86_64 emulation for FHE on Raspberry Pi
- [edge.md](documentation/edge.md): pipeline stages and run artifacts
- [edge_cloud_partitioning_strategy.md](documentation/edge_cloud_partitioning_strategy.md): measured partitioning, cloud datasets, FHE contracts, and model-selection guidance
- [reference/](documentation/reference/): external pilot reference material, not needed for deployment
