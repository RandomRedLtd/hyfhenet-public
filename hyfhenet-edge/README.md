# HyFHE-Net Edge

Pilot 1.2 edge gateway for residential energy analytics.

This repository contains the edge gateway runtime: Zigbee2MQTT/replay ingestion, normalization, snapshots, rolling features, event gates, local edge services, a one-minute local forecast, edge-side FHE client calls for `forecast`, `nilm`, and `cohort`, Docker deployment files, and report generation.

Cloud API serving, FHE model training, compiled server artifacts, model registries, and cloud datasets live in the separate cloud/FHE repositories.

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
```

Install `requirements-fhe.txt` only when testing the optional full FHE client stack outside Docker.

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
- [edge.md](documentation/edge.md): pipeline stages and run artifacts
- [edge_cloud_partitioning_strategy.md](documentation/edge_cloud_partitioning_strategy.md): measured partitioning, cloud datasets, FHE contracts, and model-selection guidance
- [reference/](documentation/reference/): external pilot reference material, not needed for deployment
