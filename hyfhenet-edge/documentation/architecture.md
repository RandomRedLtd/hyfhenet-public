# Architecture

## Boundary

HyFHE-Net edge is a standalone gateway service. It owns the live Zigbee stream, local preprocessing, local edge decisions, and the FHE client request. It does not own cloud API serving, FHE training, compiled server artifacts, model registries, or cloud datasets.

The realised decision evidence, cloud dataset inventory, FHE contracts, and model-selection guidance are maintained in `edge_cloud_partitioning_strategy.md`.

| Layer | Owns | Does Not Own |
|---|---|---|
| Edge service | Zigbee2MQTT ingestion, replay capture, normalization, data-quality checks, rolling features, event gating, energy profile, anomaly monitor, one-minute Ridge forecast, FHE client encryption/decryption | backend API, FHE model training, evaluation-key persistence beyond local client cache, server model packages, cloud training datasets |
| Cloud/FHE repos | `forecast`, `nilm`, and `cohort` FHE model packages, FastAPI serving, evaluation-key storage, inference history, training datasets, model-selection evidence, and scripts | Zigbee control, local fallback logic, edge artifact writing |

## Runtime Flow

```text
Zigbee2MQTT, EDF service SDK, or replay CSV
  -> normalize telemetry
  -> update latest sensor state
  -> build aligned snapshots
  -> build rolling edge features
  -> detect load events
  -> prepare FHE input rows for forecast, NILM, and cohort endpoints
  -> encrypt and call configured remote FHE APIs
  -> decrypt returned cloud model outputs
  -> run local services and one-minute forecast
  -> write edge run artifacts
```

If FHE credentials or client dependencies are not configured, the FHE stage emits unavailable model results and the rest of the edge pipeline continues.

## Modules

| Module | Role |
|---|---|
| `hyfhenet/ingestion` | MQTT, EDF service SDK, and replay sources, raw telemetry parsing, MQTT control helpers |
| `hyfhenet/processing` | Streaming pipeline stages |
| `hyfhenet/runtime` | Orchestration, sinks, benchmarking |
| `hyfhenet/ai` | Local edge analytics package: profile service, anomaly monitor, event gate, and short-horizon forecast runner |
| `hyfhenet/fhe` | Edge-side FHE feature contracts, FHE client, remote inference tasks |
| `hyfhenet/training` | Edge-only one-minute Ridge forecast training |

## Data

Kept in git:

- `data/zigbee_mqtt_capture.csv`
- `data/edge_load_forecast_training_dataset.csv`
- `models/edge_load_forecast_ridge.json`
- `models/edge_load_forecast_training_report.md`

Run output belongs under `artifacts/`. FHE client files and evaluation keys belong in the local cache, usually `HYFHENET_FHE_CACHE_DIR` or `/app/.cache/hyfhenet` in Docker. Cache paths are architecture-specific because the backend serves separate `x86_64` and `aarch64` model bundles.
