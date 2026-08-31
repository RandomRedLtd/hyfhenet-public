# Edge-Cloud Partitioning Strategy

This document is the single source for the edge/cloud split, cloud model datasets, FHE client contract, and model-selection evidence.

Current measurements come from:

```powershell
.\.venv\Scripts\python.exe main.py prepare-edge-report --source input --input data\zigbee_mqtt_capture.csv --config configs\pilot_1_2_edge.json --output artifacts\edge_report
```

These are workstation measurements from the checked-in capture. Regenerate the report on the MiniPC and Raspberry Pi after live capture; those device-local reports replace the baseline below.

Cloud dataset and training inventory comes from `https://github.com/RandomRedLtd/hyfhenet-fhe` at inspected head `1db2e8b`. FHE API behavior was checked against `https://github.com/RandomRedLtd/hyfhenet-backend` at inspected head `8f0bf2c` and `https://github.com/RandomRedLtd/hyfhenet-fhe-client` at inspected head `0259a5d`. Cloud API serving, compiled FHE server packages, model registries, model-selection reports, and cloud datasets stay outside this edge repository.

## Current Evidence

| Metric | Current Value | Source |
|---|---:|---|
| Configured devices | 3 | `manifest.json` |
| Raw telemetry events | 2,455 | `run_summary.json` |
| Normalized events | 2,455 | `run_summary.json` |
| Inference cycles / snapshots | 180 | `run_summary.json` |
| Avg raw events per snapshot | 13.638889 | `performance_metrics.json` |
| Forecast feature rows | 180 | `cloud_forecast_features.csv` |
| NILM feature rows | 180 | `cloud_nilm_features.csv` |
| Cohort feature rows | 180 | `cloud_cohort_features.csv` |
| Per-model row reduction before cloud handoff | 92.7% | 2,455 raw rows -> 180 feature rows |
| Latency samples | 180 | `latency_summary.json` |
| Avg / p95 / p99 total tick latency | 10.322 / 14.344 / 15.630 ms | `latency_summary.json` |
| Benchmark runs | 4 | `benchmark/summary.json` |
| Benchmark mean p95 tick latency | 13.878 ms | `benchmark/summary.json` |
| Benchmark mean throughput | 1,182.652750 raw events/s | `benchmark/summary.json` |
| Model results written | 900 | `run_summary.json` |
| FHE unavailable rows | 540 total, 180 per cloud model | `model_result_summary` |
| Edge forecast evaluations | 168 | `forecast_quality_summary` |

## Cloud Datasets

The cloud datasets belong in `hyfhenet-fhe/datasets/`; they are not cloned or copied into the edge gateway.

| Cloud Task | Dataset | Rows | Size | Current Training Model | Target |
|---|---|---:|---:|---|---|
| Long-horizon forecast | `datasets/forecast.csv` | 59,520 | 15.2 MB | Concrete-ML `RandomForestRegressor(max_depth=6, n_estimators=8)` | `target_household_power_w` |
| NILM disaggregation | `datasets/nilm.csv` | 32,256 | 8.0 MB | Concrete-ML `RandomForestRegressor(max_depth=6, n_estimators=8)` | plug 1, plug 2, HVAC, baseload, other power targets |
| Cohort benchmarking | `datasets/cohort.csv` | 336 | 61 KB | Concrete-ML `DecisionTreeRegressor()` | `cohort_label_code` |

`cohort_label` is label metadata, not an inference input. The cloud training script now drops it before preprocessing in `hyfhenet-fhe` commit `a65749d`.

## Runtime Contracts

The edge builds and records the plaintext feature rows before encryption:

| Model | Edge Feature File | Contract Source | Client Method | Remote Endpoint |
|---|---|---|---|---|
| `forecast` | `cloud_forecast_features.csv` | `FheForecastFeaturePreparer`, `FORECAST_INPUT_COLUMNS` | `HyfhenetFheClient.forecast()` | `/api/fhe/forecast/inference` |
| `nilm` | `cloud_nilm_features.csv` | `FheNilmFeaturePreparer`, `NILM_INPUT_COLUMNS` | `HyfhenetFheClient.nilm()` | `/api/fhe/nilm/inference` |
| `cohort` | `cloud_cohort_features.csv` | `FheCohortFeaturePreparer`, `COHORT_INPUT_COLUMNS` | `HyfhenetFheClient.cohort()` | `/api/fhe/cohort/inference` |

Each report also writes:

```text
artifacts/edge_report/run/cloud_forecast_feature_contract.json
artifacts/edge_report/run/cloud_model_feature_contracts.json
```

The backend exposes:

```text
GET  /api/fhe/{forecast|nilm|cohort}/client-files
POST /api/fhe/{forecast|nilm|cohort}/evaluation-key
POST /api/fhe/{forecast|nilm|cohort}/inference
```

The edge client downloads model client files, safely extracts them, creates/uploads evaluation keys, encrypts locally, posts encrypted payloads, and decrypts returned ciphertext locally. HTTPS is required by default. Set `HYFHENET_FHE_ALLOW_INSECURE_HTTP=true` only for isolated lab tests without production credentials or private data.

The current backend serves architecture-specific model bundles. The edge client sends `X-Architecture` on client-file downloads and encrypted inference requests, using `x86_64` for MiniPC Linux and `aarch64` for 64-bit Raspberry Pi. Override with `HYFHENET_FHE_ARCHITECTURE` only when auto-detection is wrong; the client cache stores model files and keys under architecture-specific folders.

Live edge gateways can set `HYFHENET_FHE_ASYNC=true` to keep local ticks non-blocking while remote FHE runs in a bounded background worker. Report evidence remains synchronous by default so measured FHE latency samples stay tied to explicit report samples.

If credentials, dependencies, connectivity, model files, or remote inference are unavailable, the gateway writes one `unavailable` result per configured cloud model and continues local processing.

## Selection Rules

| Rule | Placement Effect |
|---|---|
| Needs MQTT access, device state, local control, or low-latency fallback | Edge |
| Reduces raw telemetry before any cloud call | Edge |
| Uses a fixed input contract but depends on server-side model execution | Hybrid: edge prepares/encrypts input, cloud executes model |
| Requires FHE server package, backend registry, or training dataset | Cloud/FHE |
| Must keep running without cloud credentials | Edge stage writes explicit status and continues |
| Produces hardware-specific latency/throughput evidence | Generate on the deployed edge device |

## Decision Register

| Task | Explored / Compared | Selected Placement | Evidence And Reason |
|---|---|---|---|
| MQTT ingestion and replay capture | Direct cloud MQTT subscription, raw batch upload, local gateway capture/replay | Edge | Local capture avoids exposing broker credentials and produces replay-compatible CSV evidence. The current report normalized 2,455 events with 0 quality alerts. |
| Sensor normalization and household power resolution | Forward raw fields, cloud normalization, edge normalization with meter precedence and plug fallback | Edge | Preprocessing p95 is 0.172 ms. The current capture has no household meter, so the gateway used `sum_of_configured_smart_plugs`. |
| Snapshot alignment and rolling features | Cloud feature store, edge feature windows, raw-only handoff | Edge | 2,455 raw rows become 180 per-model feature rows. Feature-stage p95 is 0.959 ms. |
| Load-event gate | Cloud event detector, local threshold gate, no event gate | Edge | Event-gate p95 is 0.034 ms. The gate is deterministic and uses local thresholds, staleness checks, cooldown, direction, and confidence. |
| Energy profile service | Cloud service, edge service, disabled service | Edge | Service p95 is 0.150 ms and remains available when FHE calls are unavailable. |
| Anomaly monitor | Cloud model, local online detector, no anomaly path | Edge | The monitor produced 180 outputs: 5 warmup, 173 ok, and 2 alerts. It uses online statistics and configured thresholds, so no cloud model registry is required. |
| One-minute load forecast | Last-value baseline, one-hour mean baseline, linear regression, Ridge variants | Edge | The runtime Ridge model produced 180 ok predictions and 168 evaluations. Local model p95 is 0.714 ms. The selected `ridge_300` model is integrated as compact JSON with 35 ordered features and one dot product per inference. |
| Long-horizon forecast | Raw cloud upload, cloud-built features, edge-built feature contract | Hybrid | The edge prepares 35 ordered forecast inputs and the cloud owns long-horizon model execution. The report writes `cloud_forecast_features.csv` and contract JSON. |
| NILM disaggregation | Edge rule split, cloud plaintext model, cloud/FHE disaggregation model | Hybrid / Cloud-FHE | The edge prepares the NILM input row, but training and evaluation need labelled appliance targets. Cloud `nilm.csv` has 32,256 rows and five target channels. |
| Cohort benchmarking | Edge daily score, backend rules, cloud/FHE model, clustering plus rule mapping | Hybrid / Cloud-FHE / backend history | The edge prepares daily aggregate inputs. Cohort definitions, cross-household history, final benchmarking, and compiled model packages stay cloud/backend-owned. |
| FHE client inference | Plain remote calls, full local model execution, FHE client plus remote FHE server | Hybrid | The edge owns feature preparation, encryption/decryption, model-client cache, and status rows. The cloud owns model packages and server inference. Default report status is explicit `unavailable` until credentials and FHE dependencies are present. |
| Model training and compiled artifacts | Store all artifacts on edge, train only remotely, split runtime model from cloud packages | Split | The edge stores only the local one-minute Ridge model and generated evidence. Cloud/FHE owns forecast/NILM/cohort training datasets, compiled FHE packages, and model-selection reports. |

## Cloud Model Selection

Cloud practitioners must compare accuracy, encrypted runtime cost, and deployment behavior before locking a model. Record dataset version, row count, split method, metrics, Concrete-ML compile status, client package size, p95 encrypted latency, and edge fallback behavior.

| Task | Minimum Baselines | Candidate Families | Required Metrics |
|---|---|---|---|
| Forecast | persistence, one-hour mean | random forest, Ridge/linear, shallow trees | MAE, RMSE, R2, p95 encrypted inference latency |
| NILM disaggregation | proportional split, always-baseload split | multi-output random forest, constrained linear model, shallow tree/forest variants supported by Concrete-ML | per-channel MAE, aggregate reconstruction error, appliance on/off recall, p95 encrypted inference latency |
| Cohort benchmarking | rule thresholds, historical majority cohort | decision tree/classifier, random forest classifier if supported within FHE budget, clustering plus rule mapping | macro-F1 or accuracy, cluster stability if unsupervised, explainability, p95 encrypted inference latency |

Keep the cloud/FHE evidence with the backend repository rather than this edge repository. At minimum, publish plaintext-vs-FHE metrics, quantization parameters, compile status, model package hashes, backend Docker evidence, and p95 encrypted latency for each model.

| Model | Dataset Rows | Plaintext Metric | FHE Metric | Absolute Deviation | Quantization | Compile Status | p95 Encrypted Latency |
|---|---:|---:|---:|---:|---|---|---:|
| forecast | 59,520 | TBD | TBD | TBD | TBD | TBD | TBD |
| nilm | 32,256 | TBD | TBD | TBD | TBD | TBD | TBD |
| cohort | 336 | TBD | TBD | TBD | TBD | TBD | TBD |

## Local Forecast Comparison

The local forecast comparison remains in the report so a deployed MiniPC or Raspberry Pi capture can challenge the selected runtime model.

| Rank | Candidate | Type | Features | Test Rows | MAE W | RMSE W | R2 | Avg Inference ms | Selected |
|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 1 | `one_hour_mean` | baseline | 1 | 9 | 1.0215 | 1.1655 | -0.0107 | 0.000650 | no |
| 2 | `ridge_300` | ridge | 35 | 9 | 1.2366 | 1.2936 | -0.2450 | 0.024301 | yes |
| 3 | `ridge_1000` | ridge | 35 | 9 | 1.0700 | 1.4050 | -0.4687 | 0.025671 | no |
| 4 | `ridge_100` | ridge | 35 | 9 | 2.2706 | 2.6125 | -4.0777 | 0.039670 | no |
| 5 | `last_value` | baseline | 1 | 9 | 2.3156 | 3.6025 | -8.6555 | 0.000649 | no |
| 6 | `ridge_10` | ridge | 35 | 9 | 5.6651 | 5.9375 | -25.2284 | 0.032013 | no |
| 7 | `linear_regression` | linear | 35 | 9 | 9.2994 | 12.2660 | -110.9346 | 0.044082 | no |

Selection note: the one-hour mean baseline is best on this short checked-in capture. `ridge_300` remains the deployed local model because it is already integrated, uses the same 35-feature contract family as the cloud forecast, and stays below 1 ms p95 in the runtime model stage. Use longer live captures on the target edge devices to retune or replace it.

## Stage Latency

| Stage | Avg ms | p95 ms | p99 ms | Placement |
|---|---:|---:|---:|---|
| Preprocessing | 0.200 | 0.172 | 0.247 | Edge |
| Feature engineering | 0.804 | 0.959 | 1.337 | Edge |
| Event gate | 0.025 | 0.034 | 0.055 | Edge |
| Cloud feature prep | 2.173 | 4.205 | 4.857 | Edge side of hybrid FHE path |
| FHE stage, default unavailable path | 6.411 | 8.343 | 8.917 | Hybrid path with edge fallback |
| Service inference | 0.113 | 0.150 | 0.219 | Edge |
| Local model stage | 0.554 | 0.714 | 0.949 | Edge |
| Total tick | 10.322 | 14.344 | 15.630 | Edge runtime plus hybrid FHE client |

The preprocessing max includes a startup outlier; p95, p99, and max stay visible in `latency_summary.json`.

## Selected Boundary

| Edge Owns | Cloud/FHE Owns |
|---|---|
| MQTT/replay ingestion and capture | API serving |
| Sensor normalization and validation | FHE server execution |
| Household meter precedence and plug fallback | Evaluation-key storage beyond local client cache |
| Snapshot alignment and rolling feature windows | Model registry and server model packages |
| Load-event gate | Forecast/NILM/cohort model training |
| Energy profile and anomaly monitor | Forecast/NILM/cohort training datasets |
| One-minute Ridge runtime model | Compiled FHE artifacts |
| Forecast, NILM, and cohort runtime feature rows | Backend persistence/history |
| FHE client encryption/decryption and local cache | Cloud model-selection reports and production model packages |
| Device-local report and benchmark package | Final cross-household cohort benchmarking |

## Regenerate On A Device

```bash
docker compose --profile capture run --rm capture
docker compose --profile report run --rm report
```

Review:

```text
artifacts/edge_report/manifest.json
artifacts/edge_report/run/cloud_model_feature_contracts.json
artifacts/edge_report/run/latency_summary.json
artifacts/edge_report/run/performance_metrics.json
artifacts/edge_report/model/model_comparison.csv
```
