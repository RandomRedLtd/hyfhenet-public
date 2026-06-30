# Edge Pipeline

This document describes the runtime pipeline. Use `deployment.md` for Docker deployment and `edge_cloud_partitioning_strategy.md` for partitioning, cloud datasets, and model evidence.

## Inputs

- Zigbee2MQTT telemetry from two smart plugs and one temperature/humidity sensor
- optional Linky/TIC household-meter fields: `SINSTS`, `PAPP`, `EAST`, `BASE`, `EASF01`, `household_power_w`, `household_energy_wh`
- replay CSVs with the same normalized input shape

Meter readings take precedence when present. Without a household meter:

```text
household_power_w = plug_1_power_w + plug_2_power_w
```

## Commands

| Command | Use |
|---|---|
| `stream-replay` | Run the pipeline from `data/zigbee_mqtt_capture.csv`. |
| `mqtt-live` | Subscribe to Zigbee2MQTT and run continuously or for a configured window. |
| `mqtt-capture` | Save MQTT telemetry to replay CSV, optionally running the pipeline. |
| `benchmark-pipeline` | Run repeated replay passes and write timing/count summaries. |
| `train-edge-forecast` | Train the local one-minute Ridge model. |
| `prepare-edge-report` | Build the device-specific report package. |

## Stage Order

```text
preprocessing
  -> feature_engineering
  -> event_gating
  -> cloud_forecast_prep
  -> fhe_cloud_inference
  -> service_inference
  -> ai_modeling
```

`cloud_forecast_prep` now prepares the runtime rows for all configured cloud models: long-horizon forecast, NILM disaggregation, and cohort benchmarking. `fhe_cloud_inference` encrypts the selected row with the Concrete-ML client, calls the matching `/api/fhe/{model}/inference` endpoint, decrypts the response, and writes one model result per configured cloud task. Without cloud credentials or FHE dependencies, it writes explicit `unavailable` results and the run continues.

## Run Artifacts

| File | Use |
|---|---|
| `run_summary.json` | Counts, time range, device totals, and summary metrics |
| `performance_metrics.json` | Throughput, latency percentiles, FHE availability, and forecast quality |
| `edge_results.jsonl` | Quality alerts, load-event markers, service output, local model output, FHE output |
| `cloud_forecast_features.csv` | Edge-built FHE forecast input rows |
| `cloud_nilm_features.csv` | Edge-built FHE NILM input rows |
| `cloud_cohort_features.csv` | Edge-built FHE cohort input rows |
| `cloud_forecast_training_examples.csv` | Edge-only matured labels when the configured horizon is observed |
| `cloud_forecast_feature_contract.json` | Runtime FHE forecast input order |
| `cloud_model_feature_contracts.json` | Runtime FHE input order for forecast, NILM, and cohort |

`prepare-edge-report` packages these artifacts under `artifacts/edge_report` with a summary benchmark, model comparison, device profile, and generated report files.

## Metrics

Each run records:

- throughput: raw events/s, normalized events/s, snapshots/s, model results/s
- latency: avg, p50, p95, p99, and max per stage and total tick
- FHE availability: per-model status counts and ok rate
- forecast quality: MAE, RMSE, R2, mean error, p50/p95/max absolute error when targets mature

The edge stores only the local one-minute Ridge forecast. Long-horizon forecasting, NILM, cohort models, training scripts, compiled FHE server packages, and cloud datasets stay in the external cloud/FHE repositories.
