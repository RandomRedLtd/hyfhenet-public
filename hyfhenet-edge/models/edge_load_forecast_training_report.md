# Edge Short-Term Load Forecast Training Report

## Model

- Model id: `edge_short_term_load_forecast`
- Backend: `ridge_regression`
- Horizon: `1` minute(s)
- Regularization alpha: `300.0`
- Feature set: `long_term_load_forecast_v1`
- Input feature count: `35`
- Trained at: `2026-05-31T22:07:31+00:00`

## Data

- Source replay: `data\zigbee_mqtt_capture.csv`
- Supervised examples: `28`
- Train examples: `19`
- Test examples: `9`
- Train fraction: `0.7`
- Household target source: `sum_of_configured_smart_plugs`
- Saved dataset: `data\edge_load_forecast_training_dataset.csv`
- Saved model: `models\edge_load_forecast_ridge.json`

## Target Variable

- Target column: `target_household_power_w`
- Target horizon: `1` minute(s)
- Definition: monitored household active power observed at `target_timestamp = timestamp + horizon_minutes`.
- Stage 2 source: `plug_1_power_w + plug_2_power_w` from the two configured Zigbee smart plugs.
- Rows are emitted only when the future derived household-power target is observed.

## Evaluation

| Split | MAE W | RMSE W | R2 |
| --- | ---: | ---: | ---: |
| Train | 3.0251 | 3.9995 | 0.2415 |
| Test | 1.2366 | 1.2936 | -0.245 |
| All | 2.4502 | 3.3753 | 0.2495 |

## Features

| Feature | Description |
| --- | --- |
| `horizon_minutes` | Forecast horizon encoded in minutes. |
| `minute_of_day` | Minute index from midnight at feature timestamp. |
| `day_of_week` | Python weekday number, Monday=0. |
| `is_weekend` | Binary weekend flag. |
| `minute_sin` | Sine encoding of minute-of-day. |
| `minute_cos` | Cosine encoding of minute-of-day. |
| `dow_sin` | Sine encoding of day-of-week. |
| `dow_cos` | Cosine encoding of day-of-week. |
| `target_minute_of_day` | Minute index from midnight at target timestamp. |
| `target_day_of_week` | Python weekday number at target timestamp, Monday=0. |
| `target_is_weekend` | Binary weekend flag at target timestamp. |
| `target_minute_sin` | Sine encoding of target minute-of-day. |
| `target_minute_cos` | Cosine encoding of target minute-of-day. |
| `target_dow_sin` | Sine encoding of target day-of-week. |
| `target_dow_cos` | Cosine encoding of target day-of-week. |
| `linky_household_power_w` | Current household active power, from Linky/TIC when present or the edge plug-sum fallback. |
| `linky_household_power_w_missing` | Missing indicator for real household meter power; plug-sum fallback sets this to 1. |
| `linky_household_power_mean_1h_w` | One-hour mean household active power. |
| `linky_household_power_std_1h_w` | One-hour household active power standard deviation. |
| `linky_household_power_delta_1h_w` | Change in household active power over the observed one-hour window. |
| `linky_household_power_mean_24h_w` | Twenty-four-hour mean household active power over observed history. |
| `linky_household_power_std_24h_w` | Twenty-four-hour household active power standard deviation over observed history. |
| `plug_1_power_w` | Current active power from configured smart plug 1. |
| `plug_1_power_w_missing` | Missing indicator for smart plug 1 power. |
| `plug_1_power_mean_1h_w` | One-hour mean active power from configured smart plug 1. |
| `plug_2_power_w` | Current active power from configured smart plug 2. |
| `plug_2_power_w_missing` | Missing indicator for smart plug 2 power. |
| `plug_2_power_mean_1h_w` | One-hour mean active power from configured smart plug 2. |
| `monitored_plug_share` | Share of current household power represented by the configured smart plugs. |
| `indoor_temperature_c` | Current indoor temperature. |
| `indoor_humidity_pct` | Current indoor humidity. |
| `environment_missing_flag` | Binary flag showing environment data is stale or unavailable. |
| `temperature_delta_1h_c` | Change in indoor temperature over the observed one-hour window. |
| `load_event_type_code` | Integer code for the latest load event type. |
| `load_event_direction_code` | Integer code for latest load direction. |

## Notes

The model is trained once and saved. Runtime streaming loads the saved JSON model and performs inference only.
During a stream, matured prediction-vs-actual rows are written to `edge_results.jsonl` with `edge_record_type=edge_forecast_evaluation`.
The current Stage 2 capture is short, so the 1-minute horizon is used for local validation. Longer horizons need longer live data.
