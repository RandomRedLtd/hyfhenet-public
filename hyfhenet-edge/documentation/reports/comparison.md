# miniPC vs QEMU/RPi Report Comparison

This comparison uses the generated report artifacts in `artifacts/miniPC_report/edge_report_x86` and `artifacts/qemu_report`.

The miniPC artifact contains 15 live FHE report cycles. The QEMU/RPi artifact contains 180 live FHE report cycles with FHE inference on every 5-second report tick. The later 180-row miniPC packaging report repeats the 15 miniPC FHE latency values, so those repeated rows are not treated as independent FHE measurements here.

## Method

- Latency statistics come from each report's `run/latency.csv`.
- Continuous values are shown as `median (mean +/- sample SD)`.
- `Non-FHE pipeline overhead` is derived as `total_tick_ms - fhe_cloud_stage_ms`.
- Continuous-metric tests use a two-sided Mann-Whitney U test.
- FHE success rate uses Fisher's exact test.
- Throughput is a single run-level value from `performance_metrics.json`, so no per-cycle statistical test is reported.

## Presentation Table

| Measure | miniPC gateway | QEMU/RPi gateway | Difference | Test |
|---|---:|---:|---:|---:|
| End-to-end tick latency, s | 89.09 (94.88 +/- 24.07), n=15 | 92.45 (94.99 +/- 11.40), n=180 | QEMU +3.8% median | p=0.0084 |
| FHE cloud stage latency, s | 89.09 (94.88 +/- 24.07), n=15 | 92.36 (94.90 +/- 11.40), n=180 | QEMU +3.7% median | p=0.0088 |
| Non-FHE pipeline overhead, ms | 0.76 (2.92 +/- 8.21), n=15 | 73.14 (80.96 +/- 56.14), n=180 | QEMU 96x higher median | p<1e-10 |
| Edge model inference, ms | 0.25 (2.38 +/- 8.22), n=15 | 12.68 (17.81 +/- 39.10), n=180 | QEMU 51x higher median | p<1e-8 |
| FHE result success rate | 40/45 = 88.9% | 540/540 = 100% | QEMU +11.1 percentage points | p=2.18e-6 |
| Wall-clock raw event throughput | 1.72 events/s | 0.14 events/s | QEMU 91.7% lower | descriptive |

The throughput row is the full report wall-clock rate. It includes time spent waiting for FHE calls, so it should not be read as pure CSV parsing or MQTT ingestion speed.

## Findings

- FHE dominates the end-to-end latency in both reports. Average FHE time is almost identical: 94.88 s on miniPC and 94.90 s on QEMU/RPi.
- QEMU/RPi is much slower in local non-FHE work. The edge model stage has a 12.68 ms median on QEMU/RPi versus 0.25 ms on miniPC, but this is still small compared with the roughly 95 s FHE stage.
- The QEMU/RPi report is stronger for reliability evidence because it includes 180 live FHE cycles and 540 successful FHE model outputs. The miniPC source report has only 15 live cycles and 5 unavailable FHE outputs.
- The measured "cloud" endpoints were local servers, not managed cloud deployments. The gateways also ran on different computers, so hardware, emulation, and local networking contribute to the differences. A real cloud FHE backend is planned after prototype feedback; it was not deployed yet to control costs. Latency is expected to improve with a properly provisioned backend.

## Interpretation

For the presentation, the safest message is that FHE is currently the main latency bottleneck, while the gateway implementation itself remains lightweight. The QEMU/RPi path adds clear local execution overhead, especially in feature preparation and edge model inference, but this overhead is measured in milliseconds and is hidden by the FHE stage, which is measured in tens of seconds.

The QEMU/RPi report should be used as the stronger live-cycle evidence because it contains 180 FHE cycles. The miniPC report is useful for showing the faster local gateway path, but its live FHE sample size is small.

## Slide Bullets

- FHE dominates latency: both setups average about 95 s per FHE-enabled cycle; local gateway work is below 0.1 s on QEMU/RPi and near 1 ms on miniPC.
- QEMU/RPi adds large local overhead relative to miniPC: edge model median 12.68 ms vs 0.25 ms, and non-FHE pipeline median 73.14 ms vs 0.76 ms.
- QEMU/RPi provides stronger live-run evidence: 180 cycles and 540/540 successful FHE outputs; miniPC source FHE evidence has 15 cycles and 40/45 successful FHE outputs.
- Current FHE "clouds" were local servers. A managed cloud FHE backend is planned after prototype feedback; it was not deployed yet to control costs, and should reduce latency compared with the lab setup.
