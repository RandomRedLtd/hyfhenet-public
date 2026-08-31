# HyFHE-Net Edge Report

Use this to run HyFHE-Net report mode on a Linux edge device with CSV input and the FHE cloud at `https://hyfhe.net`.

Report mode only replays an existing CSV file through the edge pipeline and calls the FHE cloud.

Do not commit real API keys. Put the key only in the device-local `.env`.

## 1. Install Prerequisites

Use a Linux MiniPC or 64-bit Raspberry Pi OS. On Raspberry Pi, confirm:

```bash
uname -m
```

Expected: `aarch64` or `arm64`.

Install Docker and Git:

```bash
sudo apt-get update
sudo apt-get install -y git ca-certificates curl
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"
newgrp docker
docker compose version
```

## 2. Get The Code

```bash
git clone https://github.com/RandomRedLtd/hyfhenet.git
cd hyfhenet
git checkout main
```

## 3. Configure Report Mode

```bash
cp .env.example .env
nano .env
```

Set these values:

```text
FHE_URL=https://hyfhe.net
FHE_API=<paste-api-key-here>
HYFHENET_REPORT_INSTALL_FHE_DEPS=true
HYFHENET_REPORT_SOURCE=input
HYFHENET_REPORT_INPUT=data/zigbee_mqtt_capture.csv
HYFHENET_REPORT_OUTPUT=artifacts/edge_report
HYFHENET_REPORT_INTERVAL_SECONDS=5
HYFHENET_REPORT_BENCHMARK_RUNS=0
HYFHENET_REPORT_FHE_SAMPLE_INTERVAL_SECONDS=
HYFHENET_REPORT_FHE_TASKS=forecast,nilm,cohort
HYFHENET_REPORT_VIDEOS=false
```

## 4. Provide The CSV Input

For a first run, keep:

```text
HYFHENET_REPORT_INPUT=data/zigbee_mqtt_capture.csv
```

That file is included in the repository.

To use your own CSV instead, copy it into `artifacts/` and change `HYFHENET_REPORT_INPUT`:

```bash
mkdir -p artifacts
cp <your-input.csv> artifacts/report_input.csv
ls -lh artifacts/report_input.csv
head -5 artifacts/report_input.csv
```

Then set:

```text
HYFHENET_REPORT_INPUT=artifacts/report_input.csv
```

## 5. Build The FHE Report Image

```bash
docker compose build report
```

This can take several minutes because it installs Concrete/FHE dependencies.

## 6. Run The Cloud Report

```bash
docker compose --profile report run --rm report
```

This runs:

```text
prepare-edge-report --source input --input data/zigbee_mqtt_capture.csv --output artifacts/edge_report
```

`HYFHENET_REPORT_BENCHMARK_RUNS=0` skips extra benchmark replays, so FHE is called only during the main report pass.

Expected terminal output:

```text
Report ready: /app/artifacts/edge_report
HTML: /app/artifacts/edge_report/showcase.html
Latency samples: <number>
Manifest: /app/artifacts/edge_report/manifest.json
```

## 7. Verify The Result

Check the main summary files:

```bash
cat artifacts/edge_report/README.md
cat artifacts/edge_report/run/latency_summary.json
```

Check FHE statuses:

```bash
python3 - <<'PY'
import csv
from collections import Counter

with open("artifacts/edge_report/run/model_results.csv", newline="", encoding="utf-8") as handle:
    counts = Counter(
        (row["model_id"], row["inference_status"])
        for row in csv.DictReader(handle)
        if row["model_id"].startswith("fhe")
    )

for (model_id, status), count in sorted(counts.items()):
    print(model_id, status, count)
PY
```

Expected statuses:

```text
fhe_cohort_benchmark ok <count>
fhe_long_term_load_forecast ok <count>
fhe_nilm_disaggregation ok <count>
```

## 8. Report Files

```text
artifacts/edge_report/showcase.html
artifacts/edge_report/README.md
artifacts/edge_report/manifest.json
artifacts/edge_report/run/model_results.csv
artifacts/edge_report/run/latency.csv
artifacts/edge_report/run/latency_summary.json
artifacts/edge_report/benchmark/summary.json
artifacts/edge_report/model/model_comparison.csv
```

Copy the report off the edge device:

```bash
scp -r <user>@<edge-ip>:~/hyfhenet/artifacts/edge_report ./edge_report_device
```

## Troubleshooting

- Input file not found: confirm the CSV exists at the path set in `HYFHENET_REPORT_INPUT`.
- FHE rows are `unavailable`: check `FHE_URL`, `FHE_API`, HTTPS access, and rebuild with `HYFHENET_REPORT_INSTALL_FHE_DEPS=true`.
- Report is too slow: set `HYFHENET_REPORT_FHE_SAMPLE_INTERVAL_SECONDS` to a positive value such as `300` or `10000`, and keep `HYFHENET_REPORT_VIDEOS=false`.
