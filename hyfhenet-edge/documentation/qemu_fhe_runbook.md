# QEMU x86_64 Emulation on Raspberry Pi

Use this runbook when a Raspberry Pi must run the full Concrete-ML FHE client stack without Docker. Concrete-ML's prebuilt packages may require x86_64; this procedure creates a persistent x86_64 Debian chroot under QEMU user-mode emulation so the FHE client can run on an aarch64 host.

For the standard native Docker deployment, use [deployment.md](deployment.md). Use this guide only when Docker is unavailable or a bare-metal chroot is preferred.

## Architecture

QEMU user-mode emulation translates x86_64 instructions at the process level. The Linux kernel recognizes x86_64 ELF binaries through `binfmt_misc` and routes them through `qemu-x86_64-static`. Python and Concrete-ML therefore run in an x86_64 userspace.

The Raspberry Pi host remains aarch64. All x86_64 binaries, Python packages, and FHE client state live inside a chroot on an external drive.

## 1. Prerequisites

- Raspberry Pi 4 or 5 running 64-bit Raspberry Pi OS
- External USB drive with an ext4 filesystem and at least 16 GB free
- SSH access from a workstation
- Network access to the FHE cloud endpoint

Confirm the host architecture:

```bash
uname -m
```

Expected: `aarch64` or `arm64`.

## 2. Install QEMU and Bootstrap Tools

```bash
sudo apt-get update
sudo apt-get install -y qemu-user-static binfmt-support debootstrap
```

Verify the x86_64 binfmt registration:

```bash
cat /proc/sys/fs/binfmt_misc/qemu-x86_64
```

The entry should be present and enabled. If it is missing, reboot and check again before continuing.

## 3. Prepare The External Drive

Identify the external-drive partition before continuing:

```bash
lsblk -f
```

The examples below assume it is `/dev/sda1`. Replace that value with the correct partition for your device.

> **Warning:** `mkfs.ext4` permanently erases all data on the selected partition. Skip that command when the partition already has a usable ext4 filesystem.

```bash
sudo umount /dev/sda1 2>/dev/null || true
sudo mkfs.ext4 /dev/sda1
sudo mkdir -p /mnt/usb
sudo mount /dev/sda1 /mnt/usb
```

## 4. Create The x86_64 Chroot

```bash
sudo debootstrap --arch=amd64 bookworm /mnt/usb/x86_64-chroot http://deb.debian.org/debian
sudo ln -sfn /mnt/usb/x86_64-chroot /opt/x86_64-chroot
```

This downloads a minimal Debian x86_64 userspace and can take several minutes on a Raspberry Pi.

## 5. Enter The Chroot

Mount the required virtual filesystems and enter the chroot:

```bash
sudo mount --bind /dev /opt/x86_64-chroot/dev
sudo mount --bind /dev/pts /opt/x86_64-chroot/dev/pts
sudo mount --bind /proc /opt/x86_64-chroot/proc
sudo mount --bind /sys /opt/x86_64-chroot/sys
sudo cp /etc/resolv.conf /opt/x86_64-chroot/etc/resolv.conf
sudo chroot /opt/x86_64-chroot /bin/bash
```

Verify emulation:

```bash
uname -m
```

Expected: `x86_64`.

If `sudo` fails with `unable to allocate pty`, leave the chroot and run `sudo mount -t devpts devpts /dev/pts` on the host before repeating the bind mounts.

## 6. Install Python and FHE Dependencies

Run these commands inside the chroot:

```bash
mkdir -p /tmp
chmod 1777 /tmp
apt-get update
apt-get install -y ca-certificates curl git python3 python3-pip python3-venv

python3 -m venv /opt/concrete-env
source /opt/concrete-env/bin/activate
python -m pip install --upgrade pip
python -m pip install concrete-ml python-dotenv pandas requests scikit-learn
```

Installation is slow under emulation and can take 30–60 minutes for `concrete-ml` and its compiled dependencies.

Verify the installation:

```bash
python - <<'PY'
from concrete.fhe import Configuration
from concrete.ml.deployment import FHEModelClient

print("Concrete Python and FHEModelClient loaded successfully")
PY
```

## 7. Deploy The Edge Code

Still inside the chroot, clone the unified public repository and enter the edge project:

```bash
cd /home
git clone https://github.com/RandomRedLtd/hyfhenet-public.git
cd /home/hyfhenet-public/hyfhenet-edge
```

## 8. Configure The Environment

```bash
cp .env.example .env
nano .env
```

Set:

```text
HYFHENET_FHE_API_URL=https://hyfhe.net
HYFHENET_FHE_API_KEY=<paste-api-key-here>
HYFHENET_FHE_CACHE_DIR=.hyfhenet-cache
HYFHENET_REPORT_VIDEOS=false
```

Export the settings needed by the FHE client in the current shell:

```bash
export SETUPTOOLS_USE_DISTUTILS=stdlib
export HYFHENET_FHE_API_URL=https://hyfhe.net
export HYFHENET_FHE_API_KEY=<paste-api-key-here>
export HYFHENET_REPORT_VIDEOS=false
```

Do not commit real API keys. The `.env` file is ignored by Git.

## 9. Test The Connection

```bash
curl -sS https://hyfhe.net/api/fhe/forecast/client-files \
  -H "X-Api-Key: $HYFHENET_FHE_API_KEY" \
  -o /dev/null -w "%{http_code}\n"
```

Expected: `200`.

## 10. Run The Pipeline Tests

```bash
python -m pip install pytest
python -m pytest tests/test_pipeline.py -v
```

This validates the edge pipeline under QEMU emulation using mock FHE clients.

## 11. Run A Stream Replay (Edge-Only)

```bash
python main.py stream-replay \
  --input data/zigbee_mqtt_capture.csv \
  --output artifacts/gateway_stream \
  --as-fast-as-possible
```

Local processing runs normally. FHE cloud tasks report `unavailable` if the cloud settings are absent or invalid.

## 12. Run The Cloud Report

```bash
python main.py prepare-edge-report \
  --source input \
  --input data/zigbee_mqtt_capture.csv \
  --output artifacts/edge_report \
  --interval-seconds 5 \
  --benchmark-runs 0 \
  --no-videos
```

This replays the included capture through the full edge pipeline with live FHE cloud calls. `--benchmark-runs 0` skips extra benchmark passes, and `--no-videos` disables MP4 rendering.

To limit FHE calls on slow connections:

```bash
python main.py prepare-edge-report \
  --source input \
  --input data/zigbee_mqtt_capture.csv \
  --output artifacts/edge_report \
  --interval-seconds 5 \
  --fhe-sample-interval-seconds 300 \
  --benchmark-runs 0 \
  --no-videos
```

`--fhe-sample-interval-seconds 300` calls the FHE cloud once every five minutes instead of every tick. Local processing still runs at full cadence.

## 13. Verify The Result

```bash
cat artifacts/edge_report/run/latency_summary.json
```

Check FHE statuses:

```bash
python - <<'PY'
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

Expected successful statuses resemble:

```text
fhe_cohort_benchmark ok <count>
fhe_long_term_load_forecast ok <count>
fhe_nilm_disaggregation ok <count>
```

## 14. Copy The Report Off The Device

Exit the chroot:

```bash
exit
```

From the workstation:

```bash
scp -r <user>@<rpi-ip>:/mnt/usb/x86_64-chroot/home/hyfhenet-public/hyfhenet-edge/artifacts/edge_report ./edge_report_rpi
```

## Re-Entering The Chroot After Reboot

After a power cycle, remount the external drive and virtual filesystems:

```bash
sudo mount /dev/sda1 /mnt/usb
sudo mount --bind /dev /opt/x86_64-chroot/dev
sudo mount --bind /dev/pts /opt/x86_64-chroot/dev/pts
sudo mount --bind /proc /opt/x86_64-chroot/proc
sudo mount --bind /sys /opt/x86_64-chroot/sys
sudo cp /etc/resolv.conf /opt/x86_64-chroot/etc/resolv.conf
sudo chroot /opt/x86_64-chroot /bin/bash
source /opt/concrete-env/bin/activate
export SETUPTOOLS_USE_DISTUTILS=stdlib
export HYFHENET_FHE_API_URL=https://hyfhe.net
export HYFHENET_FHE_API_KEY=<paste-api-key-here>
export HYFHENET_REPORT_VIDEOS=false
cd /home/hyfhenet-public/hyfhenet-edge
```

If the drive auto-mounts elsewhere after reboot:

```bash
sudo umount /media/pi/<uuid>
sudo mount /dev/sda1 /mnt/usb
```

## Performance Notes

QEMU user-mode emulation adds substantial overhead to compute-bound operations. Imports, key generation, encryption, and decryption can all take much longer than on native x86_64 hardware. Key generation is generally a one-time cost because keys are cached and reused.

For an initial smoke test, use a large FHE sample interval and keep video generation disabled. Record timings on the actual Raspberry Pi before planning a full evidence run.

## Troubleshooting

- `unable to allocate pty`: run `sudo mount -t devpts devpts /dev/pts` on the host before entering the chroot.
- `No space left on device`: move the chroot to a larger external drive or clean the pip cache with `python -m pip cache purge`.
- `Illegal instruction`: the installed Concrete binary uses an instruction that the active QEMU version cannot translate; update Raspberry Pi OS and QEMU, then retry.
- FHE rows are `unavailable`: verify `HYFHENET_FHE_API_URL` and `HYFHENET_FHE_API_KEY` in both `.env` and the shell environment. Environment variables take precedence over the config file.
- The drive auto-mounts at `/media/pi/<uuid>`: unmount that path and remount the partition at `/mnt/usb`.
- `setuptools` or `distutils` errors: set `export SETUPTOOLS_USE_DISTUTILS=stdlib` before running the pipeline.
