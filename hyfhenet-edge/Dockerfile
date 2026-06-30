FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
ENV HYFHENET_FHE_CACHE_DIR=/app/.cache/hyfhenet

WORKDIR /app

ARG INSTALL_FHE_DEPS=false

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core \
      $(if [ "$INSTALL_FHE_DEPS" = "true" ]; then echo "build-essential cmake"; fi) \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-fhe.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt \
    && if [ "$INSTALL_FHE_DEPS" = "true" ]; then python -m pip install --no-cache-dir -r requirements-fhe.txt; fi

COPY hyfhenet ./hyfhenet
COPY scripts ./scripts
COPY configs ./configs
RUN mkdir -p /app/data /app/models /app/artifacts /app/.cache/hyfhenet
COPY data/zigbee_mqtt_capture.csv ./data/zigbee_mqtt_capture.csv
COPY models/edge_load_forecast_ridge.json ./models/edge_load_forecast_ridge.json
COPY main.py README.md ./

VOLUME ["/app/artifacts", "/app/.cache/hyfhenet"]

ENTRYPOINT ["python", "main.py"]
CMD ["mqtt-live", "--output", "artifacts/gateway_mqtt_live", "--continuous", "--log-level", "progress"]
