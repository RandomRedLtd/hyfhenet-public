ARG PYTHON_IMAGE=python:3.12.3-slim-bookworm

FROM ${PYTHON_IMAGE} AS python-deps

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

ARG INSTALL_FHE_DEPS=false
ARG INSTALL_BUILD_DEPS=false
ARG INSTALL_EDF_SERVICE_SDK=false
ARG EDF_SERVICE_SDK_PACKAGE=service-sdk-python

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
      $(if [ "$INSTALL_FHE_DEPS" = "true" ] || [ "$INSTALL_BUILD_DEPS" = "true" ]; then echo "build-essential cmake"; fi) \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-fhe.txt ./
COPY scripts/install_fhe_deps.sh ./install_fhe_deps.sh
RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt \
    && if [ "$INSTALL_EDF_SERVICE_SDK" = "true" ]; then python -m pip install --no-cache-dir "$EDF_SERVICE_SDK_PACKAGE"; fi \
    && if [ "$INSTALL_FHE_DEPS" = "true" ]; then sh ./install_fhe_deps.sh; fi

FROM ${PYTHON_IMAGE} AS runtime

ARG INSTALL_REPORT_DEPS=true

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
ENV HYFHENET_FHE_CACHE_DIR=/app/.cache/hyfhenet

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
      $(if [ "$INSTALL_REPORT_DEPS" = "true" ]; then echo "ffmpeg fonts-dejavu-core"; fi) \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-deps /usr/local /usr/local

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
