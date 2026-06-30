# HyFHE-net backend

## 0. Prerequisites:
- Docker
- Python 3.12.12

### 0.1. Models

The pretrained FHE models may be found in [HyFHE-net fhe pretrained Linux amd64 directory](../hyfhenet-fhe/models-pretrained-linux-amd64).

These models are only for amd64 Linux, if you are on a different platform refer to the [HyFHE-net fhe](../hyfhenet-fhe) on how to train models for your platform.

To run these pretrained models with the backend copy them to this directory before running in development and/or building a Docker image:

`$ cp -r ../hyfhenet-fhe/models-pretrained-linux-amd64 ./models`

## 1. Running in development

### 1.1. Create a virtual environment

`$ python -m venv .venv`

### 1.2. Activate the virtual environment:

`$ source .venv/bin/activate`

### 1.3. Install dependencies:

`$ chmod +x ./install-deps.sh`

`$ ./install-deps.sh`

### 1.4. Run:

`$ fastapi dev`

## 2. Building a Docker image

### 2.1. Set environment variables in `.env`:

`$ echo ADMIN_API_KEY={your_api_key} > .env`

### 2.2. Run Docker build:

`$ docker build --progress=plain --no-cache -t hyfhenet-backend .`

### 2.3. Run Docker container:

`$ docker run hyfhenet-backend -p 8000:80`
