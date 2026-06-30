# HyFHE-net fhe client

This component is designed to be a backend client library for the [HyFHE-net edge](../hyfhenet-edge).

**NOTE:** You can use this library as a standalone component to interact with the backend, but it is already included and used under the hood in the [HyFHE-net edge](../hyfhenet-edge) component.

## 1. Usage:

### 1.1. Create a virtual environment:

`$ python -m venv .venv`

### 1.2. Activate virtual environment

`$ source .venv/bin/activate`

### 1.3. Install dependencies:

`$ ./install-deps.sh`

### 1.4. Create a `.env` file from `.env.example` and set environment variables: `API_URL` and `API_KEY`

`$ echo API_URL=http://localhost:8000 > .env`

`$ echo API_KEY=secret >> .env`

### 1.5. Import `HyfhenetClient` into your code:

`from hyfhenet_client import HyfhenetClient`

### 1.6. Create a client object:

`fhe_client = HyfhenetClient()`

### 1.7. Use instantiated client:

`response = client.cohort(inference_input)`

`print(response)`

### 2. API

### 2.1. cohort(inference_input)
### 2.2. forecast(inference_input)
### 2.3. nilm(inference_input)
