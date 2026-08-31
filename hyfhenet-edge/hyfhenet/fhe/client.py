from __future__ import annotations

import os
import pickle
import platform
import shutil
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
from zipfile import ZipFile


DEFAULT_MODEL_NAME = "forecast"


class BadModelVersionError(RuntimeError):
    pass


@dataclass(frozen=True)
class FheClientConfig:
    api_url: str | None = None
    api_key: str | None = None
    cache_dir: Path | str | None = None
    client_cert_path: Path | str | None = None
    client_key_path: Path | str | None = None
    ca_bundle_path: Path | str | None = None
    architecture: str | None = None
    request_timeout_seconds: float = 60.0
    allow_insecure_http: bool = False


class HyfhenetFheClient:
    """Edge-side Concrete-ML FHE client for the remote HyFHE-Net API."""

    def __init__(self, config: FheClientConfig | None = None) -> None:
        self.config = config or FheClientConfig()
        self._load_dotenv_if_available()
        self.api_url = self._resolve_api_url()
        self.api_key = self._resolve_api_key()
        self.client_cert_path = self._resolve_optional_path(
            self.config.client_cert_path,
            "HYFHENET_FHE_CLIENT_CERT",
        )
        self.client_key_path = self._resolve_optional_path(
            self.config.client_key_path,
            "HYFHENET_FHE_CLIENT_KEY",
        )
        self.ca_bundle_path = self._resolve_optional_path(
            self.config.ca_bundle_path,
            "HYFHENET_FHE_CA_BUNDLE",
        )
        self.architecture = self._resolve_architecture()
        self.allow_insecure_http = self._resolve_allow_insecure_http()
        self.root_dir = self._resolve_cache_dir()
        self.keys_dir = self.root_dir / "fhe-keys" / self.architecture
        self.models_dir = self.root_dir / "models" / self.architecture
        self.keys_dir.mkdir(parents=True, exist_ok=True)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        self.last_timing_ms: dict[str, float] = {}
        self._fhe_model_clients: dict[str, Any] = {}
        self._preprocessors: dict[str, Any] = {}
        self._model_versions: dict[str, tuple[int, int, str]] = {}

    def infer(
        self,
        unencrypted_input: Mapping[str, Any],
        model_name: str = DEFAULT_MODEL_NAME,
    ) -> list[float]:
        self.last_timing_ms = {}
        started_at = time.perf_counter()
        try:
            requests, DataFrame, FHEModelClient = _import_runtime_dependencies()
            self._validate_api_config()

            model_dir = self.models_dir / model_name
            for attempt in range(2):
                self._ensure_model_files(model_name, model_dir, requests)
                try:
                    return self._inference_once(
                        unencrypted_input,
                        model_name,
                        model_dir,
                        requests,
                        DataFrame,
                        FHEModelClient,
                    )
                except BadModelVersionError:
                    if attempt > 0:
                        raise
                    if model_dir.exists():
                        shutil.rmtree(model_dir)
                    self._invalidate_model_cache(model_name)
            raise RuntimeError("FHE inference failed after refreshing model files.")
        finally:
            self._add_timing("fhe_client_total_ms", started_at)

    def forecast(
        self,
        unencrypted_input: Mapping[str, Any],
        model_name: str = DEFAULT_MODEL_NAME,
    ) -> list[float]:
        return self.infer(unencrypted_input, model_name=model_name)

    def nilm(
        self,
        unencrypted_input: Mapping[str, Any],
        model_name: str = "nilm",
    ) -> list[float]:
        return self.infer(unencrypted_input, model_name=model_name)

    def cohort(
        self,
        unencrypted_input: Mapping[str, Any],
        model_name: str = "cohort",
    ) -> list[float]:
        return self.infer(unencrypted_input, model_name=model_name)

    def load_forecast(
        self,
        unencrypted_input: Mapping[str, Any],
        model_name: str = DEFAULT_MODEL_NAME,
    ) -> list[float]:
        return self.forecast(unencrypted_input, model_name=model_name)

    def edge_load_forecast(
        self,
        unencrypted_input: Mapping[str, Any],
        model_name: str = DEFAULT_MODEL_NAME,
    ) -> list[float]:
        return self.forecast(unencrypted_input, model_name=model_name)

    def _inference_once(
        self,
        unencrypted_input: Mapping[str, Any],
        model_name: str,
        model_dir: Path,
        requests_module,
        dataframe_type,
        fhe_model_client_type,
    ) -> list[float]:
        local_prepare_started_at = time.perf_counter()
        frame_input = {key: [value] for key, value in unencrypted_input.items()}
        fhe_client = self._fhe_model_client(
            model_name,
            model_dir,
            fhe_model_client_type,
        )
        evaluation_key_path = model_dir / "evaluation_key"

        if not evaluation_key_path.exists():
            key_started_at = time.perf_counter()
            evaluation_key_path.write_bytes(fhe_client.get_serialized_evaluation_keys())
            self._add_timing("local_fhe_keygen_ms", key_started_at)
            upload_started_at = time.perf_counter()
            try:
                self._upload_evaluation_key(
                    model_name,
                    evaluation_key_path,
                    requests_module,
                )
            finally:
                self._add_timing("cloud_fhe_key_upload_ms", upload_started_at)
            local_prepare_started_at = time.perf_counter()

        preprocessor = self._preprocessor(model_name, model_dir)
        transformed_input = preprocessor.transform(dataframe_type(frame_input))
        encrypted_input = fhe_client.quantize_encrypt_serialize(transformed_input)
        self._add_timing("local_fhe_prepare_encrypt_ms", local_prepare_started_at)
        request_started_at = time.perf_counter()
        try:
            encrypted_output = self._send_inference_request(
                model_name,
                model_dir,
                encrypted_input,
                requests_module,
            )
        finally:
            self._add_timing("cloud_fhe_inference_wait_ms", request_started_at)
        decrypt_started_at = time.perf_counter()
        prediction = fhe_client.deserialize_decrypt_dequantize(encrypted_output)
        self._add_timing("local_fhe_decrypt_ms", decrypt_started_at)
        return _normalise_prediction(prediction)

    def _ensure_model_files(self, model_name: str, model_dir: Path, requests_module) -> None:
        required_files = ["client.zip", "data-pre-processor.pkl"]
        if model_dir.exists() and all((model_dir / name).exists() for name in required_files):
            return
        self._download_model_files(model_name, model_dir, requests_module)

    def _download_model_files(self, model_name: str, model_dir: Path, requests_module) -> None:
        self._invalidate_model_cache(model_name)
        if model_dir.exists():
            shutil.rmtree(model_dir)
        model_dir.mkdir(parents=True, exist_ok=True)

        archive_path = model_dir / "client-files.zip"
        download_started_at = time.perf_counter()
        try:
            response = requests_module.get(
                url=f"{self.api_url}/api/fhe/{model_name}/client-files",
                headers=self._api_headers(),
                stream=True,
                timeout=self.config.request_timeout_seconds,
                verify=self._request_verify(),
                cert=self._request_cert(),
            )
            response.raise_for_status()
            with archive_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        finally:
            self._add_timing(
                "cloud_fhe_client_files_download_ms",
                download_started_at,
            )

        with ZipFile(archive_path, "r") as archive:
            _safe_extract_zip(archive, model_dir)
        archive_path.unlink()

    def _add_timing(self, key: str, started_at: float) -> None:
        elapsed_ms = round(max(time.perf_counter() - started_at, 0.0) * 1000.0, 3)
        self.last_timing_ms[key] = round(
            self.last_timing_ms.get(key, 0.0) + elapsed_ms,
            3,
        )

    def _send_inference_request(
        self,
        model_name: str,
        model_dir: Path,
        encrypted_inputs: bytes,
        requests_module,
    ) -> bytes:
        response = self._post_inference(
            model_name,
            self._model_version(model_name, model_dir),
            encrypted_inputs,
            requests_module,
        )

        if response.status_code == 400 and _bad_model_version(response):
            raise BadModelVersionError("Remote FHE API rejected the cached model version.")

        response.raise_for_status()
        return response.content

    def _post_inference(
        self,
        model_name: str,
        model_version: str,
        encrypted_inputs: bytes,
        requests_module,
    ):
        return requests_module.post(
            url=f"{self.api_url}/api/fhe/{model_name}/inference",
            headers=self._api_headers({
                "Content-Type": "application/octet-stream",
                "X-Model-Version": model_version,
            }),
            data=encrypted_inputs,
            stream=True,
            timeout=self.config.request_timeout_seconds,
            verify=self._request_verify(),
            cert=self._request_cert(),
        )

    def _fhe_model_client(self, model_name: str, model_dir: Path, fhe_model_client_type):
        cached = self._fhe_model_clients.get(model_name)
        if cached is None:
            cached = fhe_model_client_type(model_dir, self.keys_dir)
            self._fhe_model_clients[model_name] = cached
        return cached

    def _preprocessor(self, model_name: str, model_dir: Path):
        cached = self._preprocessors.get(model_name)
        if cached is None:
            with (model_dir / "data-pre-processor.pkl").open("rb") as handle:
                cached = pickle.load(handle)
            self._preprocessors[model_name] = cached
        return cached

    def _model_version(self, model_name: str, model_dir: Path) -> str:
        client_zip = model_dir / "client.zip"
        stat = client_zip.stat()
        cached = self._model_versions.get(model_name)
        if cached is not None and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
            return cached[2]
        digest = _sha256_file(client_zip)
        self._model_versions[model_name] = (stat.st_mtime_ns, stat.st_size, digest)
        return digest

    def _invalidate_model_cache(self, model_name: str) -> None:
        self._fhe_model_clients.pop(model_name, None)
        self._preprocessors.pop(model_name, None)
        self._model_versions.pop(model_name, None)

    def _upload_evaluation_key(
        self,
        model_name: str,
        evaluation_key_path: Path,
        requests_module,
    ) -> None:
        with evaluation_key_path.open("rb") as handle:
            response = requests_module.post(
                url=f"{self.api_url}/api/fhe/{model_name}/evaluation-key",
                headers=self._api_headers(),
                files={"evaluationkey": handle},
                stream=True,
                timeout=self.config.request_timeout_seconds,
                verify=self._request_verify(),
                cert=self._request_cert(),
            )
        response.raise_for_status()

    def _resolve_api_url(self) -> str | None:
        value = (
            self.config.api_url
            or os.getenv("HYFHENET_FHE_API_URL")
            or os.getenv("FHE_URL")
            or os.getenv("API_URL")
        )
        return value.rstrip("/") if value else None

    def _resolve_api_key(self) -> str | None:
        return (
            self.config.api_key
            or os.getenv("HYFHENET_FHE_API_KEY")
            or os.getenv("FHE_API")
            or os.getenv("API_KEY")
        )

    def _resolve_cache_dir(self) -> Path:
        configured = (
            self.config.cache_dir
            or os.getenv("HYFHENET_FHE_CACHE_DIR")
            or (Path.home() / ".hyfhenet")
        )
        return Path(configured)

    def _resolve_architecture(self) -> str:
        configured = (
            self.config.architecture
            or os.getenv("HYFHENET_FHE_ARCHITECTURE")
            or os.getenv("HYFHENET_ARCHITECTURE")
            or platform.machine()
        )
        return _normalise_architecture(str(configured))

    @staticmethod
    def _resolve_optional_path(configured: Path | str | None, env_name: str) -> str | None:
        value = configured or os.getenv(env_name)
        return str(value) if value else None

    def _api_headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {
            "X-Api-Key": str(self.api_key),
            "X-Architecture": self.architecture,
        }
        if extra:
            headers.update(extra)
        return headers

    def _request_verify(self) -> bool | str:
        return self.ca_bundle_path or True

    def _request_cert(self) -> str | tuple[str, str] | None:
        if self.client_cert_path and self.client_key_path:
            return (self.client_cert_path, self.client_key_path)
        return self.client_cert_path

    def _validate_api_config(self) -> None:
        if not self.api_url:
            raise RuntimeError(
                "FHE API URL is not configured. Set HYFHENET_FHE_API_URL or API_URL."
            )
        if not self.api_key:
            raise RuntimeError(
                "FHE API key is not configured. Set HYFHENET_FHE_API_KEY or API_KEY."
            )
        scheme = urlparse(self.api_url).scheme.lower()
        if scheme != "https" and not self.allow_insecure_http:
            raise RuntimeError(
                "FHE API URL must use HTTPS. Set HYFHENET_FHE_ALLOW_INSECURE_HTTP=true "
                "only for isolated lab testing."
            )

    @staticmethod
    def _load_dotenv_if_available() -> None:
        try:
            from dotenv import load_dotenv
        except ImportError:
            return
        load_dotenv()

    def _resolve_allow_insecure_http(self) -> bool:
        if self.config.allow_insecure_http:
            return True
        return _bool_env(os.getenv("HYFHENET_FHE_ALLOW_INSECURE_HTTP", "false"))


HyfhenetClient = HyfhenetFheClient


def _import_runtime_dependencies():
    try:
        import requests
        from concrete.ml.deployment import FHEModelClient
        from pandas import DataFrame
    except ImportError as exc:
        raise RuntimeError(
            "FHE inference requires platform FHE dependencies. "
            "Install them with `pip install -r requirements-fhe.txt`."
        ) from exc
    return requests, DataFrame, FHEModelClient


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bad_model_version(response) -> bool:
    try:
        return response.json().get("detail") == "Bad model version"
    except ValueError:
        return False


def _safe_extract_zip(archive: ZipFile, destination: Path) -> None:
    destination = destination.resolve()
    for member in archive.infolist():
        member_path = (destination / member.filename).resolve()
        if destination != member_path and destination not in member_path.parents:
            raise RuntimeError(f"Unsafe model archive path: {member.filename}")
    archive.extractall(destination)


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalise_architecture(value: str) -> str:
    architecture = value.strip().lower().replace("-", "_")
    aliases = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "x86_64": "x86_64",
        "arm64": "aarch64",
        "aarch64": "aarch64",
    }
    return aliases.get(architecture, architecture)


def _normalise_prediction(prediction: Any) -> list[float]:
    if hasattr(prediction, "tolist"):
        prediction = prediction.tolist()
    while isinstance(prediction, list) and len(prediction) == 1 and isinstance(prediction[0], list):
        prediction = prediction[0]
    if isinstance(prediction, list):
        return [float(value) for value in prediction]
    return [float(prediction)]
