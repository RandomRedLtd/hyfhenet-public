import os
import shutil
from zipfile import ZipFile
from pathlib import Path
from pickle import load
import requests
from concrete.ml.deployment import FHEModelClient
from dotenv import load_dotenv
from pandas import DataFrame
from hashlib import file_digest
import platform

class HyfhenetClient():

    __slots__ = ("api_key", "api_url", "root_dir", "keys_dir", "models_dir", "architecture")

    def __init__(self):
        load_dotenv()

        self.api_key = os.getenv("API_KEY")
        self.api_url = os.getenv("API_URL")

        self.architecture = platform.machine().lower()

        self.__bootstrap()

    def nilm(self, unencrypted_input):
        return self.__inference("nilm", unencrypted_input)

    def cohort(self, unencrypted_input):
        return self.__inference("cohort", unencrypted_input)

    def forecast(self, unencrypted_input):
        return self.__inference("forecast", unencrypted_input)

    def __inference(self, model, unencrypted_input):
        model_dir = self.models_dir / model

        if not model_dir.exists():
            model_dir.mkdir()

            self.__download_model_files(model, model_dir)

        unencrypted_input = dict(map(lambda kv: (kv[0], [kv[1]]), unencrypted_input.items()))

        fhe_client = FHEModelClient(model_dir, self.keys_dir)

        evaluation_key_path = model_dir / "evaluation_key"

        if not evaluation_key_path.exists():
            evaluation_key_path.open("wb").write(fhe_client.get_serialized_evaluation_keys())
            self.__upload_evaluation_key(model, evaluation_key_path)

        data_pre_processor = load((model_dir / "data-pre-processor.pkl").open("rb"))

        unencrypted_input = data_pre_processor.transform(DataFrame(unencrypted_input))

        encrypted_input = fhe_client.quantize_encrypt_serialize(unencrypted_input)

        return fhe_client.deserialize_decrypt_dequantize(self.__send_inference_request(model, model_dir, encrypted_input))

    def __download_model_files(self, model_name, model_path):
        model_zip_path = model_path / "client-files.zip"

        if model_path.exists():
            shutil.rmtree(model_path)

            model_path.mkdir()

        open(model_zip_path, "wb").write(requests.get(url=f"{self.api_url}/api/fhe/{model_name}/client-files", headers={"X-Api-Key": self.api_key, "X-Architecture": self.architecture}, stream=True).raw.data)

        model_zip_file = ZipFile(model_zip_path, "r")

        model_zip_file.extractall(model_path)

        model_zip_file.close()

        os.remove(model_zip_path)

    def __send_inference_request(self, model_name, model_path, encrypted_inputs):
        with open(model_path / "client.zip", "rb", buffering=0) as f:
            model_version = file_digest(f, "sha256").hexdigest()

        response = requests.post(
            url=f"{self.api_url}/api/fhe/{model_name}/inference",
            headers={ "Content-Type": "application/octet-stream", "X-Api-Key": self.api_key, "X-Model-Version": model_version, "X-Architecture": self.architecture },
            data=encrypted_inputs,
            stream=True
        )

        if response.status_code == 400 and response.json()["detail"] == "Bad model version":
            self.download_model_files(model_name, model_path)

            with open(model_path / "version") as f:
                model_version = f.readline().strip("\n")

                response = requests.post(
                    url=f"{self.api_url}/api/fhe/{model_name}/inference",
                    headers={ "Content-Type": "application/octet-stream", "X-Api-Key": self.api_key, "X-Model-Version": model_version, "X-Architecture": self.architecture },
                    data=encrypted_inputs,
                    stream=True
                )

        return response.raw.data

    def __upload_evaluation_key(self, model_name, evaluation_key_path):
        requests.post(
            url=f"{self.api_url}/api/fhe/{model_name}/evaluation-key",
            headers={ "X-Api-Key": self.api_key },
            files={"evaluationkey": open(evaluation_key_path, "rb").read()},
            stream=True
        )

    def __bootstrap(self):
        self.root_dir = Path.home() / ".hyfhenet"

        self.keys_dir = self.root_dir / "fhe-keys"
        Path.mkdir(self.keys_dir, parents=True, exist_ok=True)

        self.models_dir = self.root_dir / "models"
        Path.mkdir(self.models_dir, parents=True, exist_ok=True)
