import zipfile
from pathlib import Path
from hashlib import file_digest
from concrete.ml.deployment import FHEModelServer
from fastapi import HTTPException
from starlette.responses import FileResponse

def fhe_inference(model_name, model_version, architecture, iot_device_id, encrypted_inputs):
    model_path = Path.cwd() / "models" / architecture / model_name

    with open(model_path / "client.zip", "rb", buffering=0) as f:
        if model_version != file_digest(f, "sha256").hexdigest():
            raise HTTPException(status_code=400, detail="Bad model version")

    evaluation_key_path = Path.cwd() / "evaluation-keys" / model_name / str(iot_device_id)

    with open(evaluation_key_path, "rb") as evaluation_key_file:
        evaluation_key = evaluation_key_file.read()

    fhe_server = FHEModelServer(model_path)

    return fhe_server.run(encrypted_inputs, evaluation_key)

def get_model_client_files(model_name, architecture):
    client_files_path = Path.cwd() / "models" / architecture / model_name / "client-files.zip"

    return FileResponse(path=client_files_path, media_type="application/octet-stream")

def upload_evaluation_key(model_name, iot_device_id, evaluation_key):
    eval_key_path = Path.cwd() / "evaluation-keys" / model_name / str(iot_device_id)

    if eval_key_path.exists():
        eval_key_path.unlink()

    with open(eval_key_path, "wb") as f:
        f.write(evaluation_key)

def upload_model_files(file):
    models_dir = Path.cwd() / "models"

    with zipfile.ZipFile(file.file) as zf:
        zf.extractall(models_dir)

def bootstrap():
    evaluation_keys_path = Path.cwd() / "evaluation-keys"

    if not evaluation_keys_path.exists():
        evaluation_keys_path.mkdir()

    models = ["nilm", "cohort", "forecast"]

    for model in models:
        dir_path = evaluation_keys_path / model
        if not dir_path.exists():
            dir_path.mkdir()
