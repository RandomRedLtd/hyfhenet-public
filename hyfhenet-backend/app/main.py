import asyncio
from datetime import datetime, UTC
import os
import re
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException, File, Response, UploadFile
from fastapi.params import Depends
from app.models import FheModel, IotDevice, InferenceHistory
from app.fhe import fhe_inference, bootstrap, upload_evaluation_key as fhe_upload_evaluation_key, get_model_client_files, upload_model_files
from app.database import SessionDep, create_db_and_tables
from dotenv import load_dotenv
import time
import aiorwlock
from fastapi.concurrency import run_in_threadpool

load_dotenv()

bootstrap()

@lru_cache
def get_admin_api_key():
    return os.getenv("ADMIN_API_KEY")

@asynccontextmanager
async def lifespan(app):
    create_db_and_tables()
    yield

def auth_handler(request: Request, session: SessionDep):
    if re.match("^/api/admin/(log-in|log-out)$", request.url.path):
        return

    api_key = request.headers.get("X-Api-Key")

    if api_key is None:
        api_key = request.cookies.get("X-Api-Key")

    if api_key is None:
        raise HTTPException(status_code=401)

    if re.match("^/api/fhe/(nilm|cohort|forecast)/(client-files|evaluation-key|inference)$", request.url.path):
        iot_device = session.query(IotDevice).where((IotDevice.api_key == api_key) & (IotDevice.deleted == False)).one_or_none()

        if api_key != get_admin_api_key() and iot_device is None:
            raise HTTPException(status_code=401)

        if iot_device is not None:
            request.state.iot_device_id = iot_device.id

        return

    if api_key != get_admin_api_key():
        raise HTTPException(status_code=401)

class ModelsRWLock:
    def __init__(self):
        self.model_locks: dict[str, aiorwlock.RWLock] = {}
        self.entrant_lock = asyncio.Lock()

    async def acquire_lock(self, key):
        if key in self.model_locks:
            return self.model_locks[key]

        async with self.entrant_lock:
            if key not in self.model_locks:
                self.model_locks[key] = aiorwlock.RWLock()
            return self.model_locks[key]

model_locks = ModelsRWLock()

app = FastAPI(lifespan=lifespan, dependencies=[Depends(auth_handler)], docs_url=None, redoc_url=None, openapi_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://hyfhe.net"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"]
)

@app.get("/api/admin/log-in")
def log_in(request: Request, response: Response):
    api_key = request.headers.get("X-Api-Key")

    if api_key == get_admin_api_key():
        response.set_cookie(key="X-Api-Key", value=api_key, httponly=True, max_age=60 * 60 * 24 * 7 * 30)
        response.status_code = 200

        return response

    return Response(status_code=401)

@app.get("/api/admin/log-out")
def log_out(response: Response):
    response.set_cookie(key="X-Api-Key", value="", expires="Thu, Jan 01 1970 00:00:00 UTC")
    response.status_code = 200

    return response

@app.get("/api/fhe/{model}/client-files")
def get_model_files(request: Request, model):
    return get_model_client_files(model, request.headers.get("X-Architecture"))

@app.post("/api/fhe/{model}/model")
async def upload_model(model, file: UploadFile):
    rw_lock = await model_locks.acquire_lock(model)

    async with rw_lock.writer_lock:
        upload_model_files(file)

        return Response(status_code=200)

@app.post("/api/fhe/{model}/inference")
async def inference(request: Request, session: SessionDep, model):
    rw_lock = await model_locks.acquire_lock(model)

    async with rw_lock.reader_lock:
        inference_input = await request.body()

        iot_device_id = getattr(request.state, "iot_device_id", None)

        if iot_device_id is None:
            iot_device_id = 0

        inference_history_entry = InferenceHistory()
        inference_history_entry.model = FheModel[model]
        inference_history_entry.date = datetime.now(UTC)
        inference_history_entry.cipher_size_bytes = len(inference_input)
        inference_history_entry.iot_device_id = iot_device_id

        start = time.time()

        inference_result = await run_in_threadpool(fhe_inference, model, request.headers.get("X-Model-Version"), request.headers.get("X-Architecture"), iot_device_id, inference_input)

        inference_history_entry.inference_time_ms = int(((time.time() - start) * 1000))

        session.add(inference_history_entry)
        session.commit()

        return Response(content=inference_result, media_type="application/octet-stream")

@app.post("/api/fhe/{model}/evaluation-key")
def upload_evaluation_key(request: Request, evaluationkey: Annotated[bytes, File()], model):
    iot_device_id = getattr(request.state, "iot_device_id", None)

    if iot_device_id is None:
        iot_device_id = 0

    fhe_upload_evaluation_key(model, iot_device_id, evaluationkey)
    return Response(status_code=200)

@app.get("/api/inference-histories/")
def all_inference_history(response: Response, session: SessionDep, page: int = 0, page_size: int = 20):
    total_count = (session.query(InferenceHistory).count())
    response.headers["X-Total-Count"] = str(total_count)

    return (session.query(InferenceHistory)
            .offset(page * page_size)
            .limit(page_size)
            .all())

@app.get("/api/inference-histories/{id}")
def inference_history(session: SessionDep, id: int):
    return session.get(InferenceHistory, id)

@app.get("/api/iot-devices/")
async def all_iot_devices(response: Response, session: SessionDep, page: int = 0, page_size: int = 20, deleted: bool = False):
    total_count = (session.query(IotDevice).count())
    response.headers["X-Total-Count"] = str(total_count)

    return (session.query(IotDevice)
            .offset(page * page_size)
            .limit(page_size)
            .all())

@app.get("/api/iot-devices/{id}")
def iot_device(session: SessionDep, id: int):
    iot_device = session.get(IotDevice, id)

    if iot_device is None:
        return Response(status_code=404)

    return iot_device

@app.post("/api/iot-devices")
def create_iot_device(iot_device: IotDevice, session: SessionDep):
    session.add(iot_device)
    session.commit()
    session.refresh(iot_device)
    return iot_device

@app.delete("/api/iot-devices/{id}")
def delete_iot_device(session: SessionDep, id: int):
    iot_device = session.get(IotDevice, id)

    if iot_device is None:
        return Response(status_code=404)

    (session.query(IotDevice)
     .where(IotDevice.id == id)
     .update({IotDevice.deleted: not iot_device.deleted}))

    session.commit()

    return Response(status_code=200)
