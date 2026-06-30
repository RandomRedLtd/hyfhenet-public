from datetime import datetime
from sqlmodel import Field, SQLModel
from enum import Enum, StrEnum


class FheModel(StrEnum):
    nilm = "nilm"
    forecast = "forecast"
    cohort = "cohort"

class InferenceHistory(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    iot_device_id: int | None = Field(foreign_key="iotdevice.id", index=True)
    model: FheModel = Field(index=True)
    date: datetime
    cipher_size_bytes: int
    inference_time_ms: int

class IotDevice(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    device_name: str = Field(unique=True)
    api_key: str = Field(index=True, unique=True)
    deleted: bool = Field(index=True, default=False)
