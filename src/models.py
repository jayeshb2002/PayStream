from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PaymentRail(str, Enum):
    UPI = "UPI"
    IMPS = "IMPS"
    RUPAY = "RUPAY"
    NEFT = "NEFT"


class PaymentStatus(str, Enum):
    INITIATED = "INITIATED"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class EventType(str, Enum):
    PAYMENT_INITIATED = "PAYMENT_INITIATED"
    PAYMENT_STATUS_UPDATED = "PAYMENT_STATUS_UPDATED"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    PAYMENT_FAILED = "PAYMENT_FAILED"


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    event_id: str = Field(min_length=1)
    transaction_id: str = Field(min_length=1)
    event_type: EventType
    payment_rail: PaymentRail
    amount_minor: int = Field(gt=0, description="Amount in paise for INR")
    currency: Literal["INR"] = "INR"
    sender_bank: str = Field(min_length=2, max_length=32)
    receiver_bank: str = Field(min_length=2, max_length=32)
    status: PaymentStatus
    event_time: datetime
    ingestion_time: datetime
    producer_id: str = Field(min_length=1)
    sequence_number: int = Field(ge=1)
    schema_version: int = Field(ge=1)

    @field_validator("event_time", "ingestion_time")
    @classmethod
    def timestamps_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamp must include a timezone")
        return value
