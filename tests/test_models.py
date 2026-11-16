from datetime import datetime, timezone
import pytest
from pydantic import ValidationError
from src.models import PaymentEvent


def valid_payload():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "event_id": "evt_test_1",
        "transaction_id": "txn_test_1",
        "event_type": "PAYMENT_INITIATED",
        "payment_rail": "UPI",
        "amount_minor": 12500,
        "currency": "INR",
        "sender_bank": "BANK_A",
        "receiver_bank": "BANK_B",
        "status": "INITIATED",
        "event_time": now,
        "ingestion_time": now,
        "producer_id": "test-producer",
        "sequence_number": 1,
        "schema_version": 1,
    }


def test_valid_event_parses():
    event = PaymentEvent.model_validate(valid_payload())
    assert event.transaction_id == "txn_test_1"
    assert event.amount_minor == 12500


def test_negative_amount_rejected():
    payload = valid_payload()
    payload["amount_minor"] = -1
    with pytest.raises(ValidationError):
        PaymentEvent.model_validate(payload)


def test_unknown_field_rejected():
    payload = valid_payload()
    payload["unexpected"] = "not-allowed"
    with pytest.raises(ValidationError):
        PaymentEvent.model_validate(payload)


def test_timezone_naive_timestamp_rejected():
    payload = valid_payload()
    payload["event_time"] = "2026-11-16T10:00:00"
    with pytest.raises(ValidationError):
        PaymentEvent.model_validate(payload)
