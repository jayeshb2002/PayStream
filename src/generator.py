import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator

from src.models import EventType, PaymentEvent, PaymentRail, PaymentStatus

BANKS = ["BANK_A", "BANK_B", "BANK_C", "BANK_D", "BANK_E"]
RAILS = list(PaymentRail)


def generate_lifecycle(
    *,
    producer_id: str = "paystream-generator-01",
    duplicate_rate: float = 0.0,
    malformed_rate: float = 0.0,
    late_rate: float = 0.0,
    late_delay_minutes: int = 10,
    rng: random.Random | None = None,
) -> Iterator[tuple[str, bytes, str]]:
    # Yield (transaction_id, serialized_payload, kind).
    rng = rng or random.Random()
    transaction_id = f"txn_{uuid.uuid4().hex[:16]}"
    event_base_time = datetime.now(timezone.utc)
    amount_minor = rng.randint(10_000, 5_000_000)
    rail = rng.choice(RAILS)
    sender, receiver = rng.sample(BANKS, 2)
    final_success = rng.random() >= 0.04

    lifecycle = [
        (EventType.PAYMENT_INITIATED, PaymentStatus.INITIATED),
        (EventType.PAYMENT_STATUS_UPDATED, PaymentStatus.PROCESSING),
        (
            EventType.PAYMENT_COMPLETED if final_success else EventType.PAYMENT_FAILED,
            PaymentStatus.SUCCESS if final_success else PaymentStatus.FAILED,
        ),
    ]

    for sequence, (event_type, status) in enumerate(lifecycle, start=1):
        event_time = event_base_time + timedelta(seconds=sequence - 1)
        if rng.random() < late_rate:
            event_time -= timedelta(minutes=late_delay_minutes)

        event = PaymentEvent(
            event_id=f"evt_{uuid.uuid4().hex}",
            transaction_id=transaction_id,
            event_type=event_type,
            payment_rail=rail,
            amount_minor=amount_minor,
            currency="INR",
            sender_bank=sender,
            receiver_bank=receiver,
            status=status,
            event_time=event_time,
            ingestion_time=datetime.now(timezone.utc),
            producer_id=producer_id,
            sequence_number=sequence,
            schema_version=1,
        )
        yield transaction_id, event.model_dump_json().encode("utf-8"), "valid"

        if rng.random() < malformed_rate:
            bad_payload = (
                "{"
                f'"event_id":"evt_bad_{uuid.uuid4().hex[:8]}",'
                f'"transaction_id":"{transaction_id}",'
                '"event_type":"PAYMENT_COMPLETED",'
                '"payment_rail":"IMPS",'
                '"amount_minor":-500,'
                '"currency":"INR"'
                "}"
            ).encode("utf-8")
            yield transaction_id, bad_payload, "malformed"
