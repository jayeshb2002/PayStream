import random
from src.generator import generate_lifecycle
from src.models import PaymentEvent


def test_generator_emits_three_ordered_events():
    events = list(generate_lifecycle(rng=random.Random(7)))
    assert len(events) == 3
    assert len({transaction_id for transaction_id, _, _ in events}) == 1
    parsed = [PaymentEvent.model_validate_json(payload) for _, payload, _ in events]
    assert [event.sequence_number for event in parsed] == [1, 2, 3]
    assert parsed[0].status == "INITIATED"
    assert parsed[1].status == "PROCESSING"
    assert parsed[2].status in {"SUCCESS", "FAILED"}


def test_event_ids_are_unique_within_lifecycle():
    events = list(generate_lifecycle(rng=random.Random(10)))
    parsed = [PaymentEvent.model_validate_json(payload) for _, payload, _ in events]
    assert len({event.event_id for event in parsed}) == 3


def test_late_events_have_old_event_time():
    events = list(generate_lifecycle(late_rate=1.0, rng=random.Random(11)))
    parsed = [PaymentEvent.model_validate_json(payload) for _, payload, _ in events]
    assert all(event.event_time < event.ingestion_time for event in parsed)


def test_malformed_rate_emits_invalid_payloads():
    events = list(generate_lifecycle(malformed_rate=1.0, rng=random.Random(12)))
    malformed = [payload for _, payload, kind in events if kind == "malformed"]
    assert len(malformed) == 3
    for payload in malformed:
        try:
            PaymentEvent.model_validate_json(payload)
        except Exception:
            continue
        raise AssertionError("Malformed payload unexpectedly validated")
