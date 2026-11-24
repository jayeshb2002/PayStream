import argparse
import random
import sys
import time
from collections import Counter

from confluent_kafka import KafkaException, Producer

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CLIENT_ID, PAYMENT_EVENTS_TOPIC
from src.generator import generate_lifecycle

from src.orchestration.failure_tracker import (
    record_pipeline_failure_safely
)

def probability(value: str) -> float:
    number = float(value)
    if not 0.0 <= number <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return number


def parse_args():
    parser = argparse.ArgumentParser(description="Publish synthetic payment events to Kafka.")
    parser.add_argument("--count", type=int, default=100, help="Number of payment lifecycles.")
    parser.add_argument("--rate", type=float, default=0, help="Transactions/second; 0 disables pacing.")
    parser.add_argument("--duplicate-rate", type=probability, default=0.0)
    parser.add_argument("--malformed-rate", type=probability, default=0.0)
    parser.add_argument("--late-rate", type=probability, default=0.0)
    parser.add_argument("--late-delay-minutes", type=int, default=10)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--producer-id", default="paystream-generator-01")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1 or args.rate < 0 or args.late_delay_minutes < 1:
        print("count must be >= 1, rate >= 0 and late-delay-minutes >= 1", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    stats = Counter()
    delivery_errors = []

    def on_delivery(error, message):
        if error is not None:
            stats["delivery_errors"] += 1
            delivery_errors.append(str(error))
        else:
            stats["delivered"] += 1

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": KAFKA_CLIENT_ID,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 10,
    })

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    started = time.monotonic()

    try:
        for index in range(args.count):
            transaction_started = time.monotonic()
            for transaction_id, payload, kind in generate_lifecycle(
                producer_id=args.producer_id,
                malformed_rate=args.malformed_rate,
                late_rate=args.late_rate,
                late_delay_minutes=args.late_delay_minutes,
                rng=rng,
            ):
                try:
                    # Keep your existing Kafka producer operation here.
                    producer.produce(
                        PAYMENT_EVENTS_TOPIC,
                        key=transaction_id.encode("utf-8"),
                        value=payload,
                        callback=on_delivery,
                    )

                except Exception as exc:
                    record_pipeline_failure_safely(
                        component="kafka_producer",
                        failure_type="KAFKA_PRODUCER_FAILURE",
                        exception=exc,
                        severity="CRITICAL",
                        context={
                            "topic": topic
                        }
                    )
                    raise
                stats["original_messages"] += 1
                stats[kind] += 1

                if kind == "valid" and rng.random() < args.duplicate_rate:
                    producer.produce(
                        PAYMENT_EVENTS_TOPIC,
                        key=transaction_id.encode("utf-8"),
                        value=payload,
                        callback=on_delivery,
                    )
                    stats["duplicate_copies"] += 1
                producer.poll(0)

            if interval:
                time.sleep(max(0.0, interval - (time.monotonic() - transaction_started)))

            if (index + 1) % 1000 == 0:
                print(f"Generated {index + 1}/{args.count} lifecycles")

        remaining = producer.flush(30)
        if remaining:
            print(f"WARNING: {remaining} messages remain queued after flush.")
    except BufferError as exc:
        print(f"Producer queue full: {exc}", file=sys.stderr)
        producer.flush(30)
        return 1
    except KafkaException as exc:
        print(f"Kafka error: {exc}", file=sys.stderr)
        producer.flush(30)
        return 1

    print("\n=== PayStream producer summary ===")
    print(f"Transactions requested : {args.count}")
    print(f"Original messages      : {stats['original_messages']}")
    print(f"Valid messages         : {stats['valid']}")
    print(f"Malformed messages     : {stats['malformed']}")
    print(f"Duplicate copies       : {stats['duplicate_copies']}")
    print(f"Successfully delivered : {stats['delivered']}")
    print(f"Delivery errors        : {stats['delivery_errors']}")
    print(f"Elapsed seconds        : {time.monotonic() - started:.2f}")
    print(f"Topic                  : {PAYMENT_EVENTS_TOPIC}")
    if delivery_errors:
        print("First delivery error:", delivery_errors[0])
    return 1 if stats["delivery_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
