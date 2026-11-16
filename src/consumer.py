import argparse
import json
import sys

from confluent_kafka import Consumer, KafkaException
from pydantic import ValidationError

from src.config import KAFKA_BOOTSTRAP_SERVERS, PAYMENT_EVENTS_TOPIC
from src.models import PaymentEvent


def parse_args():
    parser = argparse.ArgumentParser(description="Inspect payment events from Kafka.")
    parser.add_argument("--group-id", default="paystream-inspector")
    parser.add_argument("--max-messages", type=int, default=0,
                        help="Stop after N messages; 0 means run until Ctrl+C.")
    parser.add_argument("--timeout", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_messages < 0 or args.timeout <= 0:
        print("Invalid max-messages or timeout.", file=sys.stderr)
        return 2

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": args.group_id,
        "client.id": f"{args.group_id}-client",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "allow.auto.create.topics": False,
    })
    stats = {"received": 0, "valid": 0, "invalid": 0, "errors": 0}
    consumer.subscribe([PAYMENT_EVENTS_TOPIC])
    print(f"Listening on {PAYMENT_EVENTS_TOPIC} at {KAFKA_BOOTSTRAP_SERVERS}; Ctrl+C to stop.")

    try:
        while True:
            message = consumer.poll(args.timeout)
            if message is None:
                continue
            if message.error():
                print(f"Kafka consumer error: {message.error()}", file=sys.stderr)
                stats["errors"] += 1
                continue

            stats["received"] += 1
            try:
                payload = json.loads(message.value().decode("utf-8"))
                event = PaymentEvent.model_validate(payload)
                print(json.dumps(event.model_dump(mode="json"), indent=2))
                stats["valid"] += 1
            except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
                print(
                    f"INVALID partition={message.partition()} offset={message.offset()}: {exc}",
                    file=sys.stderr,
                )
                stats["invalid"] += 1

            # Inspector commits after observing a record, including invalid records.
            # The production processing/DLQ semantics are introduced in Milestone 2.
            consumer.commit(message=message, asynchronous=False)

            if args.max_messages and stats["received"] >= args.max_messages:
                break
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    except KafkaException as exc:
        print(f"Kafka error: {exc}", file=sys.stderr)
        return 1
    finally:
        consumer.close()
        print("\n=== Consumer summary ===")
        for key, value in stats.items():
            print(f"{key:10}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
