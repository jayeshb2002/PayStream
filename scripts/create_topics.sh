#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="localhost:9092"
KAFKA_CONTAINER="paystream-kafka"
TOPICS=("payment-events" "payment-events-retry" "payment-events-dlq" "payment-events-audit")

for topic in "${TOPICS[@]}"; do
  docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists --topic "${topic}" \
    --partitions 3 --replication-factor 1
done

echo
echo "PayStream topics:"
docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" --list
