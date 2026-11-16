import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
PAYMENT_EVENTS_TOPIC = os.getenv("KAFKA_TOPIC_PAYMENT_EVENTS", "payment-events")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "paystream-generator")
