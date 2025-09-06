import random
import time
import uuid

import yaml
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from faker import Faker

from infra.register_schemas import SCHEMA_REGISTRY_URL

with open("tools/data_gen/configs/generator.yaml", "r") as f:
    config = yaml.safe_load(f)

KAFKA_BROKERS = config["kafka"]["bootstrap_servers"]
TOPIC = config["kafka"]["topic"]
EPS = config["generator"]["events_per_second"]
RUN_SECS = config["generator"]["run_seconds"]

fake = Faker()

value_schema = avro.load("infra/schemas/clicks_raw_v1.avsc")
producer = AvroProducer(
    {
        "bootstrap.servers": KAFKA_BROKERS,
        "schema.registry.url": SCHEMA_REGISTRY_URL
    },
    default_value_schema=value_schema
)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def generate_event():
    return {
        "user_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "event_type": random.choice(["page_view", "click", "search", "add_to_cart"]),
        "url": fake.uri_path(),
        "referer": fake.uri(),
        "user_agent": fake.user_agent(),
        "timestamp": int(time.time() * 1000)
    }


if __name__ == "__main__":
    print(f"Starting clickstream generator for {RUN_SECS}s at {EPS} EPS...")
    start = time.time()
    sent = 0
    while time.time() - start < RUN_SECS:
        event = generate_event()
        producer.produce(
            topic=TOPIC,
            value=event,
            callback=delivery_report
        )
        producer.poll(0)
        sent += 1
        if sent % EPS == 0:
            time.sleep(1)
    producer.flush()
    print("Finished.")
