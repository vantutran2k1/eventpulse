import time
from typing import override

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from faker import Faker

from tools.data_gen.base_generator import BaseGenerator

fake = Faker()


class ClickstreamGenerator(BaseGenerator):
    def __init__(self, broker, schema_registry, topic="clicks_raw_v1", **kwargs):
        super().__init__(**kwargs)
        self.topic = topic
        self.producer = AvroProducer(
            {"bootstrap.servers": broker, "schema.registry.url": schema_registry},
            default_value_schema=avro.load("infra/schemas/clicks_raw_v1.avsc")
        )

    def generate_one(self):
        event = {
            "user_id": fake.uuid4(),
            "session_id": fake.uuid4(),
            "event_type": fake.random_element(["click", "view", "purchase"]),
            "url": fake.uri_path(),
            "referer": fake.domain_name(),
            "user_agent": fake.user_agent(),
            "timestamp": int(time.time() * 1000)
        }
        self.producer.produce(topic=self.topic, value=event)
        print(f"[Kafka] Click event: {event}")

    @override
    def run(self):
        super().run()
        self.producer.flush()
