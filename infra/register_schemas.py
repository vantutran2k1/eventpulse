import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

SCHEMA_REGISTRY_URI = os.getenv("SCHEMA_REGISTRY_URI", "http://localhost:8081")
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")


def register_schema(topic: str, schema_file: str):
    """Register an Avro schema file for a Kafka topic value."""
    subject = f"{topic}-value"
    path = os.path.join(SCHEMA_DIR, schema_file)

    with open(path, "r") as f:
        schema_str = f.read()

    payload = {"schema": schema_str}
    resp = requests.post(
        f"{SCHEMA_REGISTRY_URI}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps(payload),
    )

    if resp.status_code == 200:
        version = resp.json()["id"]
        print(f"Registered {subject} as version {version}")
    elif resp.status_code == 409:
        print(f"Schema for {subject} is incompatible with existing version")
    else:
        print(f"Failed to register {subject}: {resp.status_code} {resp.text}")


if __name__ == "__main__":
    schemas = {
        "clicks_raw_v1": "clicks_raw_v1.avsc"
    }

    for topic, file in schemas.items():
        register_schema(topic, file)
