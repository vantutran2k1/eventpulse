import os
from argparse import ArgumentParser

from dotenv import load_dotenv

from tools.data_gen.clickstream_generator import ClickstreamGenerator
from tools.data_gen.db_generators import OrderGenerator, PaymentGenerator

load_dotenv()

PG_CONN = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": os.environ.get("POSTGRES_PORT", "5432"),
    "dbname": os.environ.get("POSTGRES_DB", "transactions"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "password"),
}

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--mode", choices=["clicks", "orders", "payments"], required=True)
    parser.add_argument("--tps", type=int, default=1)
    parser.add_argument("--duration", type=int, default=10)
    args = parser.parse_args()

    if args.mode == "clicks":
        g = ClickstreamGenerator(os.environ.get("KAFKA_BROKER_URI"), os.environ.get("SCHEMA_REGISTRY_URI"),
                                 tps=args.tps, duration=args.duration)
    elif args.mode == "orders":
        g = OrderGenerator(PG_CONN, tps=args.tps, duration=args.duration)
    elif args.mode == "payments":
        g = PaymentGenerator(PG_CONN, tps=args.tps, duration=args.duration)
    else:
        raise ValueError("Invalid mode")

    g.run()
