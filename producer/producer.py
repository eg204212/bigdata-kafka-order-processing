#!/usr/bin/env python3
"""
Simple Kafka producer that serializes orders using Avro (fastavro) and publishes to a topic.

Usage examples:
  python producer.py --topic orders --count 100 --interval 0.5

Configuration via CLI args: bootstrap server, topic, message count, interval.
"""
import argparse
import json
import random
import time
import uuid
from io import BytesIO
from pathlib import Path

from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer


def load_schema(path: Path):
    with path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    return parse_schema(schema)


def random_order(products):
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(products),
        "price": round(random.uniform(1.0, 500.0), 2),
    }


def encode_avro(schema, record):
    buff = BytesIO()
    schemaless_writer(buff, schema, record)
    return buff.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="orders", help="Kafka topic to produce to")
    parser.add_argument("--count", type=int, default=0, help="Number of messages to send (0 = infinite)")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between messages")
    parser.add_argument("--schema", default=str(Path(__file__).parents[1] / "avro" / "order.avsc"), help="Path to Avro schema")
    args = parser.parse_args()

    schema_path = Path(args.schema)
    if not schema_path.exists():
        print("Schema file not found:", schema_path)
        return

    schema = load_schema(schema_path)

    producer = KafkaProducer(bootstrap_servers=[args.bootstrap])

    products = ["ItemA", "ItemB", "ItemC", "ItemD"]

    sent = 0
    try:
        while True:
            record = random_order(products)
            raw = encode_avro(schema, record)
            producer.send(args.topic, value=raw)
            sent += 1
            print(f"Sent order {record['orderId']} product={record['product']} price={record['price']}")
            if args.count and sent >= args.count:
                print(f"Finished sending {sent} messages to topic '{args.topic}'")
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"Interrupted by user after sending {sent} messages")
    finally:
        producer.flush()
        producer.close()
        if args.count == 0:
            # only print summary for infinite-mode when interrupted
            print(f"Producer stopped; total messages sent: {sent}")


if __name__ == "__main__":
    main()
