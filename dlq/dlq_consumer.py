import argparse
import json
from io import BytesIO
from pathlib import Path

from fastavro import parse_schema, schemaless_reader
from kafka import KafkaConsumer


def load_schema(path: Path):
    with path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    return parse_schema(schema)


def schemaless_decode(schema, data_bytes):
    buff = BytesIO(data_bytes)
    return schemaless_reader(buff, schema)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--dlq-topic", default="orders-dlq")
    parser.add_argument("--schema", default=str(Path(__file__).parents[1] / "avro" / "order.avsc"))
    args = parser.parse_args()

    schema = load_schema(Path(args.schema))

    consumer = KafkaConsumer(args.dlq_topic, bootstrap_servers=[args.bootstrap], auto_offset_reset="earliest")
    print("Listening for DLQ messages on topic:", args.dlq_topic)
    try:
        for msg in consumer:
            try:
                order = schemaless_decode(schema, msg.value)
                print("DLQ message (decoded):", order)
            except Exception:
                print("DLQ message (raw bytes):", msg.value)
    except KeyboardInterrupt:
        print("DLQ consumer interrupted")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
