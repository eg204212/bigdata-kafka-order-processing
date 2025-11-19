import argparse
import json
import time
import random
from collections import defaultdict
from io import BytesIO
from pathlib import Path

from fastavro import parse_schema, schemaless_reader
from kafka import KafkaConsumer, KafkaProducer


def load_schema(path: Path):
    with path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    return parse_schema(schema)


class Aggregator:
    def __init__(self):
        self.total_count = 0
        self.total_sum = 0.0
        self.by_product = defaultdict(lambda: {"count": 0, "sum": 0.0})

    def add(self, product, price):
        self.total_count += 1
        self.total_sum += price
        p = self.by_product[product]
        p["count"] += 1
        p["sum"] += price

    def overall_avg(self):
        return self.total_sum / self.total_count if self.total_count else 0.0

    def product_avg(self, product):
        p = self.by_product.get(product)
        if not p or p["count"] == 0:
            return 0.0
        return p["sum"] / p["count"]


def schemaless_decode(schema, data_bytes):
    buff = BytesIO(data_bytes)
    return schemaless_reader(buff, schema)


def process_record(record):
    """Placeholder processing logic. Can raise exceptions for transient failures."""
    # simulate random transient failures for demonstration
    if random.random() < 0.05:
        raise RuntimeError("simulated transient processing error")
    # simulate a permanent error condition rarely
    if random.random() < 0.01:
        raise ValueError("simulated permanent processing error")
    # processing successful
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--dlq-topic", default="orders-dlq")
    parser.add_argument("--group-id", default="order-consumer-group")
    parser.add_argument("--schema", default=str(Path(__file__).parents[1] / "avro" / "order.avsc"))
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--max-messages", type=int, default=0, help="Exit after processing this many messages (0 = run forever)")
    parser.add_argument("--idle-timeout", type=int, default=0, help="Exit if no messages received for this many seconds (0 = disabled)")
    args = parser.parse_args()

    schema = load_schema(Path(args.schema))

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap],
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    dlq_producer = KafkaProducer(bootstrap_servers=[args.bootstrap])

    aggregator = Aggregator()

    print("Starting consumer, listening for orders...")
    processed_count = 0
    last_msg_time = time.time()
    stop = False
    try:
        while not stop:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                # no records received this poll
                if args.idle_timeout and processed_count > 0 and (time.time() - last_msg_time) > args.idle_timeout:
                    print(f"Idle timeout of {args.idle_timeout}s reached, exiting")
                    break
                continue

            for tp, msgs in records.items():
                for msg in msgs:
                    value = msg.value
                    try:
                        order = schemaless_decode(schema, value)
                    except Exception as e:
                        print("Failed to decode Avro record, sending to DLQ:", e)
                        dlq_producer.send(args.dlq_topic, value=value)
                        consumer.commit()
                        continue

                    print(f"Received order {order['orderId']} product={order['product']} price={order['price']}")

                    success = False
                    for attempt in range(1, args.max_retries + 1):
                        try:
                            process_record(order)
                            success = True
                            break
                        except Exception as e:
                            print(f"Processing error (attempt {attempt}/{args.max_retries}): {e}")
                            time.sleep(0.5 * attempt)

                    if not success:
                        print("Exceeded retries, sending message to DLQ")
                        dlq_producer.send(args.dlq_topic, value=value)
                        consumer.commit()
                        continue

                    # update aggregation and commit offset after successful processing
                    aggregator.add(order["product"], float(order["price"]))
                    print(f"Overall running average: {aggregator.overall_avg():.2f}")
                    print(f"Product ({order['product']}) average: {aggregator.product_avg(order['product']):.2f}")
                    consumer.commit()

                    processed_count += 1
                    last_msg_time = time.time()

                    if args.max_messages and processed_count >= args.max_messages:
                        print(f"Processed {processed_count} messages (max reached), exiting")
                        stop = True
                        break

                if stop:
                    break

    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        dlq_producer.flush()
        dlq_producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
