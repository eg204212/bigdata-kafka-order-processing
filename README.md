Project: Kafka-based order processing

## Quickstart

- Install Python dependencies:

```powershell
pip install -r requirements.txt
```

- Start Kafka (docker-compose):

```powershell
docker-compose up -d
```

- Produce test orders (runs until interrupted):

```powershell
python producer/producer.py --topic orders --interval 0.5
```

- Run consumer (will process, retry on transient errors and send permanently failed messages to DLQ):

```powershell
python consumer/consumer.py
```

- Inspect DLQ messages:

```powershell
python dlq/dlq_consumer.py
```

## Notes

- Avro schema is in `avro/order.avsc` and is used by producer/consumer.
- The consumer maintains a running average of prices overall and per-product.
- Retry and DLQ behavior is implemented inside `consumer/consumer.py`.

