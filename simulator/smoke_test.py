"""Quick check that Python can talk to the cluster.

Produces 10 test payments to `payments`, then reads them back.
Run from the repo root:  python simulator/smoke_test.py
"""
import json
import time
import uuid

from confluent_kafka import Consumer, Producer

BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "payments"

producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True})
for i in range(10):
    card = f"card-{i % 3}"
    event = {
        "txn_id": str(uuid.uuid4()),
        "card_id": card,
        "amount": round(10 + i * 3.5, 2),
        "country": "CA",
        "ts_ms": int(time.time() * 1000),
    }
    producer.produce(TOPIC, key=card, value=json.dumps(event))
producer.flush()
print("Produced 10 events")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": f"smoke-test-{uuid.uuid4()}",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])
seen, deadline = 0, time.time() + 15
while seen < 10 and time.time() < deadline:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    seen += 1
    print(f"partition={msg.partition()} offset={msg.offset()} key={msg.key().decode()}")
consumer.close()
print("OK" if seen >= 10 else f"Only read {seen} events - check the cluster")
