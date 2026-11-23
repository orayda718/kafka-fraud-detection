"""Quick check that Python can talk to the cluster, using the shared message schema.

Produces 10 test payments to `payments` (same JSON as the simulator, see
TEAM_TASKS.md section 4), then reads back exactly those 10 by payment_id.
Run from the repo root:  python simulator/smoke_test.py

The test uses cards named smoke_0..smoke_2. They have no profile on
card-profiles and only 3-4 small payments each, so the detector ignores them.
"""
import json
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
TOPIC = "payments"
N = 10

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "partitioner": "murmur2_random",  # same key -> partition mapping as the Java client
})
try:
    producer.list_topics(TOPIC, timeout=10)
except KafkaException as e:
    sys.exit(f"Cannot reach Kafka at {BOOTSTRAP}: {e}")

sent = {}
partitions = {}


def on_delivery(err, msg):
    if err is None:
        partitions[json.loads(msg.value())["payment_id"]] = (msg.partition(), msg.offset())
    else:
        print(f"delivery failed: {err}")


now = int(time.time() * 1000)
for i in range(N):
    card = f"smoke_{i % 3}"
    event = {
        "payment_id": str(uuid.uuid4()),
        "card_id": card,
        "amount": round(10 + i * 3.5, 2),
        "currency": "CAD",
        "merchant": f"Coffee Shop {i + 1}",
        "merchant_category": "food",
        "country": "CA",
        "timestamp_ms": now + i,
    }
    sent[event["payment_id"]] = event
    producer.produce(TOPIC, key=card, value=json.dumps(event), on_delivery=on_delivery)
producer.flush(15)
print(f"Produced {len(partitions)}/{N} payments")

# Read back from the exact offsets the brokers reported, so this stays fast on a big topic.
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": f"smoke-test-{uuid.uuid4()}",
    "enable.auto.commit": False,
})
start = {}
for part, offset in partitions.values():
    start[part] = min(offset, start.get(part, offset))
consumer.assign([TopicPartition(TOPIC, part, offset) for part, offset in start.items()])
seen, deadline = set(), time.time() + 30
while len(seen) < N and time.time() < deadline:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    try:
        event = json.loads(msg.value())
    except ValueError:
        continue
    pid = event.get("payment_id")
    if pid in sent and pid not in seen:
        seen.add(pid)
        print(f"partition={msg.partition()} offset={msg.offset()} key={msg.key().decode()} "
              f"amount={event['amount']}")
consumer.close()

# Same key must always land in the same partition (keys decide partitioning).
by_card = {}
for pid, (part, _) in partitions.items():
    by_card.setdefault(sent[pid]["card_id"], set()).add(part)
same_partition = all(len(p) == 1 for p in by_card.values())

if len(seen) == N and same_partition:
    print("OK")
else:
    print(f"FAILED: read {len(seen)}/{N} payments back; one partition per card: {same_partition}")
    sys.exit(1)
