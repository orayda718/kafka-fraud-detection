# Kafka Fraud Detection

COMPSCI 4411 (Group 25): **Apache Kafka: Architecture, Stream Processing, and a Real-Time Fraud Detection Case Study**

A 3-broker Kafka cluster (KRaft) running a real-time payment fraud detector built with Kafka Streams.

| Component | Tech | Folder |
|---|---|---|
| Cluster + Kafka UI | Docker Compose, Apache Kafka 4.1 | `docker-compose.yml` |
| Payment simulator | Python, confluent-kafka | `simulator/` |
| Fraud detector | Java 17, Kafka Streams, Maven | `detector/` |
| Live dashboard | Streamlit | `dashboard/` |
| Experiments | Python scripts + results | `experiments/` |

Topics: `payments`, `card-profiles` (log-compacted), `fraud-alerts`, all with 6 partitions, replication factor 3 and `min.insync.replicas=2`.

## Prerequisites

- Docker Desktop (give it at least 4 GB of memory)
- Java 17+ and Maven (`brew install openjdk@17 maven`)
- Python 3.10+

## Setup

```bash
# 1. Start the cluster (first run downloads the images)
docker compose up -d
docker compose ps                 # kafka1-3 and kafka-ui should be "running"

# 2. Create the topics
./scripts/create-topics.sh

# 3. Check the KRaft quorum and partition leaders
./scripts/cluster-status.sh       # Kafka UI: http://localhost:8080

# 4. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python simulator/smoke_test.py    # should print "OK"

# 5. Build and run the detector (Ctrl+C to stop)
cd detector
mvn -q package
java -jar target/fraud-detector-0.1.0-jar-with-dependencies.jar
```

With the detector running, run `python simulator/smoke_test.py` again in another terminal. The detector should print the payments.

## Useful commands

```bash
./scripts/kill-broker.sh 2        # kill a broker mid-stream
docker start kafka2               # bring it back
docker compose down               # stop everything (keeps data)
docker compose down -v            # stop and wipe all topic data
```
