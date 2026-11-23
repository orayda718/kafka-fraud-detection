# Kafka Fraud Detection

COMPSCI 4411 (Group 25): **Apache Kafka: Architecture, Stream Processing, and a Real-Time Fraud Detection Case Study**

A real-time card-payment fraud detector built with Kafka Streams, running on a local 3-broker Apache Kafka cluster (KRaft mode, no ZooKeeper).

```
Payment simulator (Python) ──▶ payments ──▶ Fraud detector (Kafka Streams) ──▶ fraud-alerts ──▶ Dashboard (Streamlit)
          └── seeds ──▶ card-profiles (compacted) ──┘
```

| Component | Tech | Location | Status |
|---|---|---|---|
| Cluster + Kafka UI | Docker Compose, Apache Kafka 4.1.0, Kafbat UI 1.5.0 | `docker-compose.yml`, `scripts/` | ready |
| Fraud detector | Java 17, Kafka Streams, Maven | `detector/` | in progress |
| Payment simulator | Python, confluent-kafka | `simulator/` | in progress |
| Live dashboard | Streamlit | `dashboard/` | in progress |
| Experiments | Python | `experiments/` | in progress |

All data comes from the simulator (fixed random seed). There is no external dataset.

## Topics

| Topic | Key | Partitions | Replication | Config |
|---|---|---|---|---|
| `payments` | `card_id` | 6 | 3 | `min.insync.replicas=2` |
| `card-profiles` | `card_id` | 6 | 3 | `cleanup.policy=compact`, `min.insync.replicas=2` |
| `fraud-alerts` | `card_id` | 6 | 3 | `min.insync.replicas=2` |

`payments` and `card-profiles` have the same partition count so the detector's stream-table join is co-partitioned.

## Fraud rules

| Rule | Fires when | Default |
|---|---|---|
| `VELOCITY` | more than N payments on one card within a window | N = 5, 60 s |
| `GEO_MISMATCH` | two payments on one card from different countries within T | T = 10 min |
| `AMOUNT_SPIKE` | amount > K × the card's running average, after M payments | K = 5, M = 5 |
| `OVER_LIMIT` | amount > the card's `spending_limit` | from profile |

Thresholds are in `detector/src/main/resources/rules.properties`.

## Prerequisites

- Docker Desktop, with at least 4 GB of memory (Settings → Resources)
- Java 17+ and Maven: `brew install openjdk@17 maven`
- Python 3.10+

## Setup

Run everything from the repo root.

```bash
# 1. Start the cluster (the first run downloads the images)
docker compose up -d
docker compose ps                  # kafka1, kafka2, kafka3, kafka-ui should be running

# 2. Create the topics (safe to re-run)
./scripts/create-topics.sh

# 3. Check the KRaft quorum and partition leaders
./scripts/cluster-status.sh        # Kafka UI: http://localhost:8080

# 4. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python simulator/smoke_test.py     # should end with "OK"

# 5. Build, test and run the detector (Ctrl+C to stop)
cd detector
mvn -q package                     # also runs the unit tests
java -jar target/fraud-detector-0.1.0-jar-with-dependencies.jar
```

### Detector options (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `BOOTSTRAP` | `localhost:9092,localhost:9094,localhost:9096` | Kafka bootstrap servers |
| `GUARANTEE` | `exactly_once_v2` | set to `at_least_once` for the duplicate-alert comparison |
| `RULES_FILE` | bundled `rules.properties` | file with threshold overrides, no rebuild needed |

Example: `GUARANTEE=at_least_once java -jar target/fraud-detector-0.1.0-jar-with-dependencies.jar`

## Live demo

```bash
./scripts/demo.sh                  # press Enter between steps
DEMO_AUTO=1 ./scripts/demo.sh      # rehearsal mode, no pauses
```

The script starts the detector (plus the simulator and dashboard once they're in the repo) and walks through:

1. the cluster in Kafka UI: 3 brokers, partitions, replicas
2. live payments on the dashboard
3. a `burst` injection → `VELOCITY` alert
4. a `travel` injection → `GEO_MISMATCH` alert
5. killing broker 2: payments keep flowing and partition leaders move
6. restarting the detector: no duplicate alerts, state restored from changelog topics

It stops everything it started and restarts the killed broker when it exits. Logs go to `demo-logs/`.

## Experiments

### Broker failure and recovery time

```bash
python experiments/recovery/measure_recovery.py                 # 3 runs, crash broker 2 (docker kill)
python experiments/recovery/measure_recovery.py --mode stop     # graceful shutdown (docker stop)
python experiments/recovery/measure_recovery.py --acks 1        # compare acknowledgement settings
python experiments/recovery/measure_recovery.py --broker controller   # fail the active KRaft controller
python experiments/recovery/plot_recovery.py                    # plot the latest run
```

Each run first moves partition leadership back to the preferred replicas (a restarted broker rejoins as a follower, so otherwise later runs would fail a broker that leads nothing). It then sends numbered messages to a separate `recovery-test` topic at 500 msg/s, fails a broker after 10 s, restarts it 30 s later and records:

| Metric | Meaning |
|---|---|
| `leader_failover_s` | failure → every partition has a leader on a live broker |
| `producer_stall_s` / `consumer_stall_s` | longest gap with no acks / no received messages at all after the failure |
| `partitions_stalled` / `worst_partition_stall_s` | partitions whose acks paused for more than 1 s, and the longest pause on any one partition |
| `p99_ack_ms_before` / `p99_ack_ms_after` | producer acknowledgement latency before and after the failure |
| `lost_acked` | messages Kafka acknowledged but the consumer never received (expect 0 with `acks=all`) |
| `duplicates` | messages received more than once |
| `isr_restore_s` | broker restart → all partitions back to 3 in-sync replicas |

Results are appended to `experiments/results/recovery/summary.csv`, with one 100 ms timeline CSV and plot per run. Run `--help` for all options (broker, rate, timings).

A crashed broker is only declared dead after the controller stops receiving its heartbeats (`broker.session.timeout.ms`, 9 s by default), so `kill` fails over much more slowly than a graceful `stop`.

## Useful commands

```bash
./scripts/kill-broker.sh 2         # crash a broker
docker start kafka2                # bring it back
docker compose logs -f kafka1      # follow a broker's log
docker compose down                # stop everything (topic data is kept)
docker compose down -v             # stop and delete all topic data (clean start)
```

## Repository layout

```
docker-compose.yml        3 Kafka brokers (KRaft) + Kafka UI
scripts/                  topics, cluster status, broker kill, demo
detector/                 Kafka Streams fraud detector (Maven)
simulator/                payment simulator (Python)
dashboard/                Streamlit dashboard
experiments/              benchmarks, recovery and exactly-once checks; results/ holds CSVs and plots
```

## Contributing

Each person works on their own branch (`infra`, `simulator-dashboard`, `detector-core`, `detector-state`, `bench-verify`) and merges into `main` through a pull request reviewed by a teammate. Don't push directly to `main`. If you need to change a topic, schema or rule threshold, tell the group first.
