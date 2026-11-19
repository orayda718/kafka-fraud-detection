#!/usr/bin/env bash
# Creates the three project topics. Safe to re-run.
# payments and card-profiles must have the SAME partition count so the
# KStream-KTable join in the detector is co-partitioned (both keyed by card ID).
set -euo pipefail
PARTITIONS="${PARTITIONS:-6}"
KT="docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092"

$KT --create --if-not-exists --topic payments \
    --partitions "$PARTITIONS" --replication-factor 3 \
    --config min.insync.replicas=2

$KT --create --if-not-exists --topic card-profiles \
    --partitions "$PARTITIONS" --replication-factor 3 \
    --config cleanup.policy=compact --config min.insync.replicas=2

$KT --create --if-not-exists --topic fraud-alerts \
    --partitions "$PARTITIONS" --replication-factor 3 \
    --config min.insync.replicas=2

$KT --describe --topic payments --topic card-profiles --topic fraud-alerts 2>/dev/null || $KT --list
