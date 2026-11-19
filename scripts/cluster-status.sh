#!/usr/bin/env bash
# Shows KRaft quorum status and which broker leads each partition.
set -euo pipefail
docker exec kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka1:19092 describe --status
echo
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --describe
