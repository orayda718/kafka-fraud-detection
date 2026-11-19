#!/usr/bin/env bash
# Stops one broker abruptly to test fault tolerance. Usage: ./scripts/kill-broker.sh 2
# Bring it back with: docker start kafka<N>
set -euo pipefail
N="${1:-2}"
echo "$(date +%H:%M:%S)  killing kafka$N"
docker kill "kafka$N"
