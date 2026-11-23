#!/usr/bin/env bash
# Runs the live demo from TEAM_TASKS.md section 7, one step at a time.
# Press Enter to move to the next step (or run with DEMO_AUTO=1 to rehearse
# without pauses). Background processes are stopped when the script exits.
#
#   ./scripts/demo.sh
#
# The simulator and dashboard commands are set below; adjust them if their
# command-line options change.

set -uo pipefail
cd "$(dirname "$0")/.."

SIMULATOR_CMD=${SIMULATOR_CMD:-"python simulator/simulator.py"}
INJECT_CMD=${INJECT_CMD:-"python simulator/simulator.py --inject"}   # + burst|travel|spike|limit
DASHBOARD_CMD=${DASHBOARD_CMD:-"streamlit run dashboard/app.py"}
DETECTOR_JAR=detector/target/fraud-detector-0.1.0-jar-with-dependencies.jar
VICTIM=${VICTIM:-2}   # 2 or 3 (the helper commands run inside kafka1)
LOGS=demo-logs
KBIN="docker exec kafka1 /opt/kafka/bin"
BOOT="--bootstrap-server kafka1:19092"

[[ "$VICTIM" == "1" ]] && { echo "Use VICTIM=2 or 3: the demo runs its Kafka commands inside kafka1."; exit 1; }
mkdir -p "$LOGS"
PIDS=()
DETECTOR_PID=""

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
pause() {
  if [[ "${DEMO_AUTO:-0}" == "1" ]]; then sleep 5; else read -rp $'\n[Enter] next step ' _; fi
}
cleanup() {
  bold "Stopping demo processes"
  for pid in "${PIDS[@]}" $DETECTOR_PID; do kill "$pid" 2>/dev/null; done
  docker start "kafka$VICTIM" >/dev/null 2>&1
  echo "Logs are in $LOGS/"
}
trap cleanup EXIT

start_detector() {
  java -jar "$DETECTOR_JAR" >>"$LOGS/detector.log" 2>&1 &
  DETECTOR_PID=$!
  echo "Detector started (pid $DETECTOR_PID, log: $LOGS/detector.log)"
}

alert_count() {
  # Sum of end offsets of fraud-alerts across partitions (includes transaction
  # markers, so compare before/after rather than reading it as an exact count).
  $KBIN/kafka-get-offsets.sh $BOOT --topic fraud-alerts 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}

payment_count() {
  $KBIN/kafka-get-offsets.sh $BOOT --topic payments 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}

show_alerts() {
  echo "Latest alerts (read_committed):"
  $KBIN/kafka-console-consumer.sh $BOOT --topic fraud-alerts --from-beginning \
    --isolation-level read_committed --timeout-ms 4000 2>/dev/null | tail -n "${1:-5}"
}

inject() {
  if [[ -f simulator/simulator.py ]]; then
    echo "\$ $INJECT_CMD $1"
    $INJECT_CMD "$1"
  else
    echo "(simulator/simulator.py isn't in the repo yet, skipping the '$1' injection)"
  fi
}

# --- 0. Preconditions ----------------------------------------------------------
bold "0. Checking the cluster"
docker compose up -d >/dev/null
./scripts/create-topics.sh >/dev/null 2>&1
if [[ ! -f "$DETECTOR_JAR" ]]; then
  echo "Building the detector..."
  (cd detector && mvn -q package -DskipTests) || exit 1
fi

# --- 1. The cluster ------------------------------------------------------------
bold "1. Three brokers, KRaft quorum, topics and replicas"
./scripts/cluster-status.sh
echo
echo "Show this in Kafka UI: http://localhost:8080 (Brokers, Topics > payments > Partitions)"
pause

# --- 2. Pipeline running -------------------------------------------------------
bold "2. Start the detector, simulator and dashboard"
start_detector
if [[ -f simulator/simulator.py ]]; then
  $SIMULATOR_CMD >>"$LOGS/simulator.log" 2>&1 &
  PIDS+=($!); echo "Simulator started (log: $LOGS/simulator.log)"
else
  echo "(simulator/simulator.py isn't in the repo yet)"
fi
if [[ -f dashboard/app.py ]]; then
  $DASHBOARD_CMD >>"$LOGS/dashboard.log" 2>&1 &
  PIDS+=($!); echo "Dashboard starting at http://localhost:8501"
else
  echo "(dashboard/app.py isn't in the repo yet)"
fi
sleep 5
echo "Payments in the log so far: $(payment_count)"
pause

# --- 3. VELOCITY ---------------------------------------------------------------
bold "3. Inject a burst -> VELOCITY alert"
inject burst
sleep 5
show_alerts 3
pause

# --- 4. GEO_MISMATCH -----------------------------------------------------------
bold "4. Inject impossible travel -> GEO_MISMATCH alert"
inject travel
sleep 5
show_alerts 3
pause

# --- 5. Broker failure ---------------------------------------------------------
bold "5. Kill broker $VICTIM mid-stream"
before=$(payment_count)
./scripts/kill-broker.sh "$VICTIM"
echo "Waiting 15 s for leaders to move..."
sleep 15
./scripts/cluster-status.sh 2>/dev/null | grep -E "LeaderId|Topic: payments" | head -n 8
after=$(payment_count)
echo "Payments still flowing: $before -> $after"
echo "Kafka UI now shows 2 brokers; partitions that kafka$VICTIM led have new leaders."
inject spike
sleep 5
show_alerts 3
pause

# --- 6. Detector restart -------------------------------------------------------
bold "6. Restart the detector: no duplicate alerts, state restored from changelogs"
before=$(alert_count)
kill "$DETECTOR_PID" 2>/dev/null; wait "$DETECTOR_PID" 2>/dev/null
echo "Detector stopped. Alert topic offset: $before"
sleep 3
start_detector
sleep 15
after=$(alert_count)
echo "Alert topic offset after restart: $after"
echo "With exactly-once, the restart itself adds no duplicate alerts (experiments/verify checks this exactly)."
echo "State stores are rebuilt from their changelog topics:"
$KBIN/kafka-topics.sh $BOOT --list | grep -- "-changelog" || echo "(no changelog topics yet)"
pause

bold "Bringing kafka$VICTIM back"
docker start "kafka$VICTIM" >/dev/null && echo "kafka$VICTIM restarted; replicas will catch up (ISR back to 3)."
bold "Demo finished"
