#!/usr/bin/env python3
"""Measure how the cluster recovers when a broker fails.

Each run:
  1. makes sure all 3 brokers are up, every partition of `recovery-test` has
     3 in-sync replicas, and leadership is back on each partition's preferred
     replica (a restarted broker comes back as a follower, so without this
     only the first run would fail a broker that leads partitions),
  2. produces numbered messages at a fixed rate (acks=all by default) while a
     consumer reads them back and an admin client polls partition metadata,
  3. after a warm-up, kills one broker (`docker kill`, a crash) or stops it
     (`docker stop`, a graceful shutdown),
  4. restarts it after an observation window and waits for its replicas to
     catch up (ISR back to 3).

Measured per run (appended to experiments/results/recovery/summary.csv):
  leader_failover_s    failure -> every partition has a leader on a live broker
  producer_stall_s     longest gap with no successful ack at all after the failure
  partitions_stalled   partitions whose acks paused > 1 s after the failure
  worst_partition_stall_s  longest ack gap on any single partition
  consumer_stall_s     longest gap between received messages after the failure
  baseline_gap_s       longest ack gap before the failure (normal jitter, for comparison)
  p99_ack_ms_before / p99_ack_ms_after   producer ack latency before / after the failure
  lost_acked           messages Kafka acknowledged but the consumer never received
  duplicates           messages the consumer received more than once
  isr_restore_s        broker restart -> all partitions back to 3 in-sync replicas

A 100 ms timeline of each run is saved as timeline_<run_id>.csv for plotting
(see plot_recovery.py).

Usage (from the repo root, cluster running, venv active):
  python experiments/recovery/measure_recovery.py                 # 3 runs, crash broker 2
  python experiments/recovery/measure_recovery.py --mode stop     # graceful shutdown instead
  python experiments/recovery/measure_recovery.py --acks 1        # compare acknowledgement settings
  python experiments/recovery/measure_recovery.py --broker controller   # fail the active KRaft controller
"""
import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
TOPIC = "recovery-test"
BROKERS = (1, 2, 3)
RESULTS = Path(__file__).resolve().parents[1] / "results" / "recovery"

SUMMARY_FIELDS = [
    "started_at", "run_id", "mode", "acks", "victim", "victim_was_controller",
    "partitions", "partitions_led_by_victim", "rate",
    "leader_failover_s", "producer_stall_s", "partitions_stalled", "worst_partition_stall_s",
    "consumer_stall_s", "baseline_gap_s",
    "p99_ack_ms_before", "p99_ack_ms_after", "max_ack_ms",
    "sent", "acked", "delivery_errors", "consumed_unique", "duplicates", "lost_acked",
    "isr_restore_s",
]


def now():
    return time.monotonic()


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def controller_id():
    """Node id of the KRaft quorum leader (the active controller)."""
    for b in BROKERS:
        out = docker("exec", f"kafka{b}", "/opt/kafka/bin/kafka-metadata-quorum.sh",
                     "--bootstrap-server", f"kafka{b}:19092", "describe", "--status").stdout
        for line in out.splitlines():
            if line.strip().startswith("LeaderId:"):
                return int(line.split(":")[1])
    return None


def partition_state(admin):
    """{partition: (leader_id, isr_count)}, or None if metadata is unavailable."""
    try:
        md = admin.list_topics(TOPIC, timeout=1.0)
    except Exception:
        return None
    topic = md.topics.get(TOPIC)
    if topic is None or topic.error is not None:
        return None
    return {p.id: (p.leader, len(p.isrs)) for p in topic.partitions.values()}


def ensure_topic(admin, partitions):
    state = partition_state(admin)
    if state is not None:
        if len(state) != partitions:
            sys.exit(f"Topic {TOPIC} has {len(state)} partitions; delete it or pass "
                     f"--partitions {len(state)}.")
        return
    new = NewTopic(TOPIC, num_partitions=partitions, replication_factor=3,
                   config={"min.insync.replicas": "2"})
    for fut in admin.create_topics([new]).values():
        fut.result()


def wait_healthy(admin, partitions, timeout):
    deadline = now() + timeout
    while now() < deadline:
        state = partition_state(admin)
        if state and len(state) == partitions and all(
                leader >= 0 and isr == 3 for leader, isr in state.values()):
            return state
        time.sleep(0.5)
    return None


def restore_preferred_leaders(admin, timeout=60):
    """Move every partition's leadership back to its preferred (first) replica."""
    docker("exec", "kafka1", "/opt/kafka/bin/kafka-leader-election.sh",
           "--bootstrap-server", "kafka1:19092", "--election-type", "PREFERRED",
           "--all-topic-partitions")
    deadline = now() + timeout
    while now() < deadline:
        try:
            parts = admin.list_topics(TOPIC, timeout=2.0).topics[TOPIC].partitions.values()
            if all(p.leader == p.replicas[0] and len(p.isrs) == 3 for p in parts):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def percentile(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(q / 100 * (len(values) - 1))))]


def max_gap(times, start, end):
    """Longest interval between start and end with no event."""
    pts = [start] + sorted(t for t in times if start < t <= end) + [end]
    return max(b - a for a, b in zip(pts, pts[1:]))


def run_once(args):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for b in BROKERS:
        docker("start", f"kafka{b}")
    ensure_topic(admin, args.partitions)
    state = wait_healthy(admin, args.partitions, timeout=180)
    if state is None:
        sys.exit("Cluster not healthy (some partitions lack 3 in-sync replicas). "
                 "Check ./scripts/cluster-status.sh")

    if not restore_preferred_leaders(admin):
        print("  warning: leaders did not return to their preferred replicas")
    state = partition_state(admin) or state
    ctrl = controller_id()
    if args.broker == "controller" and ctrl is None:
        sys.exit("Couldn't find the active controller (kafka-metadata-quorum.sh failed).")
    victim = ctrl if args.broker == "controller" else int(args.broker)
    led_by_victim = sorted(p for p, (leader, _) in state.items() if leader == victim)
    run_id = uuid.uuid4().hex[:8]
    started_at = datetime.now().isoformat(timespec="seconds")
    print(f"\n=== run {run_id}: {args.mode} kafka{victim} "
          f"(leads partitions {led_by_victim}; active controller = node {ctrl})")

    acks = []        # (ack_time, latency_s, seq, partition)
    errors = []      # (time, error)
    received = {}    # seq -> first receive time
    meta = []        # (time, {partition: (leader, isr_count)})
    counters = {"sent": 0, "duplicates": 0}
    stop_producing, stop_consuming, stop_meta = (threading.Event() for _ in range(3))

    def producer_loop():
        p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": args.acks, "linger.ms": 5,
                      "enable.idempotence": args.acks == "all"})

        def on_delivery(err, sent_t, seq, partition):
            t = now()
            if err is not None:
                errors.append((t, str(err)))
            else:
                acks.append((t, t - sent_t, seq, partition))

        interval, next_t, seq = 1.0 / args.rate, now(), 0
        while not stop_producing.is_set():
            t = now()
            if t < next_t:
                p.poll(min(next_t - t, 0.01))
                continue
            if t - next_t > 1.0:  # don't burst to "catch up" after a long stall
                next_t = t
            value = json.dumps({"run": run_id, "seq": seq}).encode()
            try:
                p.produce(TOPIC, key=str(seq % 1000), value=value,
                          on_delivery=lambda err, m, st=t, s=seq: on_delivery(
                              err, st, s, m.partition() if m is not None else -1))
            except BufferError:
                p.poll(0.05)
                continue
            seq += 1
            next_t += interval
            p.poll(0)
        p.flush(120)
        counters["sent"] = seq

    def consumer_loop():
        c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"recovery-{run_id}",
                      "auto.offset.reset": "earliest", "enable.auto.commit": False})
        c.subscribe([TOPIC])
        while not stop_consuming.is_set():
            msg = c.poll(0.1)
            if msg is None or msg.error():
                continue
            try:
                v = json.loads(msg.value())
            except ValueError:
                continue
            if v.get("run") != run_id:
                continue  # left over from an earlier run
            if v["seq"] in received:
                counters["duplicates"] += 1
            else:
                received[v["seq"]] = now()
        c.close()

    def meta_loop():
        a = AdminClient({"bootstrap.servers": BOOTSTRAP})
        while not stop_meta.is_set():
            s = partition_state(a)
            if s is not None:
                meta.append((now(), s))
            time.sleep(0.1)

    consumer_t = threading.Thread(target=consumer_loop, daemon=True)
    meta_t = threading.Thread(target=meta_loop, daemon=True)
    producer_t = threading.Thread(target=producer_loop, daemon=True)
    t0 = now()
    for th in (consumer_t, meta_t, producer_t):
        th.start()

    time.sleep(args.warmup)
    t_kill = now()
    docker(args.mode, f"kafka{victim}")
    print(f"  {args.mode} kafka{victim} at t={t_kill - t0:.1f}s, broker down for {args.observe:.0f}s")
    time.sleep(args.observe)

    t_restart = now()
    docker("start", f"kafka{victim}")
    print(f"  restarted kafka{victim} at t={t_restart - t0:.1f}s, waiting for ISR to recover")
    t_restored = None
    while now() - t_restart < args.restore_timeout:
        if meta and meta[-1][0] > t_restart and all(
                isr == 3 for _, isr in meta[-1][1].values()):
            t_restored = meta[-1][0]
            break
        time.sleep(0.2)

    stop_producing.set()
    producer_t.join()
    time.sleep(args.drain)  # let the consumer read everything that was acked
    stop_consuming.set()
    stop_meta.set()
    consumer_t.join()
    meta_t.join()
    t_end = now()

    # --- metrics -------------------------------------------------------------
    failover = 0.0 if not led_by_victim else None
    if led_by_victim:
        for t, s in meta:
            if t_kill < t < t_restart and all(
                    leader not in (-1, victim) for leader, _ in s.values()):
                failover = t - t_kill
                break

    ack_times = [t for t, *_ in acks]
    by_partition = {}
    for t, _, _, part in acks:
        by_partition.setdefault(part, []).append(t)
    part_gaps = [max_gap(ts, t_kill, t_restart) for ts in by_partition.values()]
    recv_times = list(received.values())
    before = [lat for t, lat, *_ in acks if t0 + 2 < t <= t_kill]
    after = [lat for t, lat, *_ in acks if t_kill < t <= t_restart]
    lost = len({s for _, _, s, _ in acks} - received.keys())

    row = {
        "started_at": started_at, "run_id": run_id, "mode": args.mode, "acks": args.acks,
        "victim": victim, "victim_was_controller": ctrl == victim,
        "partitions": args.partitions, "partitions_led_by_victim": len(led_by_victim),
        "rate": args.rate,
        "leader_failover_s": None if failover is None else round(failover, 2),
        "producer_stall_s": round(max_gap(ack_times, t_kill, t_restart), 2),
        "partitions_stalled": sum(1 for g in part_gaps if g > 1.0),
        "worst_partition_stall_s": round(max(part_gaps), 2) if part_gaps else None,
        "consumer_stall_s": round(max_gap(recv_times, t_kill, t_restart), 2),
        "baseline_gap_s": round(max_gap(ack_times, t0 + 2, t_kill), 2),
        "p99_ack_ms_before": round(1000 * percentile(before, 99), 1) if before else None,
        "p99_ack_ms_after": round(1000 * percentile(after, 99), 1) if after else None,
        "max_ack_ms": round(1000 * max(lat for _, lat, *_ in acks), 1) if acks else None,
        "sent": counters["sent"], "acked": len(acks), "delivery_errors": len(errors),
        "consumed_unique": len(received), "duplicates": counters["duplicates"],
        "lost_acked": lost,
        "isr_restore_s": None if t_restored is None else round(t_restored - t_restart, 2),
    }

    # --- timeline (100 ms buckets) for plotting ------------------------------
    RESULTS.mkdir(parents=True, exist_ok=True)
    timeline = RESULTS / f"timeline_{run_id}.csv"
    n = int((t_end - t0) / 0.1) + 1
    ack_b, recv_b = [0] * n, [0] * n
    for t in ack_times:
        ack_b[min(n - 1, int((t - t0) / 0.1))] += 1
    for t in recv_times:
        recv_b[min(n - 1, int((t - t0) / 0.1))] += 1
    with timeline.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t_s", "acked", "consumed", "partitions_without_live_leader",
                    "min_isr", "event"])
        mi = 0
        for i in range(n):
            t = t0 + i * 0.1
            while mi + 1 < len(meta) and meta[mi + 1][0] <= t:
                mi += 1
            down = victim if t_kill <= t < t_restart else None
            no_leader = min_isr = ""
            if meta and meta[mi][0] <= t:
                s = meta[mi][1]
                no_leader = sum(1 for leader, _ in s.values() if leader in (-1, down))
                min_isr = min(isr for _, isr in s.values())
            event = ""
            if t <= t_kill < t + 0.1:
                event = f"{args.mode} kafka{victim}"
            elif t <= t_restart < t + 0.1:
                event = f"restart kafka{victim}"
            w.writerow([round(i * 0.1, 1), ack_b[i], recv_b[i], no_leader, min_isr, event])

    for k in ("leader_failover_s", "producer_stall_s", "partitions_stalled",
              "worst_partition_stall_s", "consumer_stall_s", "lost_acked", "duplicates",
              "isr_restore_s"):
        print(f"  {k:18} {row[k]}")
    print(f"  timeline saved to {timeline}")
    return row


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--broker", default="2", choices=("1", "2", "3", "controller"),
                    help="broker to fail; 'controller' = whichever node is the active controller")
    ap.add_argument("--mode", choices=("kill", "stop"), default="kill",
                    help="kill = crash (SIGKILL), stop = graceful shutdown")
    ap.add_argument("--acks", choices=("0", "1", "all"), default="all")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--rate", type=int, default=500, help="messages per second")
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--warmup", type=float, default=10, help="seconds before the failure")
    ap.add_argument("--observe", type=float, default=30, help="seconds the broker stays down")
    ap.add_argument("--restore-timeout", type=float, default=120)
    ap.add_argument("--drain", type=float, default=10)
    args = ap.parse_args()

    if docker("ps").returncode != 0:
        sys.exit("Docker isn't running. Start Docker Desktop and run `docker compose up -d`.")

    rows = [run_once(args) for _ in range(args.runs)]

    summary = RESULTS / "summary.csv"
    new_file = not summary.exists()
    with summary.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        if new_file:
            w.writeheader()
        w.writerows(rows)

    print(f"\n=== {len(rows)} runs ({args.mode}, acks={args.acks}) appended to {summary}")
    for k in ("leader_failover_s", "producer_stall_s", "worst_partition_stall_s",
              "consumer_stall_s", "isr_restore_s"):
        vals = [r[k] for r in rows if r[k] is not None]
        if vals:
            sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
            print(f"  {k:18} mean {statistics.mean(vals):.2f} s  ± {sd:.2f}")
    print(f"  lost_acked total   {sum(r['lost_acked'] for r in rows)}")
    print(f"  duplicates total   {sum(r['duplicates'] for r in rows)}")


if __name__ == "__main__":
    main()
