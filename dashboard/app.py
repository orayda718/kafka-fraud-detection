"""Live fraud-monitoring dashboard (Streamlit).

Reads `payments` and `fraud-alerts` with its own consumer group, so it gets its
own copy of every event without taking anything away from the detector
(`fraud-detector` group). Run from the repo root:

    streamlit run dashboard/app.py

Environment variables (all optional):
    BOOTSTRAP               Kafka bootstrap servers (default: the 3 local brokers)
    DASHBOARD_GROUP         consumer group id (default: fraud-dashboard)
    DASHBOARD_OFFSET_RESET  where a brand-new group starts: earliest (default) or latest
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter, OrderedDict, deque
from datetime import datetime

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
GROUP_ID = os.environ.get("DASHBOARD_GROUP", "fraud-dashboard")
OFFSET_RESET = os.environ.get("DASHBOARD_OFFSET_RESET", "earliest")
PAYMENTS, ALERTS = "payments", "fraud-alerts"
RULES = ["VELOCITY", "GEO_MISMATCH", "AMOUNT_SPIKE", "OVER_LIMIT"]
RATE_WINDOW_S = 10


class LiveState:
    """Everything the page shows. Filled by one background consumer thread."""

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()
        self.status = "connecting"
        self.error = ""
        self.assigned: list[str] = []
        self.last_msg_at = 0.0

    def reset(self):
        self.payments = deque(maxlen=60)            # newest payments for the live feed
        self.alerts = deque(maxlen=200)             # newest alerts
        self.card_stats: dict[str, list] = {}       # card_id -> [count, total]
        self.card_alerts = Counter()                # card_id -> alerts
        self.rule_counts = Counter()
        self.total_payments = 0
        self.event_times = deque(maxlen=200_000)    # payment timestamp_ms, for payments/sec
        self.payment_ts = OrderedDict()             # payment_id -> timestamp_ms (bounded)
        self.flagged = OrderedDict()                # payment_ids named in alerts (bounded)
        self.latencies = deque(maxlen=200)          # detected_at_ms - payment timestamp_ms

    def add_payment(self, p: dict):
        card = p.get("card_id", "?")
        amount = float(p.get("amount", 0))
        stats = self.card_stats.setdefault(card, [0, 0.0])
        stats[0] += 1
        stats[1] += amount
        self.total_payments += 1
        ts = int(p.get("timestamp_ms", 0))
        self.event_times.append(ts)
        self.payment_ts[p.get("payment_id")] = ts
        if len(self.payment_ts) > 100_000:
            self.payment_ts.popitem(last=False)
        self.payments.appendleft(p)

    def add_alert(self, a: dict):
        self.alerts.appendleft(a)
        self.rule_counts[a.get("rule", "?")] += 1
        self.card_alerts[a.get("card_id", "?")] += 1
        known = [self.payment_ts[i] for i in a.get("payment_ids", []) if i in self.payment_ts]
        if known and a.get("detected_at_ms"):
            # Latency from the payment that completed the pattern (the latest one).
            self.latencies.append(int(a["detected_at_ms"]) - max(known))
        for pid in a.get("payment_ids", []):
            self.flagged[pid] = True
        while len(self.flagged) > 20_000:
            self.flagged.popitem(last=False)


def consume_forever(state: LiveState):
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": True,
        # The detector writes alerts inside Kafka transactions (exactly-once), so only
        # show alerts from committed transactions; aborted attempts never appear.
        "isolation.level": "read_committed",
        "client.id": "fraud-dashboard",
    })

    def on_assign(_, partitions):
        with state.lock:
            state.assigned = sorted(f"{p.topic}[{p.partition}]" for p in partitions)

    def on_revoke(_, partitions):
        with state.lock:
            state.assigned = []

    consumer.subscribe([PAYMENTS, ALERTS], on_assign=on_assign, on_revoke=on_revoke)
    while True:
        try:
            msgs = consumer.consume(num_messages=500, timeout=0.5)
        except Exception as e:  # keep the dashboard alive through broker restarts
            with state.lock:
                state.status, state.error = "error", str(e)
            time.sleep(1)
            continue
        with state.lock:
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        state.error = str(msg.error())
                    continue
                try:
                    event = json.loads(msg.value())
                except (TypeError, ValueError):
                    continue
                if msg.topic() == PAYMENTS:
                    state.add_payment(event)
                else:
                    state.add_alert(event)
                state.last_msg_at = time.time()
            if msgs or state.status == "connecting":
                state.status = "connected" if state.assigned else state.status


@st.cache_resource
def live_state() -> LiveState:
    """One consumer per Streamlit server, shared by every open browser tab."""
    state = LiveState()
    threading.Thread(target=consume_forever, args=(state,), daemon=True).start()
    return state


def hhmmss(ms) -> str:
    return datetime.fromtimestamp(int(ms) / 1000).strftime("%H:%M:%S")


def red_rows(mask):
    """Styler helper: highlight rows where mask is True."""
    def style(row):
        hit = bool(mask.loc[row.name])
        return ["background-color: #fde2e2; color: #8b0000; font-weight: 600" if hit else ""] * len(row)
    return style


st.set_page_config(page_title="Fraud monitor", layout="wide")
state = live_state()

with st.sidebar:
    st.header("Kafka")
    st.caption(f"Bootstrap: `{BOOTSTRAP}`")
    st.caption(f"Consumer group: `{GROUP_ID}` (new groups start at `{OFFSET_RESET}`)")
    st.caption("Reads alerts with `isolation.level=read_committed`.")
    if st.button("Clear view"):
        with state.lock:
            state.reset()

st.title("Real-time card fraud monitor")
st.caption("payments → Kafka Streams fraud detector → fraud-alerts, all read live from Kafka")


@st.fragment(run_every="1s")
def live_view():
    now_ms = time.time() * 1000
    with state.lock:
        recent = sum(1 for t in state.event_times if now_ms - RATE_WINDOW_S * 1000 <= t <= now_ms + 1000)
        rate = recent / RATE_WINDOW_S
        # Events arrive interleaved across partitions, so sort by their own timestamps.
        payments = sorted(state.payments, key=lambda p: p.get("timestamp_ms", 0), reverse=True)
        alerts = sorted(state.alerts, key=lambda a: a.get("detected_at_ms", 0), reverse=True)
        flagged = set(state.flagged)
        rule_counts = dict(state.rule_counts)
        total_payments = state.total_payments
        lat = sorted(state.latencies)
        top = sorted(state.card_stats.items(), key=lambda kv: kv[1][0], reverse=True)[:15]
        card_alerts = dict(state.card_alerts)
        status, error, assigned = state.status, state.error, list(state.assigned)
        idle = time.time() - state.last_msg_at if state.last_msg_at else None

    with st.sidebar:
        st.subheader("Consumer status")
        st.write(f"Status: **{status}**" + (f" · last message {idle:.0f}s ago" if idle is not None else ""))
        if error:
            st.caption(f"Last error: {error}")
        st.caption(f"Partitions assigned to this consumer ({len(assigned)}):")
        st.code("\n".join(assigned) or "(none yet)", language=None)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Payments / sec", f"{rate:.1f}", help=f"By payment event time, last {RATE_WINDOW_S} s")
    c2.metric("Payments seen", f"{total_payments:,}")
    c3.metric("Fraud alerts", f"{sum(rule_counts.values()):,}")
    p50 = f"{lat[len(lat) // 2] / 1000:.2f} s" if lat else "n/a"
    c4.metric("Detection latency (p50)", p50, help="detected_at_ms minus the payment's timestamp_ms")

    rc = st.columns(4)
    for col, rule in zip(rc, RULES):
        col.metric(rule, rule_counts.get(rule, 0))

    st.subheader("Fraud alerts")
    if alerts:
        adf = pd.DataFrame([{
            "detected": hhmmss(a["detected_at_ms"]),
            "card": a.get("card_id"),
            "rule": a.get("rule"),
            "reason": a.get("reason"),
            "payments": len(a.get("payment_ids", [])),
        } for a in alerts[:25]])
        st.dataframe(adf.style.apply(red_rows(pd.Series(True, index=adf.index)), axis=1),
                     hide_index=True, width="stretch")
    else:
        st.info("No alerts yet. Inject fraud in the simulator (burst, travel, spike, limit).")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Live payments")
        if payments:
            pdf = pd.DataFrame([{
                "time": hhmmss(p["timestamp_ms"]),
                "card": p.get("card_id"),
                "amount": float(p.get("amount", 0)),
                "currency": p.get("currency"),
                "merchant": p.get("merchant"),
                "category": p.get("merchant_category"),
                "country": p.get("country"),
            } for p in payments[:20]])
            mask = pd.Series([p.get("payment_id") in flagged for p in payments[:20]], index=pdf.index)
            st.dataframe(pdf.style.apply(red_rows(mask), axis=1).format({"amount": "{:,.2f}"}),
                         hide_index=True, width="stretch")
        else:
            st.info("Waiting for payments. Start the simulator: python simulator/simulator.py")
    with right:
        st.subheader("Most active cards")
        if top:
            cdf = pd.DataFrame([{
                "card": card,
                "payments": n,
                "total": total,
                "average": total / n,
                "alerts": card_alerts.get(card, 0),
            } for card, (n, total) in top])
            st.dataframe(cdf.style.apply(red_rows(cdf["alerts"] > 0), axis=1)
                         .format({"total": "{:,.2f}", "average": "{:,.2f}"}),
                         hide_index=True, width="stretch")


live_view()
