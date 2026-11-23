"""Payment simulator for the fraud-detection case study.

What it does
  1. Creates a fixed set of cards from a seed and publishes their profiles to the
     log-compacted `card-profiles` topic (key = card_id).
  2. Streams normal card payments to `payments` (key = card_id) at a steady rate.
     Normal traffic is shaped so it can never trigger a fraud rule by itself.
  3. Injects fraud on demand (keyboard commands or --inject), and logs every
     injection to simulator/injected.log so detection can be checked.

Run from the repo root:
  python simulator/simulator.py                         # 20 payments/sec, type commands
  python simulator/simulator.py --inject burst,travel   # scheduled injections
  python simulator/simulator.py --bench --rate 2000 --duration 60 --no-keyboard

Keyboard commands while running (type and press Enter):
  burst | travel | spike | limit   inject one fraud case (optionally followed by a card_id)
  status                           print counters
  help                             list commands
  quit                             stop cleanly (flushes the producer)

Message formats follow the shared contract in TEAM_TASKS.md section 4.
"""
from __future__ import annotations

import argparse
import heapq
import json
import math
import os
import queue
import random
import signal
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, Producer, TopicPartition

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
DEFAULT_CONFIG = HERE / "config.yaml"

INJECTION_TYPES = {
    "burst": "VELOCITY",
    "travel": "GEO_MISMATCH",
    "spike": "AMOUNT_SPIKE",
    "limit": "OVER_LIMIT",
}

# Merchant categories: (weight, median amount, spread, merchant name stems).
# Amounts are log-normal around the median, scaled by each card's spending habit.
CATEGORIES = {
    "food":          (0.30, 12.0, 0.50, ["Coffee Shop", "Bistro", "Pizza Place", "Sushi Bar"]),
    "grocery":       (0.20, 60.0, 0.50, ["Grocery Mart", "Corner Market"]),
    "gas":           (0.12, 50.0, 0.30, ["Gas Station"]),
    "retail":        (0.15, 45.0, 0.60, ["Clothing Store", "Bookstore", "Home Goods"]),
    "pharmacy":      (0.08, 25.0, 0.50, ["Pharmacy"]),
    "entertainment": (0.08, 30.0, 0.50, ["Cinema", "Concert Hall", "Arcade"]),
    "online":        (0.05, 40.0, 0.70, ["Online Store"]),
    "electronics":   (0.02, 150.0, 0.60, ["Electronics Store"]),
}
CATEGORY_NAMES = list(CATEGORIES)
CATEGORY_WEIGHTS = [CATEGORIES[c][0] for c in CATEGORY_NAMES]

HOME_COUNTRIES = [("CA", 0.85), ("US", 0.10), ("GB", 0.05)]
CURRENCY = {"CA": "CAD", "US": "USD", "GB": "GBP"}
TRIP_DESTINATIONS = ["US", "MX", "FR", "GB", "IT", "JP"]
SPENDING_LIMITS = [1000.0, 1500.0, 2000.0, 2500.0, 3000.0, 5000.0]


def now_ms() -> int:
    return int(time.time() * 1000)


def floor_cents(x: float) -> float:
    return math.floor(x * 100) / 100


# --------------------------------------------------------------------------- cards


@dataclass
class Card:
    card_id: str
    home_country: str
    currency: str
    spending_limit: float
    spend_factor: float
    reserved: bool
    # Running history. Mirrors what the detector has seen for this card.
    count: int = 0
    total: float = 0.0
    last_ts: int | None = None
    last_country: str | None = None
    recent: deque = field(default_factory=deque)  # timestamps (ms) of recent payments
    current_country: str = ""                     # where normal traffic currently happens
    busy: bool = False                            # an injection is in progress on this card
    last_burst_ts: int | None = None

    def __post_init__(self):
        if not self.current_country:
            self.current_country = self.home_country

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0

    def profile(self) -> dict:
        return {"card_id": self.card_id, "home_country": self.home_country,
                "spending_limit": self.spending_limit}

    def record(self, amount: float, country: str, ts: int) -> None:
        self.count += 1
        self.total += amount
        self.last_ts = ts if self.last_ts is None else max(self.last_ts, ts)
        self.last_country = country
        self.recent.append(ts)

    def recent_count(self, ts: int, window_ms: int) -> int:
        while self.recent and self.recent[0] < ts - window_ms:
            self.recent.popleft()
        return len(self.recent)


def make_cards(seed: int, num_cards: int, reserved: int) -> list[Card]:
    rng = random.Random(seed * 1_000_003 + 1)  # separate stream: cards don't depend on traffic
    countries, weights = zip(*HOME_COUNTRIES)
    cards = []
    for i in range(num_cards):
        home = rng.choices(countries, weights)[0]
        cards.append(Card(
            card_id=f"card_{i:04d}",
            home_country=home,
            currency=CURRENCY[home],
            spending_limit=rng.choice(SPENDING_LIMITS),
            spend_factor=min(2.0, max(0.5, rng.lognormvariate(0, 0.35))),
            reserved=i >= num_cards - reserved,
        ))
    return cards


# --------------------------------------------------------------------------- injections


@dataclass
class Injection:
    injection_id: str
    type: str
    card: Card
    steps: list            # list of (offset_ms, step_fn)
    payment_ids: list = field(default_factory=list)
    timestamps: list = field(default_factory=list)
    warmup_payment_ids: list = field(default_factory=list)
    trigger_payment_id: str | None = None
    details: dict = field(default_factory=dict)
    started_ms: int = 0
    done_steps: int = 0


class Simulator:
    def __init__(self, cfg: dict, args: argparse.Namespace):
        self.cfg = cfg
        k, s, g, inj = cfg["kafka"], cfg["simulation"], cfg["guards"], cfg["injection"]
        self.bench = args.bench
        self.seed = s["seed"]
        self.rate = float(s["rate_per_sec"])
        self.duration_ms = int(float(s["duration_sec"]) * 1000)
        self.max_payments = args.count
        self.payments_topic = k["payments_topic"]
        self.profiles_topic = k["profiles_topic"]
        self.bootstrap = k["bootstrap_servers"]
        self.g = g
        self.inj_cfg = inj
        self.travel_cfg = cfg["travel"]
        self.velocity_window_ms = int(g["velocity_window_sec"] * 1000)
        self.geo_window_ms = int(g["geo_window_min"] * 60_000)
        self.geo_safe_gap_ms = self.geo_window_ms + int(g["geo_margin_sec"] * 1000)

        self.rng = random.Random(self.seed)
        self.cards = make_cards(self.seed, s["num_cards"], s["reserved_cards"])
        self.by_id = {c.card_id: c for c in self.cards}
        self.normal_cards = [c for c in self.cards if not c.reserved]
        self.reserved_cards = [c for c in self.cards if c.reserved]
        if not self.normal_cards or not self.reserved_cards:
            sys.exit("num_cards must be larger than reserved_cards, and reserved_cards >= 1")

        producer_conf = {
            "bootstrap.servers": self.bootstrap,
            "acks": str(k["acks"]),
            "enable.idempotence": bool(k["enable_idempotence"]) and str(k["acks"]) in ("all", "-1"),
            "linger.ms": int(k["linger_ms"]),
            "compression.type": k["compression_type"],
            "partitioner": k["partitioner"],
            "client.id": "payment-simulator",
        }
        self.producer = Producer(producer_conf)
        self.producer_conf = producer_conf

        self.run_id = f"run-{time.strftime('%Y%m%d-%H%M%S')}-seed{self.seed}"
        self.log_path = Path(inj["log_file"])
        if not self.log_path.is_absolute():
            self.log_path = REPO_ROOT / self.log_path
        self.events: list = []  # heap of (due_ms, seq, action); action(due_ms) runs at due time
        self.seq = 0
        self.commands: queue.Queue = queue.Queue()
        self.stop = False
        self.sent = self.delivered = self.failed = self.skipped = 0
        self.injections_done = 0
        self.inj_counter = 0
        self.pending: list[Injection] = []
        self.schedule = list(args.inject_list or inj.get("schedule") or [])
        self.schedule_start_ms = int(float(inj["schedule_start_sec"]) * 1000)
        self.schedule_every_ms = int(float(inj["schedule_every_sec"]) * 1000)

    # ------------------------------------------------------------------ kafka helpers

    def check_cluster(self) -> None:
        try:
            md = self.producer.list_topics(timeout=10)
        except KafkaException as e:
            sys.exit(f"Cannot reach Kafka at {self.bootstrap}: {e}\n"
                     "Is the cluster up? Run `docker compose up -d` and ./scripts/create-topics.sh")
        missing = [t for t in (self.payments_topic, self.profiles_topic) if t not in md.topics]
        if missing:
            sys.exit(f"Missing topics {missing}. Run ./scripts/create-topics.sh first.")
        parts = {t: len(md.topics[t].partitions) for t in (self.payments_topic, self.profiles_topic)}
        print(f"[sim] connected to {len(md.brokers)} brokers; partitions: {parts}")
        if len(set(parts.values())) != 1:
            print("[sim] WARNING: payments and card-profiles have different partition counts; "
                  "the detector's stream-table join needs them co-partitioned.")

    def on_delivery(self, err, msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            if self.failed <= 5:
                print(f"[sim] delivery failed: {err}")

    def produce(self, topic: str, key: str, value: dict, ts: int) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self.producer.produce(topic, key=key.encode(), value=payload,
                                      timestamp=ts, on_delivery=self.on_delivery)
                break
            except BufferError:  # local queue full (very high rates or a stalled broker)
                self.producer.poll(0.05)

    def publish_profiles(self) -> None:
        ts = now_ms()
        for c in self.cards:
            self.produce(self.profiles_topic, c.card_id, c.profile(), ts)
        remaining = self.producer.flush(30)
        if remaining:
            sys.exit(f"Could not publish {remaining} card profiles. Check the cluster.")
        self.delivered = 0  # count payments only from here on
        print(f"[sim] published {len(self.cards)} card profiles to {self.profiles_topic} "
              f"({len(self.reserved_cards)} reserved for `limit` injections)")

    def rebuild_state(self) -> None:
        """Replay the payments log so per-card history matches what the detector has seen."""
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": f"simulator-rebuild-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        md = consumer.list_topics(self.payments_topic, timeout=10)
        parts = list(md.topics[self.payments_topic].partitions)
        ends = {}
        for p in parts:
            low, high = consumer.get_watermark_offsets(TopicPartition(self.payments_topic, p), timeout=10)
            if high > low:
                ends[p] = high
        if not ends:
            consumer.close()
            print("[sim] payments topic is empty; starting with fresh card history")
            return
        consumer.assign([TopicPartition(self.payments_topic, p, OFFSET_BEGINNING) for p in ends])
        replayed, done = 0, set()
        deadline = time.time() + 120
        while len(done) < len(ends) and time.time() < deadline:
            for msg in consumer.consume(1000, timeout=1.0):
                if msg.error():
                    continue
                if msg.offset() + 1 >= ends.get(msg.partition(), 0):
                    done.add(msg.partition())
                try:
                    p = json.loads(msg.value())
                    card = self.by_id.get(p.get("card_id"))
                    if card is not None:
                        card.record(float(p["amount"]), p["country"], int(p["timestamp_ms"]))
                        replayed += 1
                except (ValueError, KeyError, TypeError):
                    continue
        consumer.close()
        for c in self.cards:
            if c.last_country:
                c.current_country = c.last_country
        print(f"[sim] rebuilt card history from {replayed} earlier payments in {self.payments_topic}")

    # ------------------------------------------------------------------ payments

    def new_payment_id(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def draw_amount(self, card: Card, category: str) -> float:
        _, median, sigma, _ = CATEGORIES[category]
        amount = median * card.spend_factor * self.rng.lognormvariate(0, sigma)
        amount = min(max(amount, 1.0), 600.0)
        if not self.bench:
            caps = [card.spending_limit * self.g["max_normal_vs_limit"]]
            if card.count:
                caps.append(card.average * self.g["max_normal_vs_average"])
            amount = min(amount, *caps)
        return max(floor_cents(amount), 0.01)

    def make_payment(self, card: Card, ts: int, country: str | None = None,
                     amount: float | None = None) -> dict:
        category = self.rng.choices(CATEGORY_NAMES, CATEGORY_WEIGHTS)[0]
        stem = self.rng.choice(CATEGORIES[category][3])
        merchant = f"{stem} {self.rng.randint(1, 50)}"
        if amount is None:
            amount = self.draw_amount(card, category)
        return {
            "payment_id": self.new_payment_id(),
            "card_id": card.card_id,
            "amount": round(amount, 2),
            "currency": card.currency,   # amounts are in the card's billing currency
            "merchant": merchant,
            "merchant_category": category,
            "country": country or card.last_country or card.home_country,
            "timestamp_ms": ts,
        }

    def send_payment(self, payment: dict) -> None:
        card = self.by_id[payment["card_id"]]
        self.produce(self.payments_topic, card.card_id, payment, payment["timestamp_ms"])
        card.record(payment["amount"], payment["country"], payment["timestamp_ms"])
        self.sent += 1
        if self.sent % 100 == 0:
            self.producer.poll(0)

    # Rule checks that mirror the detector (used to keep normal traffic clean and to
    # make sure an injection triggers only the rule it is meant to trigger).
    def would_velocity(self, card: Card, ts: int, extra: int = 1) -> bool:
        return card.recent_count(ts, self.velocity_window_ms) + extra > 5

    def would_geo(self, card: Card, country: str, ts: int, window_ms: int | None = None) -> bool:
        window_ms = self.geo_window_ms if window_ms is None else window_ms
        return (card.last_country is not None and card.last_country != country
                and abs(ts - card.last_ts) <= window_ms)

    def would_spike(self, card: Card, amount: float) -> bool:
        return (card.count >= self.g["spike_min_payments"] and card.average > 0
                and amount > self.g["spike_multiplier"] * card.average)

    def would_limit(self, card: Card, amount: float) -> bool:
        return amount > card.spending_limit

    def normal_country(self, card: Card) -> str:
        """Where this card pays now. Occasionally a card starts or ends a trip.

        Changing country here just moves the card "in transit": the geo guard in
        normal_payment() then holds back its payments until more than the
        GEO_MISMATCH window has passed, like a flight, so trips never cause alerts.
        """
        if card.current_country == card.home_country:
            if self.rng.random() < self.travel_cfg["start_probability"]:
                card.current_country = self.rng.choice(
                    [c for c in TRIP_DESTINATIONS if c != card.home_country])
        elif self.rng.random() < self.travel_cfg["return_probability"]:
            card.current_country = card.home_country
        return card.current_country

    def normal_payment(self, ts: int) -> None:
        if self.bench:
            self.send_payment(self.make_payment(self.rng.choice(self.normal_cards), ts))
            return
        cap = self.g["max_payments_per_minute"]
        for _ in range(30):
            card = self.rng.choice(self.normal_cards)
            if card.busy or card.recent_count(ts, self.velocity_window_ms + 5000) >= cap:
                continue
            country = self.normal_country(card)
            if self.would_geo(card, country, ts, self.geo_safe_gap_ms):
                continue
            payment = self.make_payment(card, ts, country=country)
            if self.would_spike(card, payment["amount"]) or self.would_limit(card, payment["amount"]):
                continue  # cannot happen with the amount caps, but never send a false positive
            self.send_payment(payment)
            return
        self.skipped += 1

    # ------------------------------------------------------------------ injections

    def pick(self, pool: list[Card], ok) -> Card | None:
        candidates = [c for c in pool if not c.busy and ok(c)]
        return self.rng.choice(candidates) if candidates else None

    def start_injection(self, kind: str, at_ms: int, card_id: str | None = None) -> None:
        if kind not in INJECTION_TYPES:
            print(f"[sim] unknown injection '{kind}'. Use one of: {', '.join(INJECTION_TYPES)}")
            return
        ts = self.event_ts(at_ms)
        forced = self.by_id.get(card_id) if card_id else None
        if card_id and forced is None:
            print(f"[sim] unknown card {card_id}")
            return
        if forced is not None and forced.busy:
            print(f"[sim] {card_id} already has an injection in progress")
            return
        builder = getattr(self, f"build_{kind}")
        inj = builder(ts, forced)
        if inj is None:
            return
        inj.card.busy = True
        inj.started_ms = ts
        self.pending.append(inj)
        for i, (offset, _) in enumerate(inj.steps):
            self.push(at_ms + offset, lambda due, inj=inj, i=i: self.run_step(inj, i, due))
        warm = f" after {len(inj.steps) - inj.details.get('fraud_steps', len(inj.steps))} warm-up payments" \
            if inj.details.get("fraud_steps") else ""
        print(f"[sim] injecting {kind} on {inj.card.card_id} -> expect {INJECTION_TYPES[kind]}{warm}")

    def new_injection(self, kind: str, card: Card) -> Injection:
        self.inj_counter += 1
        return Injection(injection_id=f"{self.run_id}-{self.inj_counter:04d}", type=kind,
                         card=card, steps=[])

    def build_burst(self, ts: int, forced: Card | None) -> Injection | None:
        n = int(self.inj_cfg["burst_payments"])
        spacing = int(float(self.inj_cfg["burst_spacing_sec"]) * 1000)
        if (n - 1) * spacing >= self.velocity_window_ms:
            print("[sim] burst_payments * burst_spacing_sec must fit inside the velocity window")
            return None
        # A card that had a burst in the last few minutes would be deduplicated by the detector.
        quiet_ms = 2 * self.velocity_window_ms + 60_000
        def ok(c: Card) -> bool:
            return c.last_burst_ts is None or ts - c.last_burst_ts > quiet_ms
        # Prefer a card with no payments in the last window so the alert lists only burst payments.
        idle_ms = self.velocity_window_ms + 10_000
        card = forced or self.pick(self.normal_cards, lambda c: ok(c) and c.recent_count(ts, idle_ms) == 0) \
            or self.pick(self.normal_cards, ok)
        if card is None:
            print("[sim] no card available for burst")
            return None
        inj = self.new_injection("burst", card)
        card.last_burst_ts = ts
        # The alert fires on the payment that makes the count exceed 5 within the window.
        # Usually that is the 6th burst payment, earlier if the card paid recently.
        earlier = card.recent_count(ts, self.velocity_window_ms)
        trigger_index = max(0, 5 - earlier)

        def step(i):
            def run(step_ts):
                p = self.make_payment(card, step_ts)
                inj.payment_ids.append(p["payment_id"])
                inj.timestamps.append(step_ts)
                if i == trigger_index:
                    inj.trigger_payment_id = p["payment_id"]
                self.send_payment(p)
            return run
        inj.steps = [(i * spacing, step(i)) for i in range(n)]
        inj.details = {"payments": n, "spacing_ms": spacing, "earlier_payments_in_window": earlier}
        return inj

    def build_travel(self, ts: int, forced: Card | None) -> Injection | None:
        src, dst = self.inj_cfg["travel_from"], self.inj_cfg["travel_to"]
        gap = int(float(self.inj_cfg["travel_gap_sec"]) * 1000)
        if gap > self.geo_window_ms:
            print("[sim] travel_gap_sec must be inside the GEO_MISMATCH window")
            return None
        card = forced or self.pick(self.normal_cards, lambda c: (
            c.home_country == src and c.current_country == src and not self.would_geo(c, src, ts)))
        if card is None:
            print(f"[sim] no card with home country {src} available for travel")
            return None
        inj = self.new_injection("travel", card)

        def leg(country, is_trigger):
            def run(step_ts):
                p = self.make_payment(card, step_ts, country=country)
                inj.payment_ids.append(p["payment_id"])
                inj.timestamps.append(step_ts)
                if is_trigger:
                    inj.trigger_payment_id = p["payment_id"]
                self.send_payment(p)
            return run
        inj.steps = [(0, leg(src, False)), (gap, leg(dst, True))]
        inj.details = {"from": src, "to": dst, "gap_ms": gap}
        return inj

    def build_spike(self, ts: int, forced: Card | None) -> Injection | None:
        mult = float(self.inj_cfg["spike_multiplier"])
        need = int(self.g["spike_min_payments"])

        def fits(c: Card) -> bool:  # 10x average must stay under the limit (no OVER_LIMIT too)
            avg = c.average if c.count else c.spend_factor * 30
            return mult * avg < 0.9 * c.spending_limit
        quiet_ms = 2 * self.velocity_window_ms + 60_000

        def calm(c: Card) -> bool:  # no recent burst and room for one more payment
            return not self.would_velocity(c, ts) and (
                c.last_burst_ts is None or ts - c.last_burst_ts > quiet_ms)
        card = forced or self.pick(self.normal_cards, lambda c: c.count >= need and fits(c) and calm(c))
        warmups = 0
        if card is None:
            # Fresh cluster: no card has enough history yet. Give one card a few spaced-out
            # normal payments first (13 s apart so VELOCITY can't fire), then the spike.
            card = self.pick(self.normal_cards, lambda c: fits(c) and calm(c) and not self.would_geo(
                c, c.last_country or c.home_country, ts))
            if card is None:
                print("[sim] no card available for spike")
                return None
            warmups = need - card.count
        inj = self.new_injection("spike", card)

        def warm(step_ts):
            p = self.make_payment(card, step_ts)
            inj.warmup_payment_ids.append(p["payment_id"])
            self.send_payment(p)

        def spike(step_ts):
            avg = card.average
            amount = round(mult * avg, 2)
            if not self.would_spike(card, amount) or self.would_limit(card, amount):
                print(f"[sim] WARNING: spike on {card.card_id} may not behave as expected "
                      f"(count={card.count}, avg={avg:.2f}, amount={amount:.2f})")
            p = self.make_payment(card, step_ts, amount=amount)
            inj.payment_ids.append(p["payment_id"])
            inj.timestamps.append(step_ts)
            inj.trigger_payment_id = p["payment_id"]
            inj.details.update({"running_average": round(avg, 2), "amount": amount,
                                "prior_payments": card.count})
            self.send_payment(p)
        inj.steps = [(i * 13_000, warm) for i in range(warmups)] + [(warmups * 13_000, spike)]
        inj.details = {"multiplier": mult}
        if warmups:
            inj.details["fraud_steps"] = 1
        return inj

    def build_limit(self, ts: int, forced: Card | None) -> Injection | None:
        mult = float(self.inj_cfg["limit_multiplier"])

        def ok(c: Card) -> bool:
            amount = c.spending_limit * mult * 1.1
            return (not self.would_spike(c, amount) and not self.would_velocity(c, ts)
                    and not self.would_geo(c, c.last_country or c.home_country, ts))
        # Reserved cards only ever see limit payments, so a big amount is never also a spike.
        card = forced or self.pick(self.reserved_cards, ok)
        if card is None:
            print("[sim] no reserved card available for limit; increase reserved_cards")
            return None
        inj = self.new_injection("limit", card)

        def run(step_ts):
            amount = round(card.spending_limit * mult * self.rng.uniform(1.0, 1.1), 2)
            p = self.make_payment(card, step_ts, amount=amount)
            inj.payment_ids.append(p["payment_id"])
            inj.timestamps.append(step_ts)
            inj.trigger_payment_id = p["payment_id"]
            inj.details.update({"amount": amount, "spending_limit": card.spending_limit})
            self.send_payment(p)
        inj.steps = [(0, run)]
        return inj

    def finish_injection(self, inj: Injection) -> None:
        inj.card.busy = False
        self.pending.remove(inj)
        self.injections_done += 1
        record = {
            "run_id": self.run_id,
            "injection_id": inj.injection_id,
            "type": inj.type,
            "expected_rule": INJECTION_TYPES[inj.type],
            "card_id": inj.card.card_id,
            "payment_ids": inj.payment_ids,
            "trigger_payment_id": inj.trigger_payment_id,
            "payment_timestamps_ms": inj.timestamps,
            "warmup_payment_ids": inj.warmup_payment_ids,
            "started_at_ms": inj.started_ms,
            "completed_at_ms": inj.timestamps[-1] if inj.timestamps else inj.started_ms,
            "details": {k: v for k, v in inj.details.items() if k != "fraud_steps"},
        }
        with self.log_path.open("a") as f:
            f.write(json.dumps(record) + "\n")
        print(f"[sim] logged {inj.type} on {inj.card.card_id} ({len(inj.payment_ids)} payments) "
              f"-> {self.log_path.relative_to(REPO_ROOT) if self.log_path.is_relative_to(REPO_ROOT) else self.log_path}")

    # ------------------------------------------------------------------ main loop

    def push(self, due_ms: float, action) -> None:
        self.seq += 1
        heapq.heappush(self.events, (due_ms, self.seq, action))

    def run_step(self, inj: Injection, i: int, due_ms: float) -> None:
        inj.steps[i][1](self.event_ts(due_ms))
        inj.done_steps += 1
        if inj.done_steps == len(inj.steps):
            self.finish_injection(inj)

    def event_ts(self, due_ms: float) -> int:
        # Normal mode: event time follows the schedule, so the stream is reproducible.
        # Bench mode: wall-clock time, so pipeline latency includes any producer backlog.
        return now_ms() if self.bench else self.start_epoch_ms + int(due_ms)

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.start_mono) * 1000

    def keyboard(self) -> None:
        for line in sys.stdin:
            self.commands.put(line.strip())
        # stdin closed (e.g. piped input ended): keep running until duration/quit/Ctrl+C

    def handle_command(self, line: str, at_ms: float) -> None:
        if not line:
            return
        parts = line.split()
        cmd, arg = parts[0].lower(), (parts[1] if len(parts) > 1 else None)
        aliases = {"b": "burst", "t": "travel", "s": "spike", "l": "limit", "q": "quit"}
        cmd = aliases.get(cmd, cmd)
        if cmd in INJECTION_TYPES:
            self.start_injection(cmd, at_ms, arg)
        elif cmd == "status":
            self.print_stats(at_ms)
        elif cmd in ("quit", "exit"):
            self.stop = True
        elif cmd == "help":
            print("[sim] commands: burst | travel | spike | limit [card_id], status, quit")
        else:
            print(f"[sim] unknown command '{line}' (type help)")

    def print_stats(self, at_ms: float) -> None:
        secs = max(at_ms / 1000, 1e-9)
        lag = self.elapsed_ms() - at_ms
        msg = (f"[sim] t={secs:6.0f}s sent={self.sent} ({self.sent / secs:.1f}/s) "
               f"delivered={self.delivered} failed={self.failed} injections={self.injections_done}")
        if self.pending:
            msg += f" in-progress={len(self.pending)}"
        if self.skipped:
            msg += f" skipped={self.skipped}"
        if lag > 1000:
            msg += f" BEHIND by {lag / 1000:.1f}s"
        print(msg, flush=True)

    def run(self, keyboard: bool, rebuild: bool) -> None:
        self.check_cluster()
        if rebuild and not self.bench:
            self.rebuild_state()
        self.publish_profiles()

        capacity = len(self.normal_cards) * self.g["max_payments_per_minute"] / 60
        if not self.bench and self.rate > 0.8 * capacity:
            print(f"[sim] WARNING: {self.rate}/s is close to what {len(self.normal_cards)} cards can "
                  f"carry without VELOCITY false alerts (~{capacity:.0f}/s). Use more cards or --bench.")
        print(f"[sim] {'BENCH mode, ' if self.bench else ''}streaming {self.rate:g} payments/sec to "
              f"{self.payments_topic} | seed={self.seed} acks={self.producer_conf['acks']} "
              f"idempotence={self.producer_conf['enable.idempotence']} | run_id={self.run_id}")
        if keyboard:
            print("[sim] type burst | travel | spike | limit (+Enter) to inject fraud, quit to stop")
            threading.Thread(target=self.keyboard, daemon=True).start()

        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        interval = 1000.0 / self.rate if self.rate > 0 else float("inf")
        self.start_mono = time.monotonic()
        self.start_epoch_ms = now_ms()
        next_normal = 0.0
        next_stats = float(self.cfg["simulation"]["stats_every_sec"]) * 1000
        for i, kind in enumerate(self.schedule):
            due = self.schedule_start_ms + i * self.schedule_every_ms
            self.push(due, lambda d, kind=kind: self.start_injection(kind, d))

        while not self.stop:
            now = self.elapsed_ms()
            while not self.commands.empty():
                self.handle_command(self.commands.get_nowait(), min(now, next_normal))
            # Process everything that is due, in time order.
            progressed = False
            while True:
                next_event = self.events[0][0] if self.events else float("inf")
                due = min(next_normal, next_event)
                if due > now or self.stop:
                    break
                progressed = True
                if self.duration_ms and due >= self.duration_ms:
                    self.stop = True
                    break
                if next_event <= next_normal:
                    _, _, action = heapq.heappop(self.events)
                    action(due)
                else:
                    self.normal_payment(self.event_ts(due))
                    next_normal += interval
                    if self.max_payments and self.sent >= self.max_payments:
                        self.stop = True
                        break
                if due >= next_stats:
                    self.print_stats(due)
                    next_stats += float(self.cfg["simulation"]["stats_every_sec"]) * 1000
                    self.producer.poll(0)
            self.producer.poll(0)
            if not progressed:
                wait = min(next_normal, self.events[0][0] if self.events else float("inf")) - now
                time.sleep(max(0.0, min(wait / 1000, 0.05)))

        print("[sim] stopping; flushing producer...")
        left = self.producer.flush(30)
        if self.pending:
            print(f"[sim] {len(self.pending)} injection(s) were still in progress and were NOT logged: "
                  + ", ".join(f"{i.type} on {i.card.card_id}" for i in self.pending))
        self.print_stats(self.elapsed_ms())
        if left:
            print(f"[sim] WARNING: {left} messages were not delivered")



# --------------------------------------------------------------------------- CLI


def load_config(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Card-payment simulator with fraud injection.")
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="YAML config file")
    ap.add_argument("--rate", type=float, help="normal payments per second")
    ap.add_argument("--cards", type=int, help="number of cards")
    ap.add_argument("--seed", type=int, help="random seed")
    ap.add_argument("--bootstrap", help="Kafka bootstrap servers")
    ap.add_argument("--duration", type=float, help="stop after N seconds (0 = run until stopped)")
    ap.add_argument("--count", type=int, default=0, help="stop after N payments (normal + injected)")
    ap.add_argument("--inject", help="comma-separated injections to run on a schedule, "
                                     "e.g. burst,travel,spike,limit")
    ap.add_argument("--inject-start", type=float, help="seconds before the first scheduled injection")
    ap.add_argument("--inject-every", type=float, help="seconds between scheduled injections")
    ap.add_argument("--log-file", help="injection log path (default simulator/injected.log)")
    ap.add_argument("--acks", choices=["0", "1", "all"], help="producer acks (benchmarks only)")
    ap.add_argument("--payments-topic", help="topic for payments (benchmarks only)")
    ap.add_argument("--bench", action="store_true",
                    help="benchmark mode: fixed high rate, no false-alert guards, wall-clock timestamps")
    ap.add_argument("--no-keyboard", action="store_true", help="don't read commands from stdin")
    ap.add_argument("--no-rebuild", action="store_true", help="don't replay the payments log at startup")
    args = ap.parse_args(argv)
    args.inject_list = [x.strip().lower() for x in args.inject.split(",") if x.strip()] if args.inject else None
    return args


def apply_overrides(cfg: dict, args: argparse.Namespace) -> dict:
    sim, k, inj = cfg["simulation"], cfg["kafka"], cfg["injection"]
    for attr, section, key in [("rate", sim, "rate_per_sec"), ("cards", sim, "num_cards"),
                               ("seed", sim, "seed"), ("duration", sim, "duration_sec"),
                               ("bootstrap", k, "bootstrap_servers"), ("acks", k, "acks"),
                               ("payments_topic", k, "payments_topic"),
                               ("inject_start", inj, "schedule_start_sec"),
                               ("inject_every", inj, "schedule_every_sec"),
                               ("log_file", inj, "log_file")]:
        value = getattr(args, attr)
        if value is not None:
            section[key] = value
    if os.environ.get("BOOTSTRAP"):
        k["bootstrap_servers"] = os.environ["BOOTSTRAP"]
    return cfg


def main(argv=None) -> None:
    sys.stdout.reconfigure(line_buffering=True)  # show progress promptly even when piped
    args = parse_args(argv)
    cfg = apply_overrides(load_config(args.config), args)
    for kind in args.inject_list or cfg["injection"].get("schedule") or []:
        if kind not in INJECTION_TYPES:
            sys.exit(f"unknown injection '{kind}'; use {', '.join(INJECTION_TYPES)}")
    sim = Simulator(cfg, args)
    sim.run(keyboard=not args.no_keyboard,
            rebuild=cfg["simulation"]["rebuild_state_from_log"] and not args.no_rebuild)


if __name__ == "__main__":
    main()
