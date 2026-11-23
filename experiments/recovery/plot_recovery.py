#!/usr/bin/env python3
"""Plot a broker-failure run from measure_recovery.py.

  python experiments/recovery/plot_recovery.py              # latest run
  python experiments/recovery/plot_recovery.py 3fa2b1c0     # a specific run_id

Saves timeline_<run_id>.png next to the CSV: throughput (acked vs consumed,
1 s rolling) on top, partitions without a live leader and the minimum ISR size
below, with the failure and restart marked.
"""
import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = Path(__file__).resolve().parents[1] / "results" / "recovery"
BLUE, ORANGE = "#2a78d6", "#eb6834"       # series slots 1 and 2
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def load(path):
    rows = list(csv.DictReader(path.open()))
    t = [float(r["t_s"]) for r in rows]
    acked = [int(r["acked"]) for r in rows]
    consumed = [int(r["consumed"]) for r in rows]
    no_leader = [int(r["partitions_without_live_leader"]) if r["partitions_without_live_leader"] else None
                 for r in rows]
    min_isr = [int(r["min_isr"]) if r["min_isr"] else None for r in rows]
    events = [(float(r["t_s"]), r["event"]) for r in rows if r["event"]]
    return t, acked, consumed, no_leader, min_isr, events


def rolling_per_sec(counts):
    # 10 buckets of 100 ms = 1 s window -> messages per second
    out, window = [], 0
    for i, c in enumerate(counts):
        window += c - (counts[i - 10] if i >= 10 else 0)
        out.append(window)
    return out


def style(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def main():
    if len(sys.argv) > 1:
        path = RESULTS / f"timeline_{sys.argv[1]}.csv"
    else:
        files = sorted(RESULTS.glob("timeline_*.csv"), key=lambda p: p.stat().st_mtime)
        if not files:
            sys.exit(f"No timelines in {RESULTS}. Run measure_recovery.py first.")
        path = files[-1]

    t, acked, consumed, no_leader, min_isr, events = load(path)
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(10, 6), sharex=True,
                                      gridspec_kw={"height_ratios": [2, 1]})

    top.plot(t, rolling_per_sec(acked), color=BLUE, linewidth=2, label="Acknowledged by Kafka")
    top.plot(t, rolling_per_sec(consumed), color=ORANGE, linewidth=2, label="Received by consumer")
    top.set_ylabel("Messages / s", color=MUTED)
    top.set_title(f"Broker failure and recovery (run {path.stem.split('_', 1)[1]})",
                  loc="left", color=INK, fontsize=12)
    top.legend(frameon=False, loc="lower right", bbox_to_anchor=(1, 1), ncol=2, fontsize=9)

    pts = [(x, y) for x, y in zip(t, no_leader) if y is not None]
    bottom.step([x for x, _ in pts], [y for _, y in pts], where="post", color=BLUE,
                linewidth=2, label="Partitions without a live leader")
    pts = [(x, y) for x, y in zip(t, min_isr) if y is not None]
    bottom.step([x for x, _ in pts], [y for _, y in pts], where="post", color=ORANGE,
                linewidth=2, label="Smallest in-sync replica set")
    bottom.set_ylabel("Count", color=MUTED)
    bottom.set_xlabel("Seconds since start", color=MUTED)
    bottom.set_ylim(0, 4.2)  # headroom for the legend
    bottom.legend(frameon=False, loc="upper right", ncol=2, fontsize=9)

    for ax in (top, bottom):
        style(ax)
        for x, label in events:
            ax.axvline(x, color=MUTED, linewidth=1, linestyle="--")
    for x, label in events:
        top.annotate(label, (x, 1), xycoords=("data", "axes fraction"), xytext=(4, -12),
                     textcoords="offset points", color=MUTED, fontsize=9)

    fig.tight_layout()
    out = path.with_suffix(".png")
    fig.savefig(out, dpi=200)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
