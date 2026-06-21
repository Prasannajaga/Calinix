#!/usr/bin/env python3
"""
Calinix policy benchmark plotter.

Run:
  python3 benchmark/plot_policy_bench.py \
    --input benchmark/results/policy-bench/policy_bench.csv

Outputs:
  policy_dashboard.png
  policy_router_latency.png
  policy_index_contention.png
  policy_fairness.png
  policy_staleness.png
"""
import argparse
import csv
from pathlib import Path

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError:
    print("pip install matplotlib")
    raise SystemExit(1)

DEFAULT_INPUT = Path("benchmark/results/policy-bench/policy_bench.csv")

BLUE = "#2563eb"
GREEN = "#16a34a"
AMBER = "#d97706"
RED = "#dc2626"
PURPLE = "#7c3aed"
GRAY = "#9ca3af"
DARK = "#374151"


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def read_rows(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def rows_for(rows, scenario):
    return [r for r in rows if r.get("scenario") == scenario]


def ival(row, key):
    v = row.get(key, "").strip()
    try:
        return int(float(v)) if v else 0
    except ValueError:
        return 0


def fval(row, key):
    v = row.get(key, "").strip()
    try:
        return float(v) if v else 0.0
    except ValueError:
        return 0.0


def fmt_us(v):
    return f"{v / 1000:.1f}ms" if v >= 1000 else f"{v:.0f}µs"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def clean(ax, title, subtitle, xlabel, ylabel):
    """Minimal axis: no top/right spines, light y-grid, clear subtitle."""
    ax.set_title(title, fontsize=11, fontweight="bold", loc="left", pad=18)
    if subtitle:
        ax.text(0.0, 1.03, subtitle, transform=ax.transAxes, fontsize=7.5,
                color="#6b7280", style="italic", ha="left", va="bottom")
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8.5, color=DARK)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=8.5, color=DARK)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(labelsize=8)
    ax.grid(axis="y", lw=0.3, alpha=0.35)
    ax.margins(y=0.18)  # Leave room for bar labels at the top


def bar_val(ax, bars, fmt_fn, fontsize=7):
    """Label on top of each bar."""
    for bar in bars:
        h = bar.get_height()
        if h <= 0:
            continue
        ax.text(bar.get_x() + bar.get_width() / 2, h,
                fmt_fn(h), ha="center", va="bottom", fontsize=fontsize, color=DARK)


# ═══════════════════════════════════════════════════════════════════════════
# 1. ROUTER DECISION LATENCY
# ═══════════════════════════════════════════════════════════════════════════

def draw_router_latency(ax, rows):
    data = sorted(rows_for(rows, "decision_latency"),
                  key=lambda r: ival(r, "concurrency"))
    if not data:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return

    labels = [str(ival(r, "concurrency")) for r in data]
    avg = [fval(r, "avg_us") for r in data]
    p95 = [fval(r, "p95_us") for r in data]
    p99 = [fval(r, "p99_us") for r in data]

    x = range(len(labels))
    w = 0.22
    ax.bar([i - w for i in x], avg, w, color=GRAY, label="avg")
    ax.bar(list(x), p95, w, color=BLUE, label="p95")
    b3 = ax.bar([i + w for i in x], p99, w, color=RED, label="p99")
    bar_val(ax, b3, fmt_us)

    # 100µs target line
    ax.axhline(100, ls="--", color=GREEN, lw=0.8, alpha=0.6)
    ax.text(len(labels) - 0.6, 108, "100µs target", fontsize=6.5,
            color=GREEN, ha="right")

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_us(v)))
    ax.legend(fontsize=7.5, frameon=False, ncol=3, loc="upper left")
    clean(
        ax,
        "Router Decision Latency",
        "Cost to parse JSON, tokenize, hash, search registry, score, and select target pod",
        "Concurrency (Parallel Clients)",
        "Latency (Lower is Better)"
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2. INDEX CONTENTION
# ═══════════════════════════════════════════════════════════════════════════

def draw_index_contention(ax, rows):
    data = sorted(rows_for(rows, "index_contention"),
                  key=lambda r: ival(r, "concurrency"))
    if not data:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return

    labels = [str(ival(r, "concurrency")) for r in data]
    r_avg = [fval(r, "avg_us") for r in data]
    r_p99 = [fval(r, "p99_us") for r in data]
    w_avg = [fval(r, "write_avg_us") for r in data]
    w_p99 = [fval(r, "write_p99_us") for r in data]

    x = range(len(labels))
    w = 0.18
    ax.bar([i - 1.5 * w for i in x], r_avg, w, color=GRAY, label="read avg")
    b2 = ax.bar([i - 0.5 * w for i in x], r_p99, w, color=BLUE, label="read p99")
    ax.bar([i + 0.5 * w for i in x], w_avg, w, color=AMBER, label="write avg")
    b4 = ax.bar([i + 1.5 * w for i in x], w_p99, w, color=RED, label="write p99")
    bar_val(ax, b2, fmt_us)
    bar_val(ax, b4, fmt_us)

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_us(v)))
    ax.legend(fontsize=6.5, frameon=False, ncol=2, loc="upper left")
    clean(
        ax,
        "Index Lock Contention",
        "RwLock latency under concurrent registry reads (queries) and writes (updates)",
        "Concurrency (Threads)",
        "Latency (Lower is Better)"
    )


# ═══════════════════════════════════════════════════════════════════════════
# 3. FAIRNESS
# ═══════════════════════════════════════════════════════════════════════════

def draw_fairness(ax, rows):
    fair = sorted(rows_for(rows, "hotspot_fairness"),
                  key=lambda r: ival(r, "concurrency"))
    dec = sorted(rows_for(rows, "decision_latency"),
                 key=lambda r: ival(r, "concurrency"))
    if not fair and not dec:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return

    conc_set = sorted(set(
        [ival(r, "concurrency") for r in dec] +
        [ival(r, "concurrency") for r in fair]
    ))
    dec_map = {ival(r, "concurrency"): fval(r, "jain_fairness") for r in dec}
    fair_map = {ival(r, "concurrency"): fval(r, "jain_fairness") for r in fair}

    labels = [str(c) for c in conc_set]
    y = range(len(labels))
    h = 0.32

    if dec_map:
        vals = [dec_map.get(c, 0) for c in conc_set]
        bars1 = ax.barh([i + h / 2 for i in y], vals, h, color=GRAY,
                        label="cache-only (hotspots)")
        for bar, v in zip(bars1, vals):
            if v > 0:
                ax.text(v + 0.008, bar.get_y() + bar.get_height() / 2,
                        f"{v:.3f}", va="center", fontsize=6.5, color=DARK)

    if fair_map:
        vals = [fair_map.get(c, 0) for c in conc_set]
        bars2 = ax.barh([i - h / 2 for i in y], vals, h, color=GREEN,
                        label="cache + load-aware (balanced)")
        for bar, v in zip(bars2, vals):
            if v > 0:
                ax.text(v + 0.008, bar.get_y() + bar.get_height() / 2,
                        f"{v:.3f}", va="center", fontsize=6.5, color=GREEN)

    ax.axvline(1.0, ls="--", color=BLUE, lw=0.8, alpha=0.5)
    ax.text(1.003, len(labels) - 0.5, "Perfect (1.0)", fontsize=7, color=BLUE, va="center")

    ax.set_yticks(list(y))
    ax.set_yticklabels(labels)
    ax.set_xlim(0, max(1.15, ax.get_xlim()[1]))
    ax.legend(fontsize=7.5, frameon=False, loc="lower right")
    clean(
        ax,
        "Pod Load Fairness",
        "Jain Fairness Index: 1.0 means traffic is perfectly balanced across all backend pods",
        "Jain Fairness Index (Closer to 1.0 is Better)",
        "Concurrency (Parallel Clients)"
    )


# ═══════════════════════════════════════════════════════════════════════════
# 4. STALENESS
# ═══════════════════════════════════════════════════════════════════════════

def draw_staleness(axes, rows):
    """Draw staleness on a pair of axes: axes[0] = misroute, axes[1] = cache gap."""
    ax_m, ax_g = axes
    data = sorted(rows_for(rows, "staleness_sensitivity"),
                  key=lambda r: ival(r, "lag_ms"))
    if not data:
        for a in (ax_m, ax_g):
            a.text(0.5, 0.5, "no data", ha="center", transform=a.transAxes)
        return

    labels = [f"{ival(r, 'lag_ms')}ms" for r in data]
    misroute = [fval(r, "misroute_rate") * 100 for r in data]
    gap = [fval(r, "cache_gap_avg_blocks") for r in data]
    x = range(len(labels))

    # left: misroute rate
    bars_m = ax_m.bar(x, misroute, color=RED, width=0.5)
    bar_val(ax_m, bars_m, lambda v: f"{v:.1f}%")
    ax_m.set_xticks(list(x))
    ax_m.set_xticklabels(labels)
    ax_m.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    clean(
        ax_m,
        "Stale Cache Misroutes",
        "Percentage of requests routed based on out-of-date cache metadata",
        "Event Propagation Lag",
        "Misrouted Requests (Lower is Better)"
    )

    # right: cache gap
    bars_g = ax_g.bar(x, gap, color=PURPLE, width=0.5)
    bar_val(ax_g, bars_g, lambda v: f"{v:.1f}")
    ax_g.set_xticks(list(x))
    ax_g.set_xticklabels(labels)
    clean(
        ax_g,
        "Stale Cache Prefix Gap",
        "Average number of prefix blocks missing on the chosen pod vs ground truth",
        "Event Propagation Lag",
        "Missed Prefix Blocks (Lower is Better)"
    )


# ═══════════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════

def plot_dashboard(rows, out):
    fig = plt.figure(figsize=(14, 13))

    # row 0: router latency | index contention
    # row 1: fairness (wide)
    # row 2: misroute rate  | cache gap
    gs = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.32,
                          height_ratios=[1, 1, 1])

    ax_lat = fig.add_subplot(gs[0, 0])
    ax_idx = fig.add_subplot(gs[0, 1])
    ax_fair = fig.add_subplot(gs[1, :])
    ax_stale_m = fig.add_subplot(gs[2, 0])
    ax_stale_g = fig.add_subplot(gs[2, 1])

    draw_router_latency(ax_lat, rows)
    draw_index_contention(ax_idx, rows)
    draw_fairness(ax_fair, rows)
    draw_staleness((ax_stale_m, ax_stale_g), rows)

    fig.suptitle("Calinix Cache-Aware Routing Policy Benchmark", fontsize=14, fontweight="bold",
                 y=0.985)
    fig.savefig(out / "policy_dashboard.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════════
# STANDALONE files
# ═══════════════════════════════════════════════════════════════════════════

def save_one(draw_fn, rows, out, filename, figsize=(8.5, 4.8)):
    fig, ax = plt.subplots(figsize=figsize)
    draw_fn(ax, rows)
    fig.savefig(out / filename, dpi=150, bbox_inches="tight")
    plt.close(fig)


def save_staleness(rows, out):
    data = rows_for(rows, "staleness_sensitivity")
    if not data:
        return
    fig, (ax_m, ax_g) = plt.subplots(1, 2, figsize=(13, 4.8))
    draw_staleness((ax_m, ax_g), rows)
    fig.savefig(out / "policy_staleness.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    rows = read_rows(args.input)
    if not rows:
        raise SystemExit(f"no rows in {args.input}")

    out = args.output_dir or args.input.parent
    out.mkdir(parents=True, exist_ok=True)

    plot_dashboard(rows, out)

    save_one(draw_router_latency, rows, out, "policy_router_latency.png")
    save_one(draw_index_contention, rows, out, "policy_index_contention.png")
    save_one(draw_fairness, rows, out, "policy_fairness.png")
    save_staleness(rows, out)

    print(f"plots → {out}")


if __name__ == "__main__":
    main()
