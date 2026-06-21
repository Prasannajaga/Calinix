#!/usr/bin/env python3
"""
Calinix RouteGate — Production Benchmark Plotter

Generates cache-aware L7 load-balancer analytics from benchmark CSV output.
Run:  python3 benchmark/plot_url_bench.py --input benchmark/results/<run>/bench.csv

Outputs (single-run CSV with request_id):
  summary_banner.png             — main KPI strip for requests, latency, and cache reuse
  latency_vs_size.png            — detailed scatter: latency × prompt size, cache reuse, p50/p95
  cache_warmup.png               — rolling cache-hit rate over request sequence
  pod_affinity.png               — simple per-pod request distribution and hit rate
  cache_gap_prediction.png       — cache gap and misroute timeline
  cluster_fairness.png           — rolling pod load balance

Outputs (sweep CSV with concurrency):
  sweep_summary_banner.png       — scaling KPI strip with peak and stable capacity
  sweep_throughput.png           — concurrency vs RPS
  sweep_latency.png              — concurrency vs avg/p50/p95/p99 latency
  sweep_status_codes.png         — 2xx/4xx/5xx response counts by concurrency
"""
import argparse
import csv

from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker

except ImportError:
    print("pip install matplotlib")
    raise SystemExit(1)

DEFAULT_INPUT = Path("benchmark/results/url-bench/url_bench.csv")

# ---------------------------------------------------------------------------
# Color palette (Clean Light Theme with Colored Axis Accents)
# ---------------------------------------------------------------------------
HIT = "#059669"        # Emerald green
MISS = "#94a3b8"       # Slate gray for misses
LINE = "#0f172a"       # Slate 900 (deep slate for labels/lines)
REFERENCE = "#2563eb"  # Royal blue for reference lines
BG = "#ffffff"         # White background
PANEL = "#f8fafc"      # Light panel background
GRID = "#cbd5e1"       # Light gray-blue for grids
TEXT = "#334155"       # Slate 700 for text
MUTED = "#64748b"      # Slate 500 for secondary text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def read_rows(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def int_val(row: dict, key: str) -> int:
    v = row.get(key, "").strip()
    if not v:
        return 0
    try:
        return int(float(v))
    except ValueError:
        return 0


def float_val(row: dict, key: str) -> float:
    v = row.get(key, "").strip()
    if not v:
        return 0.0
    try:
        return float(v)
    except ValueError:
        return 0.0


def is_hit(row: dict) -> bool:
    return row.get("cache_hit", "").strip().lower() == "true"


def is_miss(row: dict) -> bool:
    return row.get("cache_hit", "").strip().lower() == "false"


def has_prompt_tokens(rows: list[dict]) -> bool:
    return any(int_val(row, "prompt_tokens") > 0 for row in rows)


def prompt_size(row: dict) -> int:
    tokens = int_val(row, "prompt_tokens")
    return tokens if tokens > 0 else int_val(row, "response_bytes")


def prompt_size_label(rows: list[dict]) -> str:
    return "Prompt Tokens" if has_prompt_tokens(rows) else "Response Bytes (legacy proxy)"


def matched_prefix_tokens(row: dict) -> int:
    value = int_val(row, "matched_prefix_tokens")
    if value > 0:
        return value
    depth = int_val(row, "cache_prefix_depth")
    block_size = int_val(row, "block_size") or 4
    prompt_tokens = int_val(row, "prompt_tokens")
    estimated = depth * block_size
    return min(estimated, prompt_tokens) if prompt_tokens > 0 else estimated


def pct(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * p)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def fmt_count(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def style_ax(ax, title="", xlabel="", ylabel=""):
    """Apply a styled axis on a clean white background with colored axes."""
    ax.set_facecolor(BG)
    ax.set_title(title, fontsize=12, fontweight="bold", color=LINE, pad=12)
    ax.set_xlabel(xlabel, fontsize=10, color=TEXT, labelpad=8)
    ax.set_ylabel(ylabel, fontsize=10, color=TEXT, labelpad=8)
    ax.tick_params(colors=TEXT, labelsize=9, which="both")
    # Color the x/y axis spines
    for spine in ax.spines.values():
        spine.set_color("#64748b")  # Slate gray for colored axis lines
        spine.set_linewidth(1.0)
    ax.grid(True, linestyle=":", alpha=0.6, color="#cbd5e1") # light gray/blue grids


def annotate_points(ax, x_vals: list[int], y_vals: list[float], suffix: str = "", precision: int = 0) -> None:
    if len(x_vals) > 8:
        return
    for x, y in zip(x_vals, y_vals):
        if precision == 0:
            label = f"{y:.0f}{suffix}"
        else:
            label = f"{y:.{precision}f}{suffix}"
        ax.annotate(
            label,
            (x, y),
            textcoords="offset points",
            xytext=(0, 7),
            ha="center",
            fontsize=8,
            color=TEXT,
        )


# ---------------------------------------------------------------------------
# Plot 1 — Latency vs Prompt Size scatter
# ---------------------------------------------------------------------------
def draw_latency_vs_size(ax, rows: list[dict]) -> None:
    success = [r for r in rows if r.get("success") == "true"]
    if not success:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return

    miss_x, miss_y = [], []
    hit_x, hit_y = [], []
    for r in success:
        size = prompt_size(r)
        lat = int_val(r, "latency_ms")
        if is_hit(r):
            hit_x.append(size)
            hit_y.append(lat)
        else:
            miss_x.append(size)
            miss_y.append(lat)

    # Clean translucent scatter with white edges for crisp overlaps
    if miss_x:
        ax.scatter(
            miss_x,
            miss_y,
            color=MISS,
            alpha=0.35,
            s=14,
            edgecolors="#ffffff",
            linewidths=0.2,
            label="Cache miss",
            zorder=3,
        )
    if hit_x:
        ax.scatter(
            hit_x,
            hit_y,
            color=HIT,
            alpha=0.55,
            s=14,
            edgecolors="#ffffff",
            linewidths=0.2,
            label="Cache hit",
            zorder=4,
        )

    # Dual-layered rolling median curves with soft IQR band (matching scatter colors)
    draw_binned_median(ax, miss_x, miss_y, MUTED, "Miss median trend")
    draw_binned_median(ax, hit_x, hit_y, HIT, "Hit median trend")

    all_lat = sorted(int_val(r, "latency_ms") for r in success)
    p50 = pct(all_lat, 0.50)
    p95 = pct(all_lat, 0.95)
    
    ax.axhline(p50, color=MUTED, linestyle=":", linewidth=0.8, alpha=0.5, zorder=2)
    ax.axhline(p95, color=LINE, linestyle="--", linewidth=0.8, alpha=0.4, zorder=2)
        
    ax.text(1.01, p50, f"p50: {p50:.0f} ms", transform=ax.get_yaxis_transform(),
            color=MUTED, ha="left", va="center", fontsize=8.5, fontweight="semibold")
    ax.text(1.01, p95, f"p95: {p95:.0f} ms", transform=ax.get_yaxis_transform(),
            color=LINE, ha="left", va="center", fontsize=8.5, fontweight="semibold")

    total_prompt_tokens = sum(int_val(r, "prompt_tokens") for r in success)
    reused_tokens = sum(matched_prefix_tokens(r) for r in success)
    token_reuse = reused_tokens / total_prompt_tokens * 100.0 if total_prompt_tokens else 0.0
    hit_count = sum(1 for r in success if is_hit(r))
    
    # Modern Title + Subtitle layout
    title_text = "Latency vs. Prompt Size"
    subtitle_text = f"Requests: {len(success)}  |  Hits: {hit_count} ({hit_count / len(success) * 100.0:.1f}%)  |  Token Reuse: {token_reuse:.1f}%"
    
    style_ax(ax, "", prompt_size_label(rows), "Latency (ms)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    
    ax.set_title(title_text, fontsize=13, fontweight="bold", color=LINE, pad=24, loc="left")
    ax.text(0.0, 1.03, subtitle_text, transform=ax.transAxes, fontsize=9.5, color=MUTED, ha="left", va="bottom")

    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: fmt_count(int(x))))
    ax.legend(fontsize=9, frameon=False, labelcolor=TEXT, loc="upper right")


def plot_latency_vs_size(rows: list[dict], out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 6.5))
    fig.patch.set_facecolor(BG)
    draw_latency_vs_size(ax, rows)
    plt.savefig(out_dir / "latency_vs_size.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def draw_binned_median(ax, x_vals: list[int], y_vals: list[int], color: str, label: str) -> None:
    if len(x_vals) < 10:
        return

    ordered = sorted(zip(x_vals, y_vals), key=lambda pair: pair[0])
    xs_sorted = [p[0] for p in ordered]
    ys_sorted = [p[1] for p in ordered]

    # Use a rolling window to compute smooth medians
    window_size = max(5, len(ordered) // 6)
    xs, ys = [], []
    y25s, y75s = [], []

    for i in range(len(ordered)):
        start = max(0, i - window_size // 2)
        end = min(len(ordered), i + window_size // 2 + 1)
        window_x = xs_sorted[start:end]
        window_y = ys_sorted[start:end]
        
        xs.append(pct(window_x, 0.50))
        ys.append(pct(window_y, 0.50))
        y25s.append(pct(window_y, 0.25))
        y75s.append(pct(window_y, 0.75))

    if len(xs) >= 2:
        # Draw soft IQR band
        ax.fill_between(xs, y25s, y75s, color=color, alpha=0.04, zorder=2)
        # Draw thick soft glow line
        ax.plot(xs, ys, color=color, linewidth=3.5, alpha=0.15, zorder=4)
        # Draw core solid line
        ax.plot(xs, ys, color=color, linewidth=1.8, label=label, zorder=5)


# ---------------------------------------------------------------------------
# Plot 2 — Cache warm-up timeline
# ---------------------------------------------------------------------------
def draw_cache_warmup(ax, rows: list[dict]) -> None:
    if not rows:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return

    window = min(50, len(rows))
    hits = [is_hit(r) for r in rows]
    rolling = []
    for i in range(len(hits)):
        start = max(0, i - window + 1)
        w = hits[start:i + 1]
        rolling.append(sum(w) / len(w) * 100.0)

    x = list(range(len(rows)))
    ax.plot(x, rolling, color=LINE, linewidth=2.2, label=f"Rolling hit rate ({window}-request window)")
    ax.fill_between(x, rolling, color=MISS, alpha=0.35)

    if rolling:
        ax.text(x[0], rolling[0], f"start {rolling[0]:.0f}%", color=TEXT, fontsize=8,
                ha="left", va="bottom")
        ax.text(x[-1], rolling[-1], f"end {rolling[-1]:.0f}%", color=TEXT, fontsize=8,
                ha="right", va="bottom")

    # Mark the point where it first reaches 50%.
    for i, v in enumerate(rolling):
        if v >= 50.0:
            ax.axvline(x=i, color=REFERENCE, linestyle="--", linewidth=1, alpha=0.9)
            ax.text(i + len(rows) * 0.01, 52, f"50% at request {i}", color=TEXT, fontsize=8)
            break

    style_ax(ax, "Cache Warmup (higher is better)", "Request Sequence", "Rolling Cache Hit Rate (%)")
    ax.set_ylim(-5, 105)
    ax.legend(fontsize=9, facecolor=BG, edgecolor=GRID, labelcolor=TEXT, loc="lower right")


def plot_cache_warmup(rows: list[dict], out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5))
    fig.patch.set_facecolor(BG)
    draw_cache_warmup(ax, rows)
    plt.tight_layout()
    plt.savefig(out_dir / "cache_warmup.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


# ---------------------------------------------------------------------------
# Plot 3 — Pod affinity (stacked bar: hits/misses per pod)
# ---------------------------------------------------------------------------
def draw_pod_affinity(ax, rows: list[dict]) -> None:
    if not rows:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return
    mode = next((r.get("mode") for r in rows if r.get("mode")), "single")

    pod_hits: dict[str, int] = defaultdict(int)
    pod_misses: dict[str, int] = defaultdict(int)

    if mode == "disaggregated":
        for r in rows:
            for key in ("prefill_pod_id", "decode_pod_id"):
                pod = r.get(key, "").strip()
                if not pod:
                    continue
                label = f"{key.split('_')[0]}-{pod}"
                if is_hit(r):
                    pod_hits[label] += 1
                else:
                    pod_misses[label] += 1
    else:
        for r in rows:
            pod = r.get("target_pod_id", "").strip()
            if not pod:
                continue
            if is_hit(r):
                pod_hits[pod] += 1
            else:
                pod_misses[pod] += 1

    all_pods = sorted(set(pod_hits) | set(pod_misses),
                      key=lambda x: int(x) if x.isdigit() else x)
    if not all_pods:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return

    all_pods = sorted(
        all_pods,
        key=lambda pod: (pod_hits[pod] + pod_misses[pod], pod),
        reverse=True,
    )
    h_vals = [pod_hits[p] for p in all_pods]
    m_vals = [pod_misses[p] for p in all_pods]
    totals = [h + m for h, m in zip(h_vals, m_vals)]
    hit_rates = [(h / total * 100.0) if total else 0.0 for h, total in zip(h_vals, totals)]

    y = list(range(len(all_pods)))
    ax.barh(y, h_vals, label="Cache hits", color=HIT, height=0.55)
    ax.barh(y, m_vals, left=h_vals, label="Cache misses", color=MISS, height=0.55)
    ax.set_yticks(y)
    ax.set_yticklabels([f"Pod {p}" for p in all_pods])
    ax.invert_yaxis()

    max_total = max(totals) if totals else 1
    ax.set_xlim(0, max_total * 1.25)
    for i, (total, hit_rate) in enumerate(zip(totals, hit_rates)):
        ax.text(total + max_total * 0.02, i, f"{total} req | {hit_rate:.0f}% hit",
                va="center", fontsize=9, color=TEXT)

    style_ax(ax, "Requests by Pod (balanced distribution is better)", "Request Count", "")
    ax.legend(fontsize=9, facecolor=BG, edgecolor=GRID, labelcolor=TEXT, loc="lower right")


def plot_pod_affinity(rows: list[dict], out_dir: Path) -> None:
    mode = next((r.get("mode") for r in rows if r.get("mode")), "single")
    all_pods = set()
    if mode == "disaggregated":
        for r in rows:
            for key in ("prefill_pod_id", "decode_pod_id"):
                pod = r.get(key, "").strip()
                if pod:
                    all_pods.add(f"{key.split('_')[0]}-{pod}")
    else:
        for r in rows:
            pod = r.get("target_pod_id", "").strip()
            if pod:
                all_pods.add(pod)
    height = max(4.5, len(all_pods) * 0.55 + 1.8)
    fig, ax = plt.subplots(figsize=(11, height))
    fig.patch.set_facecolor(BG)
    draw_pod_affinity(ax, rows)
    plt.tight_layout()
    plt.savefig(out_dir / "pod_affinity.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


# ---------------------------------------------------------------------------
# Plot 4 — Summary KPI banner
# ---------------------------------------------------------------------------
def plot_summary_banner(rows: list[dict], out_dir: Path) -> None:
    total = len(rows)
    success = sum(1 for r in rows if r.get("success") == "true")
    hits = sum(1 for r in rows if is_hit(r))
    misses = sum(1 for r in rows if is_miss(r))
    timeouts = sum(1 for r in rows if r.get("timeout") == "true")
    failures = sum(1 for r in rows if r.get("success") != "true" and r.get("timeout") != "true")

    success_lat = sorted(int_val(r, "latency_ms") for r in rows if r.get("success") == "true")
    avg_lat = sum(success_lat) / len(success_lat) if success_lat else 0.0
    p50 = pct(success_lat, 0.50)
    p95 = pct(success_lat, 0.95)
    p99 = pct(success_lat, 0.99)

    # Compute total prompt tokens skipped because the chosen pod had a matching prefix.
    tokens_saved = sum(matched_prefix_tokens(r) for r in rows)
    total_prompt_tokens = sum(int_val(r, "prompt_tokens") for r in rows)
    token_hit_rate = (tokens_saved / total_prompt_tokens * 100) if total_prompt_tokens else 0.0

    hit_rate = (hits / (hits + misses) * 100) if (hits + misses) > 0 else 0.0

    cards = [
        ("Total Requests", fmt_count(total), f"{success / total * 100:.1f}% success" if total else ""),
        ("Cache Hit Rate (▲)", f"{hit_rate:.1f}%", f"{fmt_count(hits)} hits / {fmt_count(misses)} misses"),
        ("Prefix Reuse (▲)", f"{token_hit_rate:.1f}%", f"{fmt_count(tokens_saved)} matched tokens"),
        ("Avg Latency (▼)", f"{avg_lat:.0f} ms", f"p50={p50:.0f}  p95={p95:.0f}  p99={p99:.0f}"),
        ("Failures (▼)", str(failures + timeouts), f"{failures} failed / {timeouts} timeout"),
    ]

    fig, axes = plt.subplots(1, len(cards), figsize=(len(cards) * 3.1, 2.3))
    fig.patch.set_facecolor(BG)

    for ax, (title, value, sub) in zip(axes, cards):
        ax.set_facecolor(PANEL)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")

        ax.text(0.5, 0.76, title, ha="center", va="center", fontsize=9, color=MUTED)
        ax.text(0.5, 0.48, value, ha="center", va="center", fontsize=19, color=LINE, fontweight="bold")
        ax.text(0.5, 0.20, sub, ha="center", va="center", fontsize=8, color=MUTED)

        for spine in ax.spines.values():
            spine.set_color(GRID)
            spine.set_visible(True)

    plt.subplots_adjust(wspace=0.15)
    plt.savefig(out_dir / "summary_banner.png", dpi=180, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close()

# # ---------------------------------------------------------------------------
# # Plot 5 — Cache Gap & Prediction Error Distribution
# # ---------------------------------------------------------------------------
# def plot_cache_gap_and_prediction(rows: list[dict], out_dir: Path) -> None:
#     if not rows:
#         return

#     fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
#     fig.patch.set_facecolor(BG)

#     # 1. Cache Gap Rate Distribution
#     gap_rates = []
#     for r in rows:
#         val = r.get("best_cache_gap_rate", "").strip()
#         if val:
#             try:
#                 gap_rates.append(float(val) * 100.0)  # as percentage of prompt tokens
#             except ValueError:
#                 pass

#     if not gap_rates:
#         ax1.text(0.5, 0.5, "No Cache Gap Data Available\n(best_cache_gap_rate column is empty)",
#                  ha="center", va="center", color=MUTED, fontsize=10)
#         style_ax(ax1, "Cache Gap Distribution (lower is better)", "Gap Rate (%)", "Frequency")
#     else:
#         ax1.hist(gap_rates, bins=15, color=HIT, edgecolor=GRID, alpha=0.85)
#         mean_gap = sum(gap_rates) / len(gap_rates)
#         ax1.axvline(mean_gap, color=LINE, linestyle="--", linewidth=1.5)
#         ax1.text(mean_gap + 0.5, ax1.get_ylim()[1] * 0.9, f"Mean: {mean_gap:.2f}%", color=LINE, fontsize=9)
#         style_ax(ax1, "Cache Gap Distribution (lower is better)", "Gap Rate (% of prompt tokens)", "Request Count")

#     # 2. Rolling Misroute Rate & Cache Gap Rate
#     window = min(50, len(rows))
#     rolling_misroute = []
#     rolling_gap = []
    
#     for i in range(len(rows)):
#         start = max(0, i - window + 1)
#         sub_rows = rows[start:i + 1]
        
#         # Misroute rate in window
#         m_count = sum(1 for r in sub_rows if r.get("misroute", "").strip().lower() == "true")
#         rolling_misroute.append(m_count / len(sub_rows) * 100.0)
        
#         # Avg gap rate in window
#         sub_gaps = []
#         for r in sub_rows:
#             v = r.get("best_cache_gap_rate", "").strip()
#             if v:
#                 try:
#                     sub_gaps.append(float(v) * 100.0)
#                 except ValueError:
#                     pass
#         if sub_gaps:
#             rolling_gap.append(sum(sub_gaps) / len(sub_gaps))
#         else:
#             rolling_gap.append(0.0)

#     x = list(range(len(rows)))
#     ax2.plot(x, rolling_misroute, color=LINE, linewidth=1.8, label="Rolling Misroute Rate")
#     ax2.fill_between(x, rolling_misroute, color=MISS, alpha=0.2)
    
#     if any(g > 0 for g in rolling_gap):
#         ax2.plot(x, rolling_gap, color=REFERENCE, linewidth=1.8, linestyle="--", label="Rolling Avg Cache Gap")
        
#     ax2.set_ylim(-5, 105)
#     style_ax(ax2, "Routing Quality & Prediction Error (lower is better)", "Request Sequence", "Rate (%)")
#     ax2.legend(fontsize=9, facecolor=BG, edgecolor=GRID, labelcolor=TEXT, loc="upper right")

#     plt.tight_layout()
#     plt.savefig(out_dir / "cache_gap_prediction.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
#     plt.close()


# ---------------------------------------------------------------------------
# Plot 6 — Cluster Load Fairness (Jain Fairness Index)
# ---------------------------------------------------------------------------
def draw_cluster_fairness(axes, rows: list[dict]) -> None:
    ax1, ax2 = axes
    if not rows:
        for ax in (ax1, ax2):
            ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return

    mode = next((r.get("mode") for r in rows if r.get("mode")), "single")
    
    # Extract pods associated with each request
    request_pods_list = []
    all_pods = set()
    for r in rows:
        pods = []
        if mode == "disaggregated":
            for key in ("prefill_pod_id", "decode_pod_id"):
                pod = r.get(key, "").strip()
                if pod:
                    pods.append(f"{key.split('_')[0]}-{pod}")
        else:
            pod = r.get("target_pod_id", "").strip()
            if pod:
                pods.append(pod)
        request_pods_list.append(pods)
        for p in pods:
            all_pods.add(p)
            
    unique_pods = sorted(list(all_pods), key=lambda x: int(x) if x.isdigit() else x)
    
    if not unique_pods:
        ax1.text(0.5, 0.5, "No Pod Data Available", ha="center", va="center", color=MUTED)
        style_ax(ax1, "Rolling Jain Fairness Index (higher is better)", "Request Sequence", "Fairness Index")
        ax2.text(0.5, 0.5, "No Pod Data Available", ha="center", va="center", color=MUTED)
        style_ax(ax2, "Rolling Pod Load (balanced is better)", "Request Sequence", "Load")
        return

    window = min(100, len(rows))
    
    jain_history = []
    pod_histories = {pod: [] for pod in unique_pods}
    current_counts = {pod: 0 for pod in unique_pods}
    
    for t in range(len(rows)):
        for pod in request_pods_list[t]:
            if pod in current_counts:
                current_counts[pod] += 1
        out_idx = t - window
        if out_idx >= 0:
            for pod in request_pods_list[out_idx]:
                if pod in current_counts:
                    current_counts[pod] = max(0, current_counts[pod] - 1)
                    
        counts = list(current_counts.values())
        sum_c = sum(counts)
        sum_sq_c = sum(c * c for c in counts)
        if sum_sq_c > 0:
            jain = (sum_c * sum_c) / (len(unique_pods) * sum_sq_c)
        else:
            jain = 1.0
        jain_history.append(jain)
        
        for pod in unique_pods:
            pod_histories[pod].append(current_counts[pod])

    x = list(range(len(rows)))
    
    # Left: Jain Fairness Index
    ax1.plot(x, jain_history, color=LINE, linewidth=2, label="Jain Fairness Index")
    worst_case = 1.0 / len(unique_pods)
    ax1.axhline(worst_case, color=REFERENCE, linestyle=":", linewidth=1.2, 
                label=f"Worst Case (1/{len(unique_pods)} = {worst_case:.2f})")
    ax1.set_ylim(-0.05, 1.05)
    style_ax(ax1, f"Rolling Cluster Load Fairness (W={window}) (higher is better)", "Request Sequence", "Fairness Index")
    ax1.legend(fontsize=9, facecolor=BG, edgecolor=GRID, labelcolor=TEXT, loc="lower right")

    # Right: Individual pod loads
    colors = ["#2563eb", "#059669", "#d97706", "#db2777", "#7c3aed", "#4b5563"]
    for idx, pod in enumerate(unique_pods):
        color = colors[idx % len(colors)]
        ax2.plot(x, pod_histories[pod], color=color, linewidth=1.5, label=f"Pod {pod}")
        
    style_ax(ax2, f"Rolling Pod Request Load (W={window}) (balanced is better)", "Request Sequence", "Active Requests in Window")
    ax2.legend(fontsize=8, facecolor=BG, edgecolor=GRID, labelcolor=TEXT, loc="upper right")


def plot_cluster_fairness(rows: list[dict], out_dir: Path) -> None:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.patch.set_facecolor(BG)
    draw_cluster_fairness((ax1, ax2), rows)
    plt.tight_layout()
    plt.savefig(out_dir / "cluster_fairness.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def plot_url_dashboard(rows: list[dict], out_dir: Path) -> None:
    # Request-level Dashboard
    fig = plt.figure(figsize=(15, 18))
    fig.patch.set_facecolor(BG)

    # row 0: latency_vs_size (wide)
    # row 1: cache_warmup | pod_affinity
    # row 2: jain fairness | individual pod loads
    gs = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.32, height_ratios=[1.2, 1, 1])

    ax_lat = fig.add_subplot(gs[0, :])
    ax_warm = fig.add_subplot(gs[1, 0])
    ax_pod = fig.add_subplot(gs[1, 1])
    ax_jain = fig.add_subplot(gs[2, 0])
    ax_load = fig.add_subplot(gs[2, 1])

    draw_latency_vs_size(ax_lat, rows)
    draw_cache_warmup(ax_warm, rows)
    draw_pod_affinity(ax_pod, rows)
    draw_cluster_fairness((ax_jain, ax_load), rows)

    fig.suptitle("Calinix RouteGate Request Routing Dashboard", fontsize=16, fontweight="bold", y=0.985, color=LINE)
    plt.savefig(out_dir / "url_dashboard.png", dpi=150, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point — single-run CSV
# ---------------------------------------------------------------------------
def plot_request_csv(rows: list[dict], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Generating plots for {len(rows)} requests → {out_dir}")

    plot_summary_banner(rows, out_dir)
    plot_latency_vs_size(rows, out_dir)
    plot_cache_warmup(rows, out_dir)
    plot_pod_affinity(rows, out_dir)
    # plot_cache_gap_and_prediction(rows, out_dir)
    plot_cluster_fairness(rows, out_dir)
    plot_url_dashboard(rows, out_dir)

    print(f"Done — {7} plots saved to {out_dir}/")


# ---------------------------------------------------------------------------
# Sweep plots — one row per concurrency level
# ---------------------------------------------------------------------------
def plot_sweep_csv(rows: list[dict], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = sorted(rows, key=lambda row: int_val(row, "concurrency"))
    print(f"Generating sweep plots for {len(rows)} concurrency levels → {out_dir}")

    plot_sweep_summary_banner(rows, out_dir)
    plot_sweep_throughput(rows, out_dir)
    plot_sweep_latency(rows, out_dir)
    plot_sweep_status_codes(rows, out_dir)
    plot_sweep_reliability(rows, out_dir)
    plot_sweep_cache_quality(rows, out_dir)
    plot_sweep_dashboard(rows, out_dir)

    print(f"Done — {7} sweep plots saved to {out_dir}/")


def plot_sweep_summary_banner(rows: list[dict], out_dir: Path) -> None:
    if not rows:
        return

    best_rps_row = max(rows, key=lambda row: float_val(row, "rps"))
    lowest_p95_row = min(rows, key=lambda row: float_val(row, "p95_latency_ms"))
    total_requests = sum(int_val(row, "total_requests") for row in rows)
    total_success = sum(int_val(row, "success_count") for row in rows)
    total_timeouts = sum(int_val(row, "timeout_count") for row in rows)
    total_errors = sum(int_val(row, "error_count") for row in rows)
    success_rate = total_success / total_requests * 100.0 if total_requests else 0.0
    timeout_rate = total_timeouts / total_requests * 100.0 if total_requests else 0.0
    stable_rows = [
        row
        for row in rows
        if int_val(row, "total_requests") > 0
        and int_val(row, "success_count") / int_val(row, "total_requests") >= 0.99
        and int_val(row, "timeout_count") == 0
    ]
    stable_row = max(stable_rows, key=lambda row: int_val(row, "concurrency")) if stable_rows else None
    peak_total = int_val(best_rps_row, "total_requests")
    peak_success = int_val(best_rps_row, "success_count")
    peak_success_rate = peak_success / peak_total * 100.0 if peak_total else 0.0

    cards = [
        (
            "Sweep Range",
            f"{int_val(rows[0], 'concurrency')} → {int_val(rows[-1], 'concurrency')}",
            f"{len(rows)} concurrency levels",
        ),
        (
            "Peak Throughput (▲)",
            f"{float_val(best_rps_row, 'rps'):.1f} RPS",
            f"at concurrency {int_val(best_rps_row, 'concurrency')}",
        ),
        (
            "Best p95 Latency (▼)",
            f"{float_val(lowest_p95_row, 'p95_latency_ms'):.0f} ms",
            f"at concurrency {int_val(lowest_p95_row, 'concurrency')}",
        ),
        (
            "Peak p95 Latency",
            f"{float_val(best_rps_row, 'p95_latency_ms'):.0f} ms",
            f"{peak_success_rate:.1f}% success at peak RPS",
        ),
        (
            "Stable Ceiling",
            f"{int_val(stable_row, 'concurrency')}" if stable_row else "n/a",
            ">=99% success and no timeouts" if stable_row else "no fully stable sweep point",
        ),
        (
            "Total Requests",
            fmt_count(total_requests),
            f"{success_rate:.1f}% success, {timeout_rate:.1f}% timeout, {total_errors} errors",
        ),
    ]

    fig, axes = plt.subplots(1, len(cards), figsize=(len(cards) * 3.0, 2.55))
    fig.patch.set_facecolor(BG)
    fig.suptitle("Concurrency Sweep Summary", fontsize=15, fontweight="bold", color=LINE, y=1.02)

    for ax, (title, value, sub) in zip(axes, cards):
        ax.set_facecolor(PANEL)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        ax.text(0.5, 0.76, title, ha="center", va="center", fontsize=9, color=MUTED)
        ax.text(0.5, 0.48, value, ha="center", va="center", fontsize=18, color=LINE, fontweight="bold")
        ax.text(0.5, 0.20, sub, ha="center", va="center", fontsize=8, color=MUTED)
        for spine in ax.spines.values():
            spine.set_color(GRID)
            spine.set_visible(True)

    plt.subplots_adjust(wspace=0.15)
    plt.savefig(out_dir / "sweep_summary_banner.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def draw_sweep_throughput(ax, rows: list[dict]) -> None:
    if not rows:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return
    x = [int_val(row, "concurrency") for row in rows]
    rps = [float_val(row, "rps") for row in rows]

    ax.plot(x, rps, color=LINE, linewidth=2.6, marker="o", markersize=5, label="RPS")
    ax.fill_between(x, rps, color=MISS, alpha=0.22)
    peak_idx = max(range(len(rps)), key=lambda idx: rps[idx]) if rps else 0
    if len(x) <= 8:
        for idx, (concurrency, value) in enumerate(zip(x, rps)):
            if idx == peak_idx:
                continue
            ax.annotate(
                f"{value:.1f}",
                (concurrency, value),
                textcoords="offset points",
                xytext=(0, 7),
                ha="center",
                fontsize=8,
                color=TEXT,
            )

    if rps:
        ax.scatter([x[peak_idx]], [rps[peak_idx]], color=HIT, s=70, zorder=5)
        ax.annotate(
            f"peak {rps[peak_idx]:.1f} RPS",
            (x[peak_idx], rps[peak_idx]),
            textcoords="offset points",
            xytext=(0, -28),
            ha="center",
            va="top",
            fontsize=9,
            color=LINE,
            fontweight="bold",
            arrowprops={"arrowstyle": "->", "color": HIT, "linewidth": 1.2},
        )

    style_ax(ax, "Throughput Scaling (higher is better)", "Concurrency", "Requests / second")
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f"{y:.0f} RPS"))
    ax.legend(fontsize=9, frameon=False, labelcolor=TEXT, loc="lower right")


def plot_sweep_throughput(rows: list[dict], out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    fig.patch.set_facecolor(BG)
    draw_sweep_throughput(ax, rows)
    plt.tight_layout()
    plt.savefig(out_dir / "sweep_throughput.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def draw_sweep_latency(ax, rows: list[dict]) -> None:
    if not rows:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return
    x = [int_val(row, "concurrency") for row in rows]
    avg = [float_val(row, "avg_latency_ms") for row in rows]
    p50 = [float_val(row, "p50_latency_ms") for row in rows]
    p95 = [float_val(row, "p95_latency_ms") for row in rows]
    p99 = [float_val(row, "p99_latency_ms") for row in rows]

    ax.plot(x, avg, color=MUTED, linewidth=1.8, marker="o", label="Average")
    ax.plot(x, p50, color=HIT, linewidth=1.8, marker="o", label="p50")
    ax.plot(x, p95, color=REFERENCE, linewidth=2.2, marker="o", label="p95")
    ax.plot(x, p99, color=LINE, linewidth=2.2, marker="o", label="p99")
    ax.fill_between(x, p95, p99, color=LINE, alpha=0.08, label="p95-p99 tail band")
    annotate_points(ax, x, p95, suffix=" ms")

    style_ax(ax, "Latency Under Load (lower is better)", "Concurrency", "Latency (ms)")
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f"{y:.0f} ms"))
    ax.legend(fontsize=9, frameon=False, labelcolor=TEXT, loc="upper left")


def plot_sweep_latency(rows: list[dict], out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.patch.set_facecolor(BG)
    draw_sweep_latency(ax, rows)
    plt.tight_layout()
    plt.savefig(out_dir / "sweep_latency.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def draw_sweep_status_codes(ax, rows: list[dict]) -> None:
    if not rows:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return
    concurrency_labels = [str(int_val(row, "concurrency")) for row in rows]
    x = list(range(len(rows)))
    status_2xx = [int_val(row, "status_2xx") for row in rows]
    status_4xx = [int_val(row, "status_4xx") for row in rows]
    status_5xx = [int_val(row, "status_5xx") for row in rows]
    totals = [ok + client_err + server_err for ok, client_err, server_err in zip(status_2xx, status_4xx, status_5xx)]

    width = 0.72
    ax.bar(x, status_2xx, width=width, color=HIT, label="2xx")
    ax.bar(x, status_4xx, bottom=status_2xx, width=width, color="#d97706", label="4xx")
    bottom_5xx = [ok + client_err for ok, client_err in zip(status_2xx, status_4xx)]
    ax.bar(x, status_5xx, bottom=bottom_5xx, width=width, color="#dc2626", label="5xx")

    max_total = max(totals) if totals else 1
    for idx, total, ok, client_err, server_err in zip(x, totals, status_2xx, status_4xx, status_5xx):
        ok_rate = ok / total * 100.0 if total else 0.0
        ax.text(
            idx,
            max(total - max_total * 0.04, total * 0.5),
            f"{ok_rate:.1f}% 2xx",
            ha="center",
            va="top",
            fontsize=8,
            color="#ffffff",
            fontweight="bold",
        )
        if client_err or server_err:
            ax.text(
                idx,
                total * 0.5,
                f"4xx {client_err}\n5xx {server_err}",
                ha="center",
                va="center",
                fontsize=8,
                color="#ffffff",
                fontweight="bold",
            )

    style_ax(ax, "API Status Codes by Concurrency (2xx higher is better)", "Concurrency", "Response Count")
    ax.set_xticks(x)
    ax.set_xticklabels(concurrency_labels)
    ax.set_ylim(0, max_total * 1.12)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: fmt_count(int(y))))
    ax.legend(
        fontsize=9,
        frameon=False,
        labelcolor=TEXT,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.14),
        ncol=3,
    )


def plot_sweep_status_codes(rows: list[dict], out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    fig.patch.set_facecolor(BG)
    draw_sweep_status_codes(ax, rows)
    plt.tight_layout()
    plt.savefig(out_dir / "sweep_status_codes.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def draw_sweep_reliability(ax, rows: list[dict]) -> None:
    if not rows:
        ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return
    x = [int_val(row, "concurrency") for row in rows]
    success = [int_val(row, "success_count") for row in rows]
    errors = [int_val(row, "error_count") for row in rows]
    timeouts = [int_val(row, "timeout_count") for row in rows]

    width = 0.72
    ax.bar(x, success, width=width, color=HIT, label="Success")
    ax.bar(x, errors, bottom=success, width=width, color="#d97706", label="Errors")
    err_plus_success = [s + e for s, e in zip(success, errors)]
    ax.bar(x, timeouts, bottom=err_plus_success, width=width, color="#dc2626", label="Timeouts")

    totals = [s + e + t for s, e, t in zip(success, errors, timeouts)]
    max_total = max(totals) if totals else 1
    for concurrency, total, ok in zip(x, totals, success):
        rate = ok / total * 100.0 if total else 0.0
        ax.text(concurrency, total + max_total * 0.02, f"{rate:.1f}%", ha="center", va="bottom", fontsize=8, color=TEXT)

    style_ax(ax, "Reliability by Concurrency (higher success is better)", "Concurrency", "Request Count")
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: fmt_count(int(y))))
    ax.legend(fontsize=9, frameon=False, labelcolor=TEXT, loc="upper left")


def plot_sweep_reliability(rows: list[dict], out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    fig.patch.set_facecolor(BG)
    draw_sweep_reliability(ax, rows)
    plt.tight_layout()
    plt.savefig(out_dir / "sweep_reliability.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def draw_sweep_cache_quality(axes, rows: list[dict]) -> None:
    ax1, ax2 = axes
    if not rows:
        for ax in (ax1, ax2):
            ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
        return
    x = [int_val(row, "concurrency") for row in rows]
    cache_hit_rate = []
    prefix_hit_rate = []
    gap_rate = []
    misroute_rate = []

    for row in rows:
        hits = int_val(row, "cache_hit_count")
        misses = int_val(row, "cache_miss_count")
        cache_hit_rate.append((hits / (hits + misses) * 100.0) if hits + misses else 0.0)
        prefix_hit_rate.append(float_val(row, "token_weighted_prefix_hit_rate") * 100.0)
        gap_rate.append(float_val(row, "avg_cache_gap_rate") * 100.0)
        misroute_rate.append(float_val(row, "misroute_rate") * 100.0)

    ax1.plot(x, cache_hit_rate, color=HIT, linewidth=2.2, marker="o", label="Cache hit rate")
    ax1.plot(x, prefix_hit_rate, color=REFERENCE, linewidth=2.2, marker="o", label="Token-weighted prefix reuse")
    annotate_points(ax1, x, prefix_hit_rate, suffix="%", precision=1)
    ax1.set_ylim(-5, 105)
    style_ax(ax1, "Cache Reuse by Load (higher is better)", "Concurrency", "Rate (%)")
    ax1.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax1.legend(fontsize=9, frameon=False, labelcolor=TEXT, loc="lower right")

    ax2.plot(x, gap_rate, color="#d97706", linewidth=2.2, marker="o", label="Average cache gap")
    ax2.plot(x, misroute_rate, color="#dc2626", linewidth=2.2, marker="o", label="Misroute rate")
    annotate_points(ax2, x, misroute_rate, suffix="%", precision=1)
    upper = max([5.0] + gap_rate + misroute_rate)
    ax2.set_ylim(-0.5, min(105.0, upper * 1.25))
    style_ax(ax2, "Routing Loss by Load (lower is better)", "Concurrency", "Rate (%)")
    ax2.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax2.legend(fontsize=9, frameon=False, labelcolor=TEXT, loc="upper left")


def plot_sweep_cache_quality(rows: list[dict], out_dir: Path) -> None:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.6))
    fig.patch.set_facecolor(BG)
    draw_sweep_cache_quality((ax1, ax2), rows)
    plt.tight_layout()
    plt.savefig(out_dir / "sweep_cache_quality.png", dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close()


def plot_sweep_dashboard(rows: list[dict], out_dir: Path) -> None:
    # Sweep-level Dashboard
    fig = plt.figure(figsize=(15, 18))
    fig.patch.set_facecolor(BG)

    # row 0: sweep_throughput | sweep_latency
    # row 1: sweep_status_codes | sweep_reliability
    # row 2: sweep_cache_quality (left: reuse, right: loss)
    gs = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.32)

    ax_tp = fig.add_subplot(gs[0, 0])
    ax_lat = fig.add_subplot(gs[0, 1])
    ax_sc = fig.add_subplot(gs[1, 0])
    ax_rel = fig.add_subplot(gs[1, 1])
    ax_cq1 = fig.add_subplot(gs[2, 0])
    ax_cq2 = fig.add_subplot(gs[2, 1])

    draw_sweep_throughput(ax_tp, rows)
    draw_sweep_latency(ax_lat, rows)
    draw_sweep_status_codes(ax_sc, rows)
    draw_sweep_reliability(ax_rel, rows)
    draw_sweep_cache_quality((ax_cq1, ax_cq2), rows)

    fig.suptitle("Calinix RouteGate Concurrency Sweep Dashboard", fontsize=16, fontweight="bold", y=0.985, color=LINE)
    plt.savefig(out_dir / "sweep_dashboard.png", dpi=150, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Calinix RouteGate benchmark plotter")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args()
    out_dir = args.out_dir or args.input.parent

    rows = read_rows(args.input)
    if not rows:
        print(f"no rows found in {args.input}")
        return

    if "request_id" in rows[0]:
        plot_request_csv(rows, out_dir)
    elif "concurrency" in rows[0]:
        plot_sweep_csv(rows, out_dir)
    else:
        print(f"unrecognized CSV shape: {args.input}")


if __name__ == "__main__":
    main()
