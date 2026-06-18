# Cache-Aware L7 Load Balancer Benchmark Guide for LLM Inference

## Purpose

This README defines the benchmark suite for a **cache-aware L7 load balancer / router** used for LLM inference.

A normal L7 load balancer asks:

```text
Which backend is least busy?
```

A cache-aware LLM router asks:

```text
Which backend can serve this request with the most useful KV-cache reuse, without creating a hotspot?
```

This matters because LLM inference backends are not stateless. They hold local KV cache, reuse prompt prefixes, maintain multi-turn conversation state, and may split work between prefill and decode workers.

---

## Core Benchmark Goals

A cache-aware L7 router should prove that it can:

1. Reduce **Time to First Token** by reusing KV cache.
2. Increase **prefix cache hit rate**.
3. Avoid recomputing repeated prompt tokens.
4. Avoid overloading warm backends.
5. Keep routing overhead low.
6. Handle stale cache state, pod churn, evictions, and autoscaling.
7. Preserve tenant, model, and version isolation.
8. Improve goodput under realistic SLOs.

---

These are the traffic patterns that should be tested.


| Benchmark Workload           | Use Case                                                         | What It Proves                                                                                |
| ---------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Cold traffic                 | Every prompt is unique                                           | Router should not perform worse than round-robin or least-load when cache reuse is impossible |
| Repeated prefix traffic      | Same system prompt, RAG template, long shared instruction prefix | Measures whether the router finds warm KV cache                                               |
| Multi-turn conversation      | Chat sessions, agents, copilots                                  | Measures conversation continuity and cache reuse across turns                                 |
| Long-context workload        | 32k, 64k, 128k+ token prompts                                    | Cache misses become very expensive; TTFT becomes highly sensitive                             |
| RAG workload                 | Many users share retrieved documents or prompt templates         | Measures partial prefix reuse, not only exact full-prompt reuse                               |
| Mixed short/long prompts     | Real production traffic                                          | Tests whether long prompts starve short prompts                                               |
| Burst traffic                | Sudden QPS spike                                                 | Tests whether cache-aware routing causes hotspots                                             |
| Tenant-isolated traffic      | Multiple customers, tenants, or organizations                    | Ensures cache reuse never crosses security boundaries                                         |
| Pod churn / rolling deploy   | Pods restart, autoscale, drain, or disappear                     | Tests stale cache state and dead-backend avoidance                                            |
| Cache eviction pressure      | KV cache close to capacity                                       | Tests whether routing still works when cache state changes quickly                            |
| Disaggregated prefill/decode | Separate prefill and decode pools                                | Tests multi-step routing and KV handoff                                                       |
| Event loss / stale state     | Delayed or missing KV-cache events                               | Tests robustness when the router cache index is imperfect                                     |

---

# 2. User-Facing Latency Benchmarks


| Benchmark                           | Use Case                   | Why It Matters                                               | Poor Result                              | Best Result                                  |
| ----------------------------------- | -------------------------- | ------------------------------------------------------------ | ---------------------------------------- | -------------------------------------------- |
| TTFT - Time to First Token          | Chat, agents, streaming UX | Main latency metric affected by prefix cache hits and misses | User waits too long before output starts | First token arrives quickly and consistently |
| E2E latency                         | Full response completion   | Measures total user wait time                                | Response starts fast but finishes slowly | Full response completes quickly              |
| TPOT - Time Per Output Token        | Decode performance         | Measures decode speed after first token                      | Slow token generation                    | Stable low ms/token                          |
| ITL - Inter Token Latency           | Streaming smoothness       | Measures spacing between streamed tokens                     | Choppy output stream                     | Smooth token stream                          |
| TBT - Time Between Tokens at client | Client-observed streaming  | Detects proxy buffering and network jitter                   | Tokens arrive in bursts                  | Tokens arrive steadily                       |
| Queue wait time                     | Saturated system           | Shows backend contention                                     | Requests wait in backend queue           | Low queue delay even under load              |
| Tail latency p95/p99/p99.9          | Production SLO             | Catches rare bad routing decisions                           | Some users get very bad latency          | Tail latency close to median                 |

---

# 3. Cache-Effectiveness Benchmarks


| Benchmark                   | Use Case                     | Why It Matters                                            | Poor Result                              | Best Result                                   |
| --------------------------- | ---------------------------- | --------------------------------------------------------- | ---------------------------------------- | --------------------------------------------- |
| Prefix cache hit rate       | Shared prompts, RAG, chat    | Core metric for cache-aware routing                       | Router sends requests to cold pods       | Most reusable tokens are served from KV cache |
| Matched prefix depth        | Long-context prompts         | Longer cached prefix means less prefill work              | Only shallow prefix reused               | Deep prefix reused                            |
| Best-vs-chosen cache gap    | Router quality               | Measures whether router selected the best cache candidate | Router ignores a better warm pod         | Chosen pod is close to optimal                |
| Avoided prefill tokens      | GPU cost                     | Directly measures saved compute                           | GPUs recompute cached tokens             | Large fraction of input tokens skipped        |
| TTFT reduction from caching | User-visible cache benefit   | Converts cache hit into latency gain                      | High hit rate but no latency improvement | TTFT drops as cached tokens increase          |
| Cache eviction rate         | Cache pressure               | High eviction destroys reuse                              | Warm prefixes disappear before reuse     | Hot prefixes stay cached                      |
| Cache capacity utilization  | KV memory sizing             | Shows whether cache is too small or too large             | OOM, eviction storms, or wasted memory   | Healthy utilization with useful retention     |
| Cache-state staleness       | Event-driven router state    | Router depends on accurate KV events                      | Router thinks evicted cache still exists | Router state is close to backend state        |
| Misroute rate               | Correctness and performance  | Measures bad cache route decisions                        | Chosen pod does not have expected blocks | Chosen pod has expected cache blocks          |
| Cache hint success rate     | Prefill/decode and remote KV | Ensures cache hints actually resolve                      | Decode pod cannot use hinted KV          | Cache hints are successfully consumed         |

---

# 4. Load-Balancing and Capacity Benchmarks


| Benchmark               | Use Case              | Why It Matters                                       | Poor Result                                  | Best Result                                   |
| ----------------------- | --------------------- | ---------------------------------------------------- | -------------------------------------------- | --------------------------------------------- |
| Request throughput      | API capacity          | Completed requests per second                        | Low completions or high latency              | More completed requests per second            |
| Input token throughput  | Prefill-heavy traffic | Measures prefill capacity                            | Long prompts bottleneck system               | High input token throughput                   |
| Output token throughput | Decode-heavy traffic  | Measures generation capacity                         | Low tokens per second                        | High stable output tokens per second          |
| Goodput                 | Production SLO        | Counts only successful SLO-compliant work            | High throughput but many SLO misses          | High useful throughput                        |
| SLO attainment rate     | SLA validation        | Measures percent of requests meeting latency targets | Many requests violate TTFT, TPOT, or E2E SLO | Most requests satisfy SLO                     |
| Per-pod load balance    | Hotspot prevention    | Cache-aware routing can overload warm pods           | One warm pod gets hammered                   | Load spread according to capacity             |
| Jain fairness index     | Multi-pod fairness    | Quantifies load distribution                         | Index near`1/N` means unfair                 | Index near`1.0` means balanced                |
| Hotspot rate            | Burst protection      | Detects overloaded backends                          | Cache affinity creates overload              | No pod exceeds load threshold                 |
| GPU utilization         | Cost efficiency       | Measures useful GPU work                             | Idle GPUs or overloaded GPUs                 | High useful utilization without SLO loss      |
| KV memory utilization   | Cache sizing          | KV cache consumes GPU memory                         | OOM, eviction storms, or low batch capacity  | Enough cache without starving active requests |

---

# 5. Router Hot-Path Benchmarks


| Benchmark                           | Use Case                  | Why It Matters                                                | Poor Result                         | Best Result                                    |
| ----------------------------------- | ------------------------- | ------------------------------------------------------------- | ----------------------------------- | ---------------------------------------------- |
| Router decision latency             | Every request             | Router is on the critical path                                | Router adds milliseconds            | Microsecond or low-ms overhead                 |
| Cache index query latency           | Prefix-aware routing      | Finding cached blocks must be fast                            | Cache lookup consumes TTFT budget   | Lookup overhead is negligible                  |
| Router CPU utilization              | Control-plane sizing      | Tokenization, hashing, and scoring can be expensive           | Router becomes bottleneck           | Router scales with QPS                         |
| Router memory usage                 | Large clusters            | Cache index can grow large                                    | Router OOM or GC pressure           | Predictable bounded memory                     |
| State update ingestion rate         | KV event stream           | Router must process register/evict events                     | Cache index lags reality            | Updates processed in real time                 |
| Event lag                           | Staleness control         | Measures delay from backend cache change to router visibility | Router routes using old cache state | Low event lag                                  |
| Duplicate/idempotent event handling | Replay or pub-sub systems | Event streams may replay or duplicate messages                | Duplicate events corrupt index      | Duplicate events are harmless                  |
| Pod removal convergence             | Rolling deploys           | Router must stop routing to dead pods                         | Requests sent to dead pod           | Dead pod removed immediately from routing path |

---

# 6. Correctness and Safety Benchmarks


| Benchmark                           | Use Case                  | Why It Matters                                        | Poor Result                  | Best Result                         |
| ----------------------------------- | ------------------------- | ----------------------------------------------------- | ---------------------------- | ----------------------------------- |
| Wrong-prefix reuse rate             | KV correctness            | Reusing KV from a different prefix can corrupt output | Incorrect model output       | Zero wrong-prefix reuse             |
| Cross-tenant cache hit rate         | Multi-tenant security     | Tenant A must not reuse Tenant B's private context    | Data leakage risk            | Zero cross-tenant reuse             |
| Model/version mismatch rate         | Rolling model deploys     | KV from one model/version may be invalid for another  | Bad output or backend errors | Cache isolated by model and version |
| Hash collision / hash mismatch rate | Block hashing correctness | Router relies on block hashes                         | Wrong cache selected         | Collision practically zero          |
| Fallback correctness                | Cache miss or stale state | Router must safely fall back to cold prefill          | Request fails                | Request succeeds cold               |

---

# 7. Disaggregated Prefill/Decode Benchmarks


| Benchmark                                    | Use Case                         | Why It Matters                                        | Poor Result                     | Best Result                             |
| -------------------------------------------- | -------------------------------- | ----------------------------------------------------- | ------------------------------- | --------------------------------------- |
| Prefill pod selection quality                | Compute-heavy stage              | Prefill should go where uncached compute is efficient | Slow TTFT                       | Low prefill time                        |
| Decode pod selection quality                 | Memory-bandwidth-heavy stage     | Decode should go where generation load is healthy     | Bad TPOT                        | Stable decode speed                     |
| KV transfer latency                          | Prefill to decode handoff        | Remote KV movement can erase cache benefit            | Fast prefill but slow handoff   | Low handoff overhead                    |
| Workflow latency breakdown                   | Multi-step execution             | Need per-stage visibility                             | Hard to debug latency           | Clear prefill/decode/transfer breakdown |
| Second-stage decision dependency correctness | Decode depends on prefill choice | Decode routing may need cache hint from prefill       | Decode cannot use prefetched KV | Decode consumes correct KV hint         |

---

# 8. Routing Policies to Compare

Every benchmark should compare the cache-aware router against simpler baselines.


| Policy                                     | Why Test It                                     |
| ------------------------------------------ | ----------------------------------------------- |
| Round-robin                                | Baseline generic load balancing                 |
| Least-load / least-requests                | Baseline capacity-aware routing                 |
| Consistent hash by session                 | Baseline sticky routing                         |
| Cache-aware only                           | Shows raw cache benefit but may create hotspots |
| Cache-aware + least-load                   | Usually the real production target              |
| Cache-aware + session affinity             | Best for multi-turn chat                        |
| Cache-aware + tenant/model isolation       | Required for production safety                  |
| Cache-aware + disaggregated prefill/decode | Required for split prefill/decode systems       |

---

# 9. Minimum Pass/Fail Checklist


| Area                    | Minimum Expectation                                                |
| ----------------------- | ------------------------------------------------------------------ |
| TTFT                    | Improves significantly on repeated-prefix and multi-turn workloads |
| Prefix hit rate         | Much higher than round-robin or least-load                         |
| Cache gap               | Chosen pod is usually close to best available cached pod           |
| Misroute rate           | Very low                                                           |
| Goodput                 | Higher than baseline at the same SLO                               |
| Load balance            | No persistent hot pod                                              |
| Router p99 latency      | Small enough to not affect TTFT budget                             |
| Event lag               | Low enough that cache index remains useful                         |
| Pod churn               | No routing to dead pods after shutdown detection                   |
| Tenant isolation        | Zero cross-tenant cache reuse                                      |
| Model/version isolation | Zero invalid KV reuse                                              |
| Disaggregated routing   | Prefill/decode handoff does not erase cache benefit                |

Strongest single summary benchmark:

```text
Goodput under repeated-prefix + multi-turn + burst workload,
while maintaining low misroute rate and no hotspots.
```

---

# 10. Formula Summary


| Metric                          | Formula                                                              |
| ------------------------------- | -------------------------------------------------------------------- |
| TTFT                            | `first_token_timestamp - request_submit_timestamp`                   |
| E2E latency                     | `final_token_timestamp - request_submit_timestamp`                   |
| TPOT                            | `(E2E - TTFT) / (output_tokens - 1)`                                 |
| Prefix cache hit rate           | `cached_prompt_tokens / total_prompt_tokens`                         |
| Cache gap                       | `best_possible_cached_tokens - actual_cached_tokens_on_chosen_pod`   |
| Cache gap rate                  | `cache_gap_tokens / prompt_tokens`                                   |
| Misroute rate                   | `misroutes / total_requests`                                         |
| Cache prediction error rate     | `abs(expected_cached_tokens - actual_cached_tokens) / prompt_tokens` |
| Avoided prefill tokens          | `cached_prompt_tokens_actual`                                        |
| Uncached prompt tokens          | `prompt_tokens - cached_prompt_tokens_actual`                        |
| Prefill ms per uncached token   | `prefill_latency_ms / uncached_prompt_tokens`                        |
| Queue wait time                 | `backend_start_timestamp - backend_queue_enter_timestamp`            |
| Request throughput              | `successful_completed_requests / benchmark_duration_seconds`         |
| Input token throughput          | `total_prompt_tokens / duration_seconds`                             |
| Uncached input token throughput | `uncached_prompt_tokens / duration_seconds`                          |
| Output token throughput         | `total_output_tokens / duration_seconds`                             |
| SLO attainment rate             | `requests_meeting_SLO / total_requests`                              |
| Goodput                         | `SLO_compliant_successful_requests / duration_seconds`               |
| Router decision latency         | `route_end_timestamp - route_start_timestamp`                        |
| Jain fairness index             | `(sum(load_i)^2) / (N * sum(load_i^2))`                              |
| Hotspot rate                    | `pods_above_load_threshold / total_pods`                             |
| KV cache utilization            | `kv_cache_used_bytes / kv_cache_capacity_bytes`                      |
| Eviction rate                   | `eviction_events / duration_seconds`                                 |
| Evictions per request           | `eviction_events / completed_requests`                               |
| Pod churn recovery time         | `last_route_to_dead_pod_ts - pod_shutdown_detected_ts`               |
| Cross-tenant violation rate     | `cache_hits_from_wrong_tenant / total_cache_hits`                    |

---

# 11. Sample Request Telemetry Schema

Each request should emit telemetry similar to this:

```python
request = {
    "request_id": "r1",
    "tenant_id": "t1",
    "session_id": "s1",

    "submit_ts": 0.000,
    "route_start_ts": 0.001,
    "route_end_ts": 0.003,
    "first_token_ts": 0.250,
    "last_token_ts": 1.850,

    "prompt_tokens": 8000,
    "output_tokens": 200,

    "cached_prompt_tokens_expected": 6000,
    "cached_prompt_tokens_actual": 5800,
    "best_possible_cached_tokens": 7000,

    "chosen_pod": "pod-a",
    "previous_session_pod": "pod-a",

    "backend_queue_start_ts": 0.004,
    "backend_start_ts": 0.050,
    "backend_end_ts": 1.850,

    "prefill_start_ts": 0.050,
    "prefill_end_ts": 0.230,
    "decode_start_ts": 0.250,
    "decode_end_ts": 1.850,

    "kv_transfer_ms": 12.0,
    "status": 200,
    "error": False,

    "slo_ttft_ms": 500,
    "slo_tpot_ms": 30,
    "slo_e2e_ms": 3000,
}
```

---

# 12. Python Code: Metric Calculator

```python
import numpy as np
import pandas as pd


def percentile(series, p):
    series = pd.Series(series).dropna()
    if len(series) == 0:
        return None
    return float(np.percentile(series, p))


def jain_fairness(values):
    """
    Jain fairness index:

        J = (sum(x_i)^2) / (n * sum(x_i^2))

    J = 1.0 means perfectly balanced.
    J approaches 1/n when one backend gets almost all load.
    """
    values = np.array(values, dtype=float)
    values = values[values >= 0]

    if len(values) == 0:
        return None

    denominator = len(values) * np.sum(values ** 2)
    if denominator == 0:
        return 1.0

    return float((np.sum(values) ** 2) / denominator)


def calculate_request_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # User-facing latency
    df["ttft_ms"] = (df["first_token_ts"] - df["submit_ts"]) * 1000
    df["e2e_ms"] = (df["last_token_ts"] - df["submit_ts"]) * 1000

    # TPOT is undefined for output_tokens <= 1
    df["tpot_ms"] = np.where(
        df["output_tokens"] > 1,
        (df["e2e_ms"] - df["ttft_ms"]) / (df["output_tokens"] - 1),
        np.nan,
    )

    # Router overhead
    df["router_decision_ms"] = (
        df["route_end_ts"] - df["route_start_ts"]
    ) * 1000

    # Backend queueing
    df["queue_wait_ms"] = (
        df["backend_start_ts"] - df["backend_queue_start_ts"]
    ) * 1000

    # Phase latency
    df["prefill_ms"] = (
        df["prefill_end_ts"] - df["prefill_start_ts"]
    ) * 1000

    df["decode_ms"] = (
        df["decode_end_ts"] - df["decode_start_ts"]
    ) * 1000

    # Cache metrics
    df["actual_prefix_hit_rate"] = (
        df["cached_prompt_tokens_actual"] / df["prompt_tokens"]
    )

    df["expected_prefix_hit_rate"] = (
        df["cached_prompt_tokens_expected"] / df["prompt_tokens"]
    )

    df["best_possible_hit_rate"] = (
        df["best_possible_cached_tokens"] / df["prompt_tokens"]
    )

    df["cache_gap_tokens"] = (
        df["best_possible_cached_tokens"] - df["cached_prompt_tokens_actual"]
    )

    df["cache_gap_rate"] = (
        df["cache_gap_tokens"] / df["prompt_tokens"]
    )

    df["uncached_prompt_tokens"] = (
        df["prompt_tokens"] - df["cached_prompt_tokens_actual"]
    )

    df["prefill_ms_per_uncached_token"] = np.where(
        df["uncached_prompt_tokens"] > 0,
        df["prefill_ms"] / df["uncached_prompt_tokens"],
        0,
    )

    # Cache prediction / staleness
    df["cache_prediction_error_tokens"] = (
        df["cached_prompt_tokens_expected"] - df["cached_prompt_tokens_actual"]
    )

    df["cache_prediction_error_rate"] = (
        df["cache_prediction_error_tokens"].abs() / df["prompt_tokens"]
    )

    df["misroute"] = (
        df["cached_prompt_tokens_expected"] > df["cached_prompt_tokens_actual"]
    )

    # Session continuity
    df["session_sticky_hit"] = (
        df["chosen_pod"] == df["previous_session_pod"]
    )

    # SLO attainment
    df["slo_met"] = (
        (df["status"] == 200) &
        (~df["error"]) &
        (df["ttft_ms"] <= df["slo_ttft_ms"]) &
        (df["tpot_ms"] <= df["slo_tpot_ms"]) &
        (df["e2e_ms"] <= df["slo_e2e_ms"])
    )

    return df


def calculate_summary(df: pd.DataFrame, duration_seconds: float) -> dict:
    df = calculate_request_metrics(df)

    successful = df[(df["status"] == 200) & (~df["error"])]

    total_prompt_tokens = df["prompt_tokens"].sum()
    total_cached_tokens = df["cached_prompt_tokens_actual"].sum()
    total_uncached_tokens = df["uncached_prompt_tokens"].sum()
    total_output_tokens = df["output_tokens"].sum()

    return {
        # Latency
        "ttft_p50_ms": percentile(df["ttft_ms"], 50),
        "ttft_p95_ms": percentile(df["ttft_ms"], 95),
        "ttft_p99_ms": percentile(df["ttft_ms"], 99),

        "e2e_p50_ms": percentile(df["e2e_ms"], 50),
        "e2e_p95_ms": percentile(df["e2e_ms"], 95),
        "e2e_p99_ms": percentile(df["e2e_ms"], 99),

        "tpot_p50_ms": percentile(df["tpot_ms"], 50),
        "tpot_p95_ms": percentile(df["tpot_ms"], 95),
        "tpot_p99_ms": percentile(df["tpot_ms"], 99),

        "queue_wait_p95_ms": percentile(df["queue_wait_ms"], 95),
        "router_decision_p99_ms": percentile(df["router_decision_ms"], 99),

        # Cache
        "token_weighted_prefix_hit_rate": (
            total_cached_tokens / total_prompt_tokens
            if total_prompt_tokens > 0 else None
        ),
        "request_avg_prefix_hit_rate": df["actual_prefix_hit_rate"].mean(),
        "best_possible_hit_rate": (
            df["best_possible_cached_tokens"].sum() / total_prompt_tokens
            if total_prompt_tokens > 0 else None
        ),
        "avg_cache_gap_tokens": df["cache_gap_tokens"].mean(),
        "avg_cache_gap_rate": df["cache_gap_rate"].mean(),
        "misroute_rate": df["misroute"].mean(),
        "avg_cache_prediction_error_rate": df["cache_prediction_error_rate"].mean(),

        # Throughput
        "request_throughput_rps": len(successful) / duration_seconds,
        "input_token_throughput_all_tps": total_prompt_tokens / duration_seconds,
        "input_token_throughput_uncached_tps": total_uncached_tokens / duration_seconds,
        "output_token_throughput_tps": total_output_tokens / duration_seconds,

        # SLO / goodput
        "slo_attainment_rate": df["slo_met"].mean(),
        "goodput_rps": df["slo_met"].sum() / duration_seconds,

        # Session
        "session_affinity_hit_rate": df["session_sticky_hit"].mean(),

        # Phase
        "prefill_p95_ms": percentile(df["prefill_ms"], 95),
        "decode_p95_ms": percentile(df["decode_ms"], 95),
        "kv_transfer_p95_ms": percentile(df["kv_transfer_ms"], 95),
    }
```

---

# 13. Python Code: Example Usage

```python
import pandas as pd

rows = [
    {
        "request_id": "r1",
        "tenant_id": "t1",
        "session_id": "s1",
        "submit_ts": 0.000,
        "route_start_ts": 0.001,
        "route_end_ts": 0.003,
        "first_token_ts": 0.250,
        "last_token_ts": 1.850,
        "prompt_tokens": 8000,
        "output_tokens": 200,
        "cached_prompt_tokens_expected": 6000,
        "cached_prompt_tokens_actual": 5800,
        "best_possible_cached_tokens": 7000,
        "chosen_pod": "pod-a",
        "previous_session_pod": "pod-a",
        "backend_queue_start_ts": 0.004,
        "backend_start_ts": 0.050,
        "backend_end_ts": 1.850,
        "prefill_start_ts": 0.050,
        "prefill_end_ts": 0.230,
        "decode_start_ts": 0.250,
        "decode_end_ts": 1.850,
        "kv_transfer_ms": 12.0,
        "status": 200,
        "error": False,
        "slo_ttft_ms": 500,
        "slo_tpot_ms": 30,
        "slo_e2e_ms": 3000,
    },
    {
        "request_id": "r2",
        "tenant_id": "t1",
        "session_id": "s1",
        "submit_ts": 2.000,
        "route_start_ts": 2.001,
        "route_end_ts": 2.006,
        "first_token_ts": 2.900,
        "last_token_ts": 4.400,
        "prompt_tokens": 9000,
        "output_tokens": 150,
        "cached_prompt_tokens_expected": 7000,
        "cached_prompt_tokens_actual": 1000,
        "best_possible_cached_tokens": 7500,
        "chosen_pod": "pod-b",
        "previous_session_pod": "pod-a",
        "backend_queue_start_ts": 2.007,
        "backend_start_ts": 2.400,
        "backend_end_ts": 4.400,
        "prefill_start_ts": 2.400,
        "prefill_end_ts": 2.880,
        "decode_start_ts": 2.900,
        "decode_end_ts": 4.400,
        "kv_transfer_ms": 40.0,
        "status": 200,
        "error": False,
        "slo_ttft_ms": 500,
        "slo_tpot_ms": 30,
        "slo_e2e_ms": 3000,
    },
]

df = pd.DataFrame(rows)

request_level = calculate_request_metrics(df)
summary = calculate_summary(df, duration_seconds=5.0)

print(request_level[[
    "request_id",
    "ttft_ms",
    "e2e_ms",
    "tpot_ms",
    "actual_prefix_hit_rate",
    "cache_gap_tokens",
    "misroute",
    "slo_met",
]])

print(summary)
```

---

# 14. Extra Code Snippets

## Load Fairness

```python
loads = [100, 110, 95, 105]
fairness = jain_fairness(loads)
print(fairness)
```

## Hotspot Rate

```python
import pandas as pd

pod_load = pd.Series({
    "pod-a": 1200,
    "pod-b": 300,
    "pod-c": 280,
    "pod-d": 260,
})

threshold = 1000
hotspot_rate = (pod_load > threshold).mean()
print(hotspot_rate)
```

## KV Cache Utilization

```python
kv_cache_used_bytes = 70 * 1024**3
kv_cache_capacity_bytes = 80 * 1024**3

kv_utilization = kv_cache_used_bytes / kv_cache_capacity_bytes
print(kv_utilization)
```

## Cache Eviction Rate

```python
eviction_events = 12_000
completed_requests = 50_000
duration_seconds = 600

evictions_per_second = eviction_events / duration_seconds
evictions_per_request = eviction_events / completed_requests

print(evictions_per_second)
print(evictions_per_request)
```

## Pod Churn Recovery Time

```python
pod_shutdown_detected_ts = 100.0
last_route_to_dead_pod_ts = 100.250

recovery_ms = (
    last_route_to_dead_pod_ts - pod_shutdown_detected_ts
) * 1000

print(recovery_ms)
```

## Cross-Tenant Cache Violation Rate

```python
import pandas as pd

cache_hits = pd.DataFrame([
    {"request_tenant": "t1", "cache_tenant": "t1"},
    {"request_tenant": "t1", "cache_tenant": "t1"},
    {"request_tenant": "t2", "cache_tenant": "t1"},
])

violations = cache_hits["request_tenant"] != cache_hits["cache_tenant"]
violation_rate = violations.mean()

print(violation_rate)
```

---

# 15. Recommended Final Evaluation

A cache-aware L7 load balancer is production-ready only if it improves cache reuse without damaging fairness, correctness, and reliability.

The final benchmark should run this combined scenario:

```text
Repeated-prefix traffic
+ multi-turn conversations
+ long-context prompts
+ burst load
+ pod churn
+ cache eviction pressure
+ tenant isolation
```

Then compare:

```text
round-robin
vs least-load
vs session-sticky
vs cache-aware only
vs cache-aware + least-load
vs cache-aware + least-load + safety isolation
```

The best router is not the one with only the highest cache hit rate.

The best router is the one with:

```text
highest goodput,
lowest TTFT,
low p95/p99 latency,
high prefix cache hit rate,
low cache gap,
low misroute rate,
no hotspots,
zero tenant/model/version violations,
and low router overhead.
```
