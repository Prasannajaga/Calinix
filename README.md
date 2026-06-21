# Cache-Aware Routing Microbench

This is not a traditional load balancer like Nginx. It is a cache-aware load balancer that supports prefill and decode disaggregation using two modes:

1. **Single** (unified routing)
2. **Disaggregated dispatch** (separate prefill and decode routing)

Calinix determines which pods are best suited for each request, handling the routing complexity so you can manage your inference logic hassle-free.

## Configurations

The CALinix loads this YAML configuration at startup. By default, it expects a file named `./config.yaml` in the active working directory.

To use a custom configuration file, pass its path as the first command-line argument:

```bash
# Start Calinix with default ./config.yaml
cargo run --release

# Start Calinix with a specific configuration file
cargo run --release -- /path/to/my-config.yaml
```

### Example

```yaml
version: 1

gateway:
  port: 8080
  strategy: cacheAware

health:
  endpoint: /health
  intervalMs: 2000
  timeoutMs: 2000
  healthyThreshold: 2
  unhealthyThreshold: 3

cacheRegistry:
  enabled: true
  maxPods: 256
  shardsCount: 256
  staleTtlMs: 30000

upstreams:
  single:
    mode: single
    pods:
      - id: single-1
        url: http://single-1:8000
      - id: single-2
        url: http://single-2:8000

  dispatch:
    mode: dispatch
    prefill:
      pods:
        - id: prefill-1
          url: http://prefill-1:8000
        - id: prefill-2
          url: http://prefill-2:8000

    decode:
      pods:
        - id: decode-1
          url: http://decode-1:8000
        - id: decode-2
          url: http://decode-2:8000
```

## Core Workflow

![Routing Stages](./designs/stages.png)

Calinix processes each incoming request through a high-performance, 4-stage routing pipeline optimized for zero dynamic heap allocations on the critical path:

1. **Prepare Stage:** Deserializes the OpenAI JSON payload and computes a FNV-1a-based **Cumulative Hash Chain** of token blocks (e.g., block size of 4 or 32) to uniquely represent the prompt's prefix context.
2. **Filter Stage:** Isolates healthy candidate pods using $O(1)$ bitwise `AND` intersections on host bitmaps. Performs a parallel binary search on the 256-shard index to find which pods host matching prefix hashes.
3. **Score Stage:** Computes a multi-dimensional score for each candidate pod based on:
   * **Cache Score:** Percentage of matching prefix blocks cached in the pod.
   * **Load Score:** Available connection capacity based on current inflight load.
   * **Sticky Score:** Session affinity for multi-turn conversational chats.
   * **Locality Score:** Co-locates disaggregated decoders with the selected prefill node to bypass cross-network KV cache transfers.
4. **Pick Stage:** Selects the highest-scoring candidate (breaking ties deterministically by pod ID) and injects the routing headers (e.g., `x-calinix-target-pod-id`) to guide upstream gateway proxy forwarding.

![Core Workflow](./designs/coreworkflow.png)

## Why HostBitmap

`HostBitmap` uses `[u64; 4]` to represent 256 pods. This allows check and intersection operations to execute as 4 CPU bitwise operations, keeping performance constant ($O(1)$) rather than growing linearly with the pod count.

### Representation Comparison

| Representation | Type / Structure | Memory Size | p99 Latency | Relative Performance |
| :--- | :--- | :---: | :---: | :---: |
| **Bitmap (`HostBitmap`)** | `[u64; 4]` | **32 bytes** | **1.042 µs** | **Baseline (1.0x)** |
| **Array** | `[bool; 256]` | 256 bytes | 3.521 µs | ~3.37x slower |
| **List** | `Vec<usize>` | 2048 bytes | 6.060 µs | ~5.81x slower |

## Why Cumulative Hashes

The index stores cumulative prefix hashes, not random block hashes. Prefix `k` represents the full prompt prefix up to block `k`, so a match means the pod can reuse that whole prefix.

### Example of Cumulative Prefix Hashing

Consider a prompt: `"Explain cache aware routing in simple words"` with a **block size of 2 tokens**:

1. **Tokenization:** `["Explain", "cache", "aware", "routing", "in", "simple", "words"]`
2. **Block Construction & Cumulative Hash Chain:**
   * **Block 1:** `["Explain", "cache"]` $\rightarrow$ `hash_1 = hash("Explain" + "cache")`
   * **Block 2:** `["aware", "routing"]` $\rightarrow$ `hash_2 = hash(hash_1 + "aware" + "routing")`
   * **Block 3:** `["in", "simple"]` $\rightarrow$ `hash_3 = hash(hash_2 + "in" + "simple")`
   * **Block 4:** `["words"]` $\rightarrow$ `hash_4 = hash(hash_3 + "words")`

If a subsequent request arrives with a similar prompt prefix: `"Explain cache aware routing in deep details"`:

* **Block 1:** `["Explain", "cache"]` $\rightarrow$ Matches `hash_1` (Prefix match: 2 tokens)
* **Block 2:** `["aware", "routing"]` $\rightarrow$ Matches `hash_2` (Prefix match: 4 tokens)
* **Block 3:** `["in", "deep"]` $\rightarrow$ `hash(hash_2 + "in" + "deep")` $\neq$ `hash_3` (Mismatch!)

Since the hashes are cumulative, a match at `hash_2` guarantees that the *entire prefix* of 4 tokens matches exactly. The load balancer can confidently route the request to a pod caching up to `hash_2`, avoiding redundant prefill computation for the first 4 tokens.

## Performance & Benchmarks

I've run more experiments on the benchmarks and found this configuration (`new-policy-v3`) is the best. To see more detailed benchmark results and the step-by-step optimization journey, see [bench.md](./bench.md).

### 1. Routing Decision Latency

* **1 Concurrency:** ~273 µs

* **12 Concurrency:** ~534 µs
* **128 Concurrency:** ~2.45 ms (p50: 454 µs)

![Routing Latency](benchmark/results/new-policy-v3/policy_router_latency.png)

### 2. Lock Contention (256-shard index with 128 threads)

* **Read Queries:** < 2 µs

* **Write Updates:** < 70 µs

![Index Contention](benchmark/results/new-policy-v3/policy_index_contention.png)

### 3. Load Fairness & Cache Hits

* **Jain Fairness Index:** 0.751 (vs 0.076 for cache-only)

* **Cache Hit Rate:** ~100% (under hot prefix load)

![Cluster Fairness](benchmark/results/new-policy-v3/policy_fairness.png)

## P/D Disaggregation Design

<img src="designs/moderl-inference.svg" alt="Architecture diagram" width="1200">
