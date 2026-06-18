# Cache-Aware Routing Microbench

This is not a traditional load balancer like Nginx. It is a cache-aware load balancer that supports prefill and decode disaggregation using two modes:

1. **Single** (unified routing)
2. **Disaggregated dispatch** (separate prefill and decode routing)

Calinix determines which pods are best suited for each request, handling the routing complexity so you can manage your inference logic hassle-free.

## config

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

## workflow

This benchmark follows Modular exactly:

1. Storage: HostBitmap
   blockHash -> HostBitmap, where HostBitmap is a fixed 256-bit bitmap.
2. Concurrency: sharded index
   256 shards, each holding HashMap<BlockHash, HostBitmap> behind its own lock.
3. Fibonacci hashing
   shard = top bits of hash * 0x9E3779B97F4A7C15.
   Compared against low-bit sharding to show distribution quality.
4. Prefix query with binary search
   Given a cumulative hash chain, find each pod's longest cached prefix.
   Binary query is compared against naive N x P scanning.

## Why HostBitmap

`HostBitmap` is exactly `[u64; 4]`, covering 256 pods. Intersecting cache owners with alive pods or role candidates is just four `u64` operations.

### Benchmark Results

Here are the reproduced benchmark results:

| Representation | p99 Latency | Relative Performance |
| :--- | :---: | :---: |
| **Bitmap (`[u64; 4]`)** | **1.042 µs** | **Baseline (1.0x)** |
| **Array (`[bool; 256]`)** | 3.521 µs | ~3.37x slower |
| **List (`Vec<usize>`)** | 6.060 µs | ~5.81x slower |

### Memory Footprint

To represent pod cache ownership and health, each pod is mapped to a single bit. For 256 pods, here is how the memory footprints compare:

| Representation | Type / Structure | Calculation | Memory Size |
| :--- | :--- | :--- | :---: |
| **Bitmap** | `[u64; 4]` | 4 × 64 bits | **32 bytes** |
| **Array** | `[bool; 256]` | 256 × 1 byte | 256 bytes |
| **List** | `Vec<usize>` | 256 × 8 bytes | 2048 bytes |

With a bitmap, checking and intersecting pod sets becomes a few CPU bitwise operations. For 256 pods, it is just 4 × `u64` bitwise operations.

Each `u64` operation processes 64 pod states simultaneously. The work stays constant, whereas list and array operations grow linearly with the number of pods.

## Why Cumulative Hashes

The index stores cumulative prefix hashes, not random block hashes. Prefix `k` represents the full prompt prefix up to block `k`, so a match means the pod can reuse that whole prefix.

## P/D Disaggregation

<img src="moderl-inference.svg" alt="Architecture diagram" width="1200">
