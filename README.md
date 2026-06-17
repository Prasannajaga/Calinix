# Cache-Aware Routing Microbench

this is not your traiditonal load balancer like nginx
this is cache aware load balancer which supports
prefill + decode disaggregated using two mods

1. single
2. disaggregated dispatch

calinix tells which pods is suited for each request
it owns the load balancer you own the your inference logic
without any hassel.

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

So I reproduced the benchmark

bitmap p99 = 1.042 µs
list   p99 = 6.060 µs
array  p99 = 3.521 µs

So:
list  is ~5.81x slower
array is ~3.37x slower

heres why the clear goal is to represent pod Bitmap:
[u64; 4] = 4 * 64 bits = 256 bits
4 * u64 = 4 * 8 bytes = 32 bytes in memory

List:
Vec<usize>
256 * 8 bytes = 2048 bytes in memory

Array:
[bool; 256]
256 * 1 byte = 256 bytes in memorycache ownership/ health.
each pod is a single bit so for 256 pods is 256 bits

Bitmap:
[u64; 4] = 4 * 64 bits = 256 bits
4 * u64 = 4 * 8 bytes = 32 bytes in memory

List:
Vec<usize>
256 * 8 bytes = 2048 bytes in memory

Array:
[bool; 256]
256 * 1 byte = 256 bytes in memory

with bitmap, checking/intersecting pod sets becomes a few CPU bitwise operations so for 256 pods, it is just 4 x u64 bitwise operations.

Each u64 operation works on 64 pod states at once, so the work stays constant but list and array operations grow linearly with the number of pods.

## Why Cumulative Hashes

The index stores cumulative prefix hashes, not random block hashes. Prefix `k` represents the full prompt prefix up to block `k`, so a match means the pod can reuse that whole prefix.

## System design

<img src="moderl-inference.svg" alt="Architecture diagram" width="1200">
