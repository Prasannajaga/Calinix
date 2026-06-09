# Part 2 Benchmark Walkthrough

This file explains the Part 2 benchmarks in `src/bench_part2.rs`.

The important point: these commands benchmark the data-layer query path, not mocked execution. The mocked single/disaggregated execution path sleeps for about 100ms, so it would hide whether the routing metadata query itself stays in microseconds.

## CLI Entry Points

The command dispatch is in `src/main.rs`.

- `src/main.rs:1` loads `mod bench_part2`.
- `src/main.rs:26` reads command-line args.
- `src/main.rs:28` calls `run_microbench_command`.
- `src/main.rs:104` dispatches `bench-part2-query`.
- `src/main.rs:110` dispatches `bench-part2-compare`.
- `src/main.rs:116` dispatches `bench-part2-shards`.
- `src/main.rs:120` dispatches `bench-part2-concurrency`.

Run the four Part 2 commands like this:

```bash
cargo run -- bench-part2-query --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
cargo run -- bench-part2-compare --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
cargo run -- bench-part2-shards --chains 10000 --blocks-per-chain 64
cargo run -- bench-part2-concurrency --readers 8 --writers 2 --duration-secs 10 --pods 64 --blocks 32
```

## Shared Building Blocks

All Part 2 benchmarks use the same core pieces.

### 1. Cumulative Block Hashes

Code reference: `src/hash.rs:5`, `src/hash.rs:32`, `src/hash.rs:61`.

`BlockHash` is a `u64`. The benchmark does not use independent random hashes. It creates cumulative hashes:

```text
local block hash = hash("chain=<chain_id>:block=<i>")
cumulative hash = hash(previous cumulative hash, local block hash)
```

Step by step:

1. `make_synthetic_chain` starts with the FNV offset basis.
2. For each block index, it hashes a deterministic string for that chain/block.
3. It calls `combine_cumulative(prev, local)`.
4. It pushes the cumulative hash into the query chain.
5. It sets `prev = cumulative` for the next block.

This models the router rule that block `K` represents the whole prefix through block `K`.

### 2. HostBitmap Storage

Code reference: `src/bitmap.rs:3`, `src/bitmap.rs:7`, `src/bitmap.rs:25`, `src/bitmap.rs:57`, `src/bitmap.rs:73`, `src/bitmap.rs:87`.

`HostBitmap` is the storage format for `blockHash -> pods that own this prefix`.

Step by step:

1. It supports up to 256 pods.
2. It stores those 256 bits in `[u64; 4]`.
3. `set(pod)` marks that a pod owns a block hash.
4. `clear(pod)` removes that pod from the bitmap.
5. `contains(pod)` tests one pod.
6. `and(other)` intersects two pod sets.
7. `minus(other)` removes one pod set from another.
8. `for_each_set_bit` iterates only pods that are present.

The important operation in the query path is:

```text
owners_alive = owners_for_hash & global_alive
```

That is only four `u64` AND operations.

### 3. Sharded Index

Code reference: `src/indexer.rs:9`, `src/indexer.rs:15`, `src/indexer.rs:27`, `src/indexer.rs:41`, `src/indexer.rs:50`, `src/indexer.rs:61`, `src/indexer.rs:68`, `src/indexer.rs:78`.

The index is:

```text
Vec<RwLock<HashMap<BlockHash, HostBitmap>>>
```

Step by step:

1. There are 256 shards.
2. A block hash is mapped to a shard with Fibonacci hashing.
3. Each shard has its own `RwLock`.
4. Each shard stores `BlockHash -> HostBitmap`.
5. `register(pod, hash)` writes one bit into one shard.
6. `evict(pod, hash)` clears one bit from one shard.
7. `shutdown(pod)` only clears the pod in the global alive bitmap.
8. `owners(hash)` reads the owner bitmap from one shard.
9. `owners_alive(hash)` returns `owners(hash).and(alive())`.

Shutdown is intentionally cheap. It does not scan every shard. Dead pods are filtered at read time by `owners_alive`.

### 4. Latency Summaries

Code reference: `src/metrics.rs:12`, `src/metrics.rs:25`, `src/metrics.rs:88`.

Every benchmark records elapsed nanoseconds into preallocated vectors. Then `summarize_ns`:

1. Sorts the samples in place.
2. Computes min, max, and average.
3. Computes p50 at `len * 50 / 100`.
4. Computes p95 at `len * 95 / 100`.
5. Computes p99 at `len * 99 / 100`.
6. Clamps percentile indexes to `len - 1`.

The benchmark output prints microseconds because the goal is to show the data-layer path is microsecond-scale.

## Synthetic Cache State

Code reference: `src/bench_part2.rs:13`, `src/bench_part2.rs:60`.

`build_synthetic_state(pods, blocks, dropoffs)` builds the benchmark fixture.

It returns:

```text
SyntheticPart2State {
  indexer,
  query_chain,
  expected_depths,
  candidate_pods,
}
```

Step by step:

1. Clamp pod count to `1..=256`.
2. Build one cumulative query chain with `make_synthetic_chain`.
3. Create a `ShardedBlockIndexer`.
4. Create an empty `candidate_pods` bitmap.
5. Split pods across `dropoffs` distinct prefix depths.
6. For each pod, choose its prefix depth.
7. Add the pod to `candidate_pods`.
8. Register `query_chain[0..depth]` for that pod.
9. Save the expected depth for correctness tests.

Example:

```text
pods=8
blocks=8
dropoffs=4

depths = 8, 6, 4, 2, 8, 6, 4, 2
```

A pod with depth `6` gets the first six cumulative block hashes. This preserves the Modular prefix invariant: if a pod has block `K`, it also has the prior prefix.

## Benchmark 1: `bench-part2-query`

Code reference: `src/bench_part2.rs:229`.

Command:

```bash
cargo run -- bench-part2-query --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
```

This is the main benchmark. It measures the exact Part 2 hot path:

```text
query chain of cumulative block hashes
-> Fibonacci shard selection
-> sharded map lookup: blockHash -> HostBitmap
-> intersect with global alive bitmap
-> binary search prefix matching
-> return prefix depth per pod
```

Step by step:

1. `bench_part2_query` clamps arguments and builds synthetic state.
2. It allocates the latency sample vector before the measured loop.
3. It allocates the `depths` buffer before the measured loop.
4. It allocates the binary-search `stack` before the measured loop.
5. For each iteration, it starts a timer.
6. It calls `query_prefix_depths_binary_into`.
7. It records elapsed nanoseconds.
8. It accumulates shard lookup, bitmap intersection, and search-frame counters.
9. It adds the returned depths into `checksum` so the compiler cannot remove the work.
10. After all iterations, it summarizes latency and prints metrics.

The measured operation is the call at `src/bench_part2.rs:243`.

The printed fields mean:

- `latency_p50_us`: median query time.
- `latency_p95_us`: 95th percentile query time.
- `latency_p99_us`: 99th percentile query time.
- `latency_max_us`: slowest observed query.
- `avg_shard_lookups_per_query`: how many `blockHash -> HostBitmap` reads each query needed.
- `avg_bitmap_intersections_per_query`: how many bitmap set operations each query did.
- `avg_search_frames_per_query`: how many grouped binary-search frames were processed.
- `checksum`: correctness/anti-optimization guard.

## Binary Prefix Query

Code reference: `src/bench_part2.rs:35`, `src/bench_part2.rs:100`, `src/bench_part2.rs:118`.

This is the query used by `bench-part2-query`.

The search frame is:

```text
SearchFrame {
  min_prefix_depth,
  max_prefix_depth,
  candidate_pods,
}
```

`min_prefix_depth` and `max_prefix_depth` are prefix depths, not array indexes. They range from `0` to `query_chain.len()`.

Step by step:

1. Clear the output `depths` buffer.
2. Clear the reusable search stack.
3. Compute `alive_candidate_pods = candidate_pods & indexer.alive()`.
4. Push one initial frame: `min_prefix_depth=0`, `max_prefix_depth=query_chain.len()`, `candidate_pods=alive_candidate_pods`.
5. Pop a frame.
6. If `candidate_pods` is empty, skip it.
7. If `min_prefix_depth == max_prefix_depth`, assign that prefix depth to every pod in `candidate_pods`.
8. Otherwise compute `probe_prefix_depth = (min_prefix_depth + max_prefix_depth + 1) / 2`.
9. Lookup `owners_alive(query_chain[probe_prefix_depth - 1])`.
10. Split pods into `pods_at_or_above_probe = candidate_pods & pods_with_probe_prefix` and `pods_below_probe = candidate_pods - pods_at_or_above_probe`.
11. Pods in `pods_at_or_above_probe` have at least `probe_prefix_depth`, so they continue to the upper half.
12. Pods in `pods_below_probe` do not have `probe_prefix_depth`, so they search the lower half.
13. Repeat until all live candidate pods are assigned a prefix depth.

Why this is not `N x P`:

The algorithm does not test every pod against every block. It carries groups of pods as bitmaps and splits them with bitmap operations. One shard lookup can classify many pods at once.

## Benchmark 2: `bench-part2-compare`

Code reference: `src/bench_part2.rs:177`, `src/bench_part2.rs:198`, `src/bench_part2.rs:287`.

Command:

```bash
cargo run -- bench-part2-compare --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
```

This benchmark compares the grouped binary query against a naive scan.

Step by step:

1. Build the same synthetic state as `bench-part2-query`.
2. Allocate binary latency samples before the loop.
3. Allocate naive latency samples before the loop.
4. Allocate reusable output buffers before the loop.
5. For each iteration, run `query_prefix_depths_binary_into`.
6. Record binary elapsed time.
7. Run `query_prefix_depths_naive_into`.
8. Record naive elapsed time.
9. Assert that both depth arrays are exactly equal.
10. Accumulate lookup counts and pod bit tests.
11. Print latency, work counters, speedup, and checksum.

The naive query works like this:

1. Compute `alive_candidates = candidates & indexer.alive()`.
2. Start with all alive candidates still matching.
3. For every block hash in the query chain, call `owners_alive(hash)`.
4. For every candidate pod that is still matching, test `owners.contains(pod)`.
5. If the pod owns the block, update its depth.
6. If the pod misses, remove it from the matching set.

The naive query is intentionally simple. It is there to prove correctness and show how much work binary grouping avoids.

The printed fields mean:

- `binary_p50_us`, `binary_p95_us`, `binary_p99_us`: latency for grouped binary search.
- `binary_avg_shard_lookups`: average block-hash lookups for binary search.
- `naive_p50_us`, `naive_p95_us`, `naive_p99_us`: latency for naive scanning.
- `naive_avg_shard_lookups`: average block-hash lookups for naive scanning.
- `naive_avg_pod_bit_tests`: average per-pod membership tests.
- `speedup_p50`: `naive_p50 / binary_p50`.
- `speedup_p99`: `naive_p99 / binary_p99`.
- `lookup_reduction`: `naive_avg_shard_lookups / binary_avg_shard_lookups`.
- `checksum`: correctness/anti-optimization guard.

Note: very small runs may not show a clean speedup because timing noise dominates. Use the full command size for meaningful results.

## Benchmark 3: `bench-part2-shards`

Code reference: `src/indexer.rs:15`, `src/indexer.rs:19`, `src/bench_part2.rs:369`, `src/bench_part2.rs:549`.

Command:

```bash
cargo run -- bench-part2-shards --chains 10000 --blocks-per-chain 64
```

This benchmark checks shard distribution.

Step by step:

1. Create two arrays of 256 counters: one for low-bit sharding and one for Fibonacci sharding.
2. For each chain id, build a cumulative synthetic chain.
3. For every cumulative hash in that chain, increment the low-bit shard counter.
4. For the same hash, increment the Fibonacci shard counter.
5. Compute distribution statistics for both arrays.
6. Print both distributions.

Low-bit shard selection:

```text
shard = hash & 255
```

Fibonacci shard selection:

```text
shard = top 8 bits of hash * 0x9E3779B97F4A7C15
```

The printed fields mean:

- `min_entries`: fewest hashes in any shard.
- `max_entries`: most hashes in any shard.
- `avg_entries`: average hashes per shard.
- `stddev`: spread around the average.
- `empty_shards`: shards that received no hashes.
- `skew_ratio`: `max_entries / avg_entries`.
- `hottest_shard`: shard id with the most entries.

Lower skew and lower standard deviation mean better distribution.

## Benchmark 4: `bench-part2-concurrency`

Code reference: `src/bench_part2.rs:396`.

Command:

```bash
cargo run -- bench-part2-concurrency --readers 8 --writers 2 --duration-secs 10 --pods 64 --blocks 32
```

This benchmark stresses the sharded index with concurrent query readers and metadata writers.

Step by step:

1. Clamp args and build synthetic state.
2. Wrap the indexer and query chain in `Arc`.
3. Create shared atomic counters for reader ops, writer ops, dead-pod returns, checksum, and stop flag.
4. Spawn reader threads.
5. Spawn writer threads.
6. Sleep for `duration_secs`.
7. Set the stop flag.
8. Join all threads.
9. Merge reader latency samples.
10. Merge writer latency samples.
11. Summarize reader query latency and writer event latency.
12. Print throughput, latency, correctness guard, and checksum.

Reader thread loop:

1. Snapshot `alive_before`.
2. Run the full binary prefix query.
3. Record query latency.
4. Check whether any pod that was dead in `alive_before` got a nonzero depth.
5. Add to checksum.
6. Increment reader op count.

Writer thread loop:

1. Pick a deterministic hash from the query chain.
2. Pick a deterministic pod id.
3. Apply one idempotent event:
   - register
   - evict
   - register
4. Occasionally call `shutdown(pod)`.
5. Record event latency.
6. Increment writer op count.

The printed fields mean:

- `reader_ops`: total completed prefix queries.
- `writer_ops`: total metadata events.
- `reader_ops_per_sec`: query throughput.
- `writer_ops_per_sec`: event throughput.
- `query_p50_us`, `query_p95_us`, `query_p99_us`: reader query latency.
- `event_p50_us`, `event_p95_us`, `event_p99_us`: writer event latency.
- `dead_pod_returned_count`: correctness guard for alive masking.
- `checksum`: anti-optimization guard.

`dead_pod_returned_count` should be zero. If it is not zero, the benchmark prints:

```text
ERROR: dead pod returned by prefix query
```

## How To Read Results

For the Part 2 data-layer claim, start with:

```bash
cargo run -- bench-part2-query --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
```

The headline result is `latency_p99_us`.

Then run:

```bash
cargo run -- bench-part2-compare --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
```

Use this to confirm:

1. Binary and naive depths match.
2. Binary uses fewer shard lookups.
3. Binary avoids many per-pod bit tests.

Then run:

```bash
cargo run -- bench-part2-shards --chains 10000 --blocks-per-chain 64
```

Use this to inspect hash distribution across 256 shards.

Finally run:

```bash
cargo run -- bench-part2-concurrency --readers 8 --writers 2 --duration-secs 10 --pods 64 --blocks 32
```

Use this to confirm:

1. Queries remain microsecond-scale under concurrent metadata events.
2. Writers can update shards independently.
3. Shutdown is cheap because it only updates the global alive bitmap.
4. Dead pods are not returned by prefix queries.

## What Is Not Being Benchmarked Here

These Part 2 commands do not benchmark:

- mocked model execution
- network transport
- HTTP handlers
- serialization
- isolated bitmap methods as the main result

The only primary result is the full metadata query path:

```text
cumulative query chain
-> Fibonacci shard selection
-> sharded map lookup
-> alive bitmap intersection
-> grouped binary prefix matching
-> prefix depth per pod
```


```bash
--readers 10000
```

Means: start **10,000 query workers**.

Example: imagine 10,000 users all asking, “Which pod should handle this request?” at the same time.

```bash
--writers 0
```

Means: start **0 update workers**.

Example: no pods are being added/removed/changed while the queries run. It is read-only traffic.

```bash
--duration-secs 10
```

Means: run the test for **10 seconds**.

Example: those 10,000 query workers keep asking questions repeatedly for 10 seconds, then the benchmark stops and prints results.

```bash
--pods 64
```

Means: simulate **64 pods/servers**.

Example: the router has 64 possible backend pods it can choose from or check cache matches against.

```bash
--blocks 32
```

Means: each query chain has **32 blocks**.

Example: pretend the incoming prompt/cache history is split into 32 chunks. The benchmark checks how many of those chunks each pod already has cached.

So together:

```bash
cargo run --release -- bench-part2-concurrency \
  --readers 10000 \
  --writers 0 \
  --duration-secs 10 \
  --pods 64 \
  --blocks 32
```

means:

> “For 10 seconds, simulate 10,000 concurrent query workers repeatedly checking cache-prefix matches across 64 pods, where each query has 32 blocks, with no pod update traffic happening.”
