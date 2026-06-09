# Cache-Aware Routing Microbench

Tiny std-only Rust benchmark for the router hot path:
 

## Run

Cargo requires `--` before binary arguments:

```bash
cargo run -- --single "the cat sat on the table" --hits 1000
cargo run -- --disaggregated "the cat sat on the table" --hits 1000
cargo run -- bench-part2-query --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
cargo run -- bench-part2-compare --iterations 100000 --pods 64 --blocks 32 --dropoffs 4
cargo run -- bench-part2-shards --chains 10000 --blocks-per-chain 64
cargo run -- bench-part2-concurrency --readers 8 --writers 2 --duration-secs 10 --pods 64 --blocks 32
```

If you run the compiled binary directly:

```bash
target/debug/cache-aware-routing --single "the cat sat on the table" --hits 1000
target/debug/cache-aware-routing --disaggregated "the cat sat on the table" --hits 1000
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

## Why Cumulative Hashes

The index stores cumulative prefix hashes, not random block hashes. Prefix `k` represents the full prompt prefix up to block `k`, so a match means the pod can reuse that whole prefix.
 

## System design 

<img src="moderl-inference.svg" alt="Architecture diagram" width="1200"> 