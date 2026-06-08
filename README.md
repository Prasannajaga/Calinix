# Cache-Aware Routing Microbench

Tiny std-only Rust benchmark for the router hot path:

- tokenise prompt
- build cumulative prefix hashes
- look up `block_hash -> HostBitmap`
- mask with alive pods
- binary-search longest prefix match
- pick a mock pod
- call local mock functions, not TCP servers

No pods listen on ports. No HTTP. No async runtime. No serde/clap/tokio/hyper.

## Run

Cargo requires `--` before binary arguments:

```bash
cargo run -- --single "the cat sat on the table" --hits 1000
cargo run -- --disaggregated "the cat sat on the table" --hits 1000
```

If you run the compiled binary directly:

```bash
target/debug/cache-aware-routing --single "the cat sat on the table" --hits 1000
target/debug/cache-aware-routing --disaggregated "the cat sat on the table" --hits 1000
```

## What `--hits` Means

`--hits 1000` means run 1000 repeated cache-hit routing iterations against a warmed local index and print microsecond timings.

Example output:

```text
mode: Single
prompt blocks: 2
cache-hit iterations: 1000
last result: RouteResult { ... }
avg route time: 1.234 us
p50 route time: 1.100 us
p95 route time: 1.600 us
selected response pod counts: [1000, 0, 0, 0, 0, 0, 0, 0]
```

## Why HostBitmap

`HostBitmap` is exactly `[u64; 4]`, covering 256 pods. Intersecting cache owners with alive pods or role candidates is just four `u64` operations.

## Why Cumulative Hashes

The index stores cumulative prefix hashes, not random block hashes. Prefix `k` represents the full prompt prefix up to block `k`, so a match means the pod can reuse that whole prefix.

## Files

```text
src/main.rs     CLI and minimal mock routing
src/bitmap.rs   fixed-size HostBitmap
src/hash.rs     deterministic FNV-1a prompt hashing
src/indexer.rs  sharded block index and prefix binary search
src/types.rs    tiny pod/result types
src/tests.rs    focused tests
```
