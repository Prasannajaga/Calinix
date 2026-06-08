# Cache-Aware Routing

Dependency-free local Rust implementation of a Modular-style LLM inference router hot path. It mocks serving pods and focuses on cache-aware routing, bitmap ownership lookups, sticky sessions, load-aware tiebreaking, and a sequenced execution layer for single and disaggregated dispatch.

## Run

```bash
cargo run -- router --spawn-mocks
cargo run -- mock-pod --pod-id 0 --role both --port 9100
cargo run -- event register --pod 0 --prompt "the cat sat on the table"
cargo run -- event evict --pod 0 --prompt "the cat sat on the table"
cargo run -- event shutdown --pod 0
cargo run -- request --session user_123 --prompt "the cat sat on the table" --mode single
cargo run -- request --session user_123 --prompt "the cat sat on the table" --mode disaggregated
cargo run -- bench --requests 1000 --sessions 50
cargo test
```

## Architecture

```text
Event CLI
   |
   v
Admin/Event Server
   |
   v
ShardedBlockIndexer
block_hash -> HostBitmap
alive bitmap

Client Request
   |
   v
Router
   |
   v
Prepare
   |
   v
Filter
   |
   v
Score
   |
   v
Pick
   |
   v
Workflow builds RoutingPlan
   |
   v
Executor
   |
   +--> Single Dispatch: Pod
   |
   +--> Disaggregated Dispatch:
          Step 1: Prefill Pod
          Step 2: Decode Pod
```

## Hot Path And Cold Path

Hot path:

- `Prepare`: tokenize, group into fixed-size blocks, compute raw and cumulative hashes.
- Bitmap lookup: `block_hash -> HostBitmap` through `ShardedBlockIndexer`.
- Alive bitmap masking: dead pods are removed without waiting for cleanup.
- `Filter`: role, health, alive, and max-concurrency filtering.
- `Score`: cache affinity, load, sticky session, and locality.
- `Pick`: deterministic highest score with sticky-session preference.
- `Execute`: run the `RoutingPlan`.

Cold/mock path:

- Mock pod serving over a simple TCP line protocol.
- Background cleanup of dead pod bits from shards.
- Dump/debug admin commands.

## Why HostBitmap

`HostBitmap` is a fixed `[u64; 4]`, covering 256 pods without heap allocation inside the bitmap. That makes intersections cheap: cache owners can be masked by alive pods, role candidates, or other filters with four word operations.

## Why Cumulative Prefix Hashes

A random middle block is not enough for prefix reuse. The router registers cumulative prefix hashes, where position `k` represents the exact prompt prefix chain up to block `k`. If a pod owns that cumulative hash, it owns the reusable prefix through that point.

## Why Binary Search

Prefix matching uses grouped binary search over cumulative hashes. At each midpoint, the index lookup returns a bitmap of pods owning that prefix. Candidates split into `yes` and `no` bitmaps, avoiding a naive pod-by-block scan.

## Why The Execution Loop

Scoring and execution are separated. `Workflow` builds a `RoutingPlan`; the executor only runs steps. The same readable `for step in plan.steps` loop handles single dispatch and disaggregated dispatch. Prefill output updates `ExecutionContext.cache_transfer_id`, which decode then consumes.

## Protocol

Admin/event server on `127.0.0.1:7001` accepts:

```text
REGISTER pod=<id> block=<hash>
EVICT pod=<id> block=<hash>
SHUTDOWN pod=<id>
REGISTER_PROMPT pod=<id> prompt=<prompt>
EVICT_PROMPT pod=<id> prompt=<prompt>
CLEANUP_DEAD pod=<id>
DUMP
```

Request server on `127.0.0.1:7000` accepts:

```text
REQUEST session=<session> mode=<single|disaggregated> prompt=<prompt>
```

Mock pods accept:

```text
SINGLE request_id=<id> session=<session> prompt=<prompt>
PREFILL request_id=<id> session=<session> prompt=<prompt>
DECODE request_id=<id> session=<session> cache_transfer_id=<id> prompt=<prompt>
```

The static local layout uses pod `0` as `Both` so the single-dispatch demo has a valid target. Pods `1`, `2`, and `4` are prefill; pods `3`, `5`, `6`, and `7` are decode.
