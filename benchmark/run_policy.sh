#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUN_NAME="${RUN_NAME:-policy-bench}"
PODS="${PODS:-16}"
SHARDS="${SHARDS:-256}"
REQUESTS="${REQUESTS:-1000}"
CONCURRENCY_SWEEP="${CONCURRENCY_SWEEP:-1,4,8,12,32,64,128}"
BLOCK_SIZE="${BLOCK_SIZE:-32}"
PROMPT_BLOCKS="${PROMPT_BLOCKS:-256}"
SHARED_PREFIX_BLOCKS="${SHARED_PREFIX_BLOCKS:-128}"
SKEW_PERCENT="${SKEW_PERCENT:-90}"
WRITE_RATIO_PERCENT="${WRITE_RATIO_PERCENT:-25}"
LAG_SWEEP_MS="${LAG_SWEEP_MS:-0,100,500,1000,5000}"
QPS="${QPS:-1000}"
CACHE_WEIGHT="${CACHE_WEIGHT:-0.60}"
LOAD_WEIGHT="${LOAD_WEIGHT:-0.30}"
OUTPUT="${OUTPUT:-policy_bench.csv}"
PLOT="${PLOT:-1}"

usage() {
  cat <<'EOF'
usage: benchmark/run_policy.sh [options]

Runs the local Calinix policy benchmark for router overhead, index contention,
hotspot fairness, and staleness sensitivity.

Options:
  --name <name>                    Result directory name.
  --pods <n>                       Synthetic pod count.
  --shards <n>                     Cache index shard count.
  --requests <n>                   Operations per scenario.
  --concurrency-sweep <list>       Comma-separated concurrency levels.
  --block-size <n>                 Cache block size.
  --prompt-blocks <n>              Synthetic prompt length in blocks.
  --shared-prefix-blocks <n>       Shared RAG-style prefix length in blocks.
  --skew-percent <0-100>           Percent hot shared-prefix traffic.
  --write-ratio-percent <n>        Index writes as percent of reads.
  --lag-sweep-ms <list>            Event-lag sweep for staleness.
  --qps <n>                        Simulated staleness arrival rate.
  --cache-weight <f>               Router cache score weight.
  --load-weight <f>                Router load score weight.
  --output <path>                  CSV path or output file name.
  --no-plot                        Skip plot generation.
  -h, --help                       Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      RUN_NAME="$2"
      shift 2
      ;;
    --pods)
      PODS="$2"
      shift 2
      ;;
    --shards)
      SHARDS="$2"
      shift 2
      ;;
    --requests)
      REQUESTS="$2"
      shift 2
      ;;
    --concurrency-sweep)
      CONCURRENCY_SWEEP="$2"
      shift 2
      ;;
    --block-size)
      BLOCK_SIZE="$2"
      shift 2
      ;;
    --prompt-blocks)
      PROMPT_BLOCKS="$2"
      shift 2
      ;;
    --shared-prefix-blocks)
      SHARED_PREFIX_BLOCKS="$2"
      shift 2
      ;;
    --skew-percent)
      SKEW_PERCENT="$2"
      shift 2
      ;;
    --write-ratio-percent)
      WRITE_RATIO_PERCENT="$2"
      shift 2
      ;;
    --lag-sweep-ms)
      LAG_SWEEP_MS="$2"
      shift 2
      ;;
    --qps)
      QPS="$2"
      shift 2
      ;;
    --cache-weight)
      CACHE_WEIGHT="$2"
      shift 2
      ;;
    --load-weight)
      LOAD_WEIGHT="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --no-plot)
      PLOT=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

cargo run --release --bin calinix-policy-bench -- \
  --name "$RUN_NAME" \
  --pods "$PODS" \
  --shards "$SHARDS" \
  --requests "$REQUESTS" \
  --concurrency-sweep "$CONCURRENCY_SWEEP" \
  --block-size "$BLOCK_SIZE" \
  --prompt-blocks "$PROMPT_BLOCKS" \
  --shared-prefix-blocks "$SHARED_PREFIX_BLOCKS" \
  --skew-percent "$SKEW_PERCENT" \
  --write-ratio-percent "$WRITE_RATIO_PERCENT" \
  --lag-sweep-ms "$LAG_SWEEP_MS" \
  --qps "$QPS" \
  --cache-weight "$CACHE_WEIGHT" \
  --load-weight "$LOAD_WEIGHT" \
  --output "$OUTPUT"

if [[ "$PLOT" == "1" ]]; then
  if [[ "$OUTPUT" = /* || "$OUTPUT" == */* ]]; then
    PLOT_INPUT="$OUTPUT"
  else
    PLOT_INPUT="benchmark/results/${RUN_NAME}/${OUTPUT}"
  fi

  uv run python benchmark/plot_policy_bench.py --input "$PLOT_INPUT"
fi
