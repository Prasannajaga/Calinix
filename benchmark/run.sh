#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

URL="${URL:-http://127.0.0.1:18080/v1/chat/completions}"
BENCH_MODE="${BENCH_MODE:-sweep}"

CONCURRENCY_GIVEN=0
if [[ -n "${CONCURRENCY:-}" ]]; then
  CONCURRENCY_GIVEN=1
fi

CONCURRENCY="${CONCURRENCY:-1000}"
CONCURRENCY_SWEEP="${CONCURRENCY_SWEEP:-1,10,50,100,200}"
THREADS="${THREADS:-4}"
BLOCK_SIZE="${BLOCK_SIZE:-32}"
REQUESTS="${REQUESTS-1000}"
TIMEOUT_MS="${TIMEOUT_MS:-30000}"
MODE="${MODE:-single}"
RUN_PREFIX="${RUN_PREFIX-}"
INTERVAL_SECS="${INTERVAL_SECS:-15}"
PLOT="${PLOT:-1}"

# Optional hooks. Use these for cold-cache benchmarking between profiles.
# Example:
#   RESET_CMD='docker compose -f e2e/routing/docker-compose.yml restart calinix-router'
#   RESET_SLEEP_SECS=20 benchmark/run.sh
RESET_CMD="${RESET_CMD:-}"
RESET_SLEEP_SECS="${RESET_SLEEP_SECS:-20}"

usage() {
  cat <<'EOF'
usage: benchmark/run.sh [options]

Runs short, mixed, and huge payload profiles.

Options:
  --bench-mode sweep|single     Use concurrency sweep or one fixed concurrency.
  --concurrency <n>             Fixed concurrency for --bench-mode single.
  --concurrency-sweep <list>    Comma-separated sweep values.
  --requests <n>                Fixed-request mode. Empty env REQUESTS= selects duration mode.
  --timeout-ms <n>              Duration-mode milliseconds when requests are disabled.
  --run-prefix <name>           Result directory prefix.
  --mode <single|disaggregated> Calinix routing mode header.
  --url <url>                   Load balancer OpenAI-compatible endpoint.
  --threads <n>                 Tokio worker threads for the benchmark client.
  --block-size <n>              Cache block size for benchmark metrics.
  --interval-secs <n>           Sleep between payload profiles.
  --no-plot                     Skip plot generation.
  --reset-cmd <cmd>             Command to run before each profile.
  --reset-sleep-secs <n>        Sleep after reset command.
  -h, --help                    Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bench-mode)
      BENCH_MODE="$2"
      shift 2
      ;;
    --concurrency)
      CONCURRENCY="$2"
      CONCURRENCY_GIVEN=1
      shift 2
      ;;
    --concurrency-sweep)
      CONCURRENCY_SWEEP="$2"
      shift 2
      ;;
    --requests)
      REQUESTS="$2"
      shift 2
      ;;
    --timeout-ms)
      TIMEOUT_MS="$2"
      REQUESTS=""
      shift 2
      ;;
    --run-prefix)
      RUN_PREFIX="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --url)
      URL="$2"
      shift 2
      ;;
    --threads)
      THREADS="$2"
      shift 2
      ;;
    --block-size)
      BLOCK_SIZE="$2"
      shift 2
      ;;
    --interval-secs)
      INTERVAL_SECS="$2"
      shift 2
      ;;
    --no-plot)
      PLOT=0
      shift
      ;;
    --reset-cmd)
      RESET_CMD="$2"
      shift 2
      ;;
    --reset-sleep-secs)
      RESET_SLEEP_SECS="$2"
      shift 2
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

if [[ "$CONCURRENCY_GIVEN" -eq 1 ]]; then
  BENCH_MODE="single"
fi

if [[ -n "$REQUESTS" ]]; then
  TIMEOUT_MS=""
fi

if [[ "$BENCH_MODE" != "sweep" && "$BENCH_MODE" != "single" ]]; then
  echo "--bench-mode must be sweep or single" >&2
  exit 1
fi

if [[ -z "$RUN_PREFIX" ]]; then
  RUN_PREFIX="$BENCH_MODE"
fi

LOG_ROOT="benchmark/results/${RUN_PREFIX}-logs"
mkdir -p "$LOG_ROOT"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*"
}

run_profile() {
  local profile="$1"
  local payload_file="$2"
  local run_name="${RUN_PREFIX}-${profile}"
  local result_dir="benchmark/results/${run_name}"
  local log_file="${LOG_ROOT}/${profile}.log"
  local output_file="url_bench_sweep.csv"

  log "starting profile=${profile} payload=${payload_file} run=${run_name}"

  if [[ -n "$RESET_CMD" ]]; then
    log "resetting server/cache before ${profile}: ${RESET_CMD}"
    bash -c "$RESET_CMD" 2>&1 | tee -a "$log_file"
    log "sleeping ${RESET_SLEEP_SECS}s after reset"
    sleep "$RESET_SLEEP_SECS"
  fi

  local cmd=(
    cargo run --bin calinix-url-bench --
    --name "$run_name"
    --url "$URL"
    --threads "$THREADS"
    --payload file "$payload_file"
    --block-size "$BLOCK_SIZE"
  )

  if [[ "$BENCH_MODE" == "sweep" ]]; then
    cmd+=(--concurrency-sweep "$CONCURRENCY_SWEEP")
    output_file="url_bench_sweep.csv"
  else
    cmd+=(--concurrency "$CONCURRENCY")
    output_file="url_bench.csv"
  fi

  cmd+=(--output "$output_file")

  if [[ -n "$REQUESTS" ]]; then
    cmd+=(--requests "$REQUESTS")
  else
    cmd+=(--timeout-ms "$TIMEOUT_MS")
  fi

  if [[ -n "$MODE" ]]; then
    cmd+=(--mode "$MODE")
  fi

  log "command: ${cmd[*]}"
  "${cmd[@]}" 2>&1 | tee "$log_file"

  if [[ "$PLOT" == "1" ]]; then
    log "plotting ${result_dir}/${output_file}"
    uv run python benchmark/plot_url_bench.py \
      --input "${result_dir}/${output_file}" 2>&1 | tee -a "$log_file"
  fi

  log "finished profile=${profile}; results=${result_dir}; log=${log_file}"
}

log "benchmark ${BENCH_MODE} started"
log "url=${URL}"
if [[ "$BENCH_MODE" == "sweep" ]]; then
  log "concurrency_sweep=${CONCURRENCY_SWEEP}"
else
  log "concurrency=${CONCURRENCY}"
fi
if [[ -n "$REQUESTS" ]]; then
  if [[ "$BENCH_MODE" == "sweep" ]]; then
    log "mode=fixed-request requests_per_concurrency=${REQUESTS}"
  else
    log "mode=fixed-request requests_per_profile=${REQUESTS}"
  fi
else
  log "mode=duration timeout_ms=${TIMEOUT_MS}"
fi
log "interval_secs=${INTERVAL_SECS}"

run_profile short benchmark/data/short_payloads.json
log "sleeping ${INTERVAL_SECS}s before next profile"
sleep "$INTERVAL_SECS"

run_profile mixed benchmark/data/mixed_payloads.json
log "sleeping ${INTERVAL_SECS}s before next profile"
sleep "$INTERVAL_SECS"

run_profile huge benchmark/data/huge_payloads.json

log "benchmark ${BENCH_MODE} finished"
log "result dirs:"
log "  benchmark/results/${RUN_PREFIX}-short"
log "  benchmark/results/${RUN_PREFIX}-mixed"
log "  benchmark/results/${RUN_PREFIX}-huge"
log "logs: ${LOG_ROOT}"
