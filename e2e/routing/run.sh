#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/e2e/routing/docker-compose.yml"
CALINIX_E2E_URL="${CALINIX_E2E_URL:-http://127.0.0.1:18080}"

cd "$ROOT_DIR"
docker compose -f "$COMPOSE_FILE" up --build -d

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if curl -fsS "$CALINIX_E2E_URL/health" >/dev/null; then
    break
  fi
  sleep 1
done

curl -fsS "$CALINIX_E2E_URL/health" >/dev/null

CALINIX_E2E_URL="$CALINIX_E2E_URL" \
  cargo test exposes_openai_compatible_routes_through_http_api \
  -- --ignored --nocapture
