#!/usr/bin/env bash
# run-integration-tests.sh — spin up a Redis container, run the full test
# suite (miniredis + real-Redis integration tests), then tear down.
#
# Usage:
#   ./scripts/run-integration-tests.sh
#   REDIS_PORT=6399 ./scripts/run-integration-tests.sh
#
# The EXIT trap ensures the container is removed on success, failure,
# or Ctrl-C. Exit code reflects go test result.

set -euo pipefail

CONTAINER="${CONTAINER:-dasher-test-redis}"
PORT="${REDIS_PORT:-6399}"

cleanup() {
    echo "--- removing container ${CONTAINER}"
    docker rm -f "$CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

echo "--- starting Redis container on port ${PORT}"
docker run -d \
    --name "$CONTAINER" \
    -p "${PORT}:6379" \
    redis:7-alpine

echo "--- waiting for Redis to be ready"
until docker exec "$CONTAINER" redis-cli ping 2>/dev/null | grep -q PONG; do
    sleep 0.2
done
echo "--- Redis ready"

cd "$(dirname "$0")/.."
echo "--- running full test suite (REDIS_ADDR=localhost:${PORT})"
REDIS_ADDR="localhost:${PORT}" go test ./... -count=1
