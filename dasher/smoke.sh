#!/usr/bin/env bash
set -euo pipefail

REDIS_ADDR="${DASHER_REDIS_ADDR:-localhost:6379}"
INSTANCE=bayer-17909
STREAM="${INSTANCE}.cdc.orders"
GROUP=dasher

echo "=== Ensuring Redis is running ==="
docker exec rp_redis redis-cli ping >/dev/null 2>&1 || {
  docker compose -f ../../docker-compose.yml up -d redis 2>/dev/null || true
  until docker exec rp_redis redis-cli ping >/dev/null 2>&1; do sleep 1; done
}
echo "Redis OK"

echo "=== Building dasher ==="
go build -o /tmp/dasher ./cmd/dasher

echo "=== Creating consumer group ==="
docker exec rp_redis redis-cli XGROUP CREATE "$STREAM" "$GROUP" '$' MKSTREAM 2>/dev/null || true

echo "=== Starting dasher in background ==="
DASHER_INSTANCE_ID="$INSTANCE" \
  DASHER_CONFIG=config.smoke.yaml \
  DASHER_REDIS_ADDR="$REDIS_ADDR" \
  DASHER_AUTH_TOKEN=t \
  /tmp/dasher &
DASHER_PID=$!
trap 'kill $DASHER_PID 2>/dev/null || true' EXIT
sleep 1

echo "=== Seeding stream with XADD ==="
docker exec rp_redis redis-cli XADD "$STREAM" '*' \
  op insert \
  table orders \
  schema public \
  lsn 0/1 \
  streamed_at "$(date -u +%FT%TZ)" \
  data '{"id":1}'

echo "=== Waiting for ack (up to 5s) ==="
for i in $(seq 1 50); do
  count=$(docker exec rp_redis redis-cli --raw XPENDING "$STREAM" "$GROUP" | head -1)
  if [ "$count" = "0" ]; then
    echo "SUCCESS: entry acked (0 pending)"
    exit 0
  fi
  sleep 0.1
done

echo "FAIL: entry still pending after 5s"
exit 1
