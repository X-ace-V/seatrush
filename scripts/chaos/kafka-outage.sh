#!/bin/bash
# Kafka goes down mid-sale, and two API replicas crash during the outage.
# Pass: after draining, no payment is stuck pending and nobody is charged twice.
# Usage: scripts/chaos/kafka-outage.sh [users]   (stack must be up, 4 app replicas)
set -u
cd "$(dirname "$0")/../.."
out=$(mktemp)
node scripts/seed.ts >/dev/null
node scripts/pay-check.ts "${1:-400}" > "$out" 2>&1 &
sleep 3; echo ">> stopping kafka"; docker compose stop kafka >/dev/null 2>&1
sleep 5; echo ">> killing app-1 and app-2"; docker kill seatrush-app-1 seatrush-app-2 >/dev/null; docker start seatrush-app-1 seatrush-app-2 >/dev/null
sleep 5; echo ">> starting kafka"; docker compose start kafka >/dev/null 2>&1
wait; cat "$out"
sleep 15 # let the relay and worker drain
npm run -s check
