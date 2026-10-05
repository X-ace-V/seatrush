#!/bin/bash
# SIGKILL the payment worker three times while it is charging a backlog, so
# Kafka redelivers batches that were already charged.
# Pass: provider deduplicates the retries by idempotency key, nobody is
# charged twice, nothing stuck after draining.
set -u
cd "$(dirname "$0")/../.."
out=$(mktemp)
charges() { docker exec seatrush-postgres-1 psql -U seatrush -tAc 'select count(*) from provider_charges'; }
node scripts/seed.ts >/dev/null
docker stop seatrush-payment-worker-1 >/dev/null   # let a backlog build up
node scripts/pay-check.ts "${1:-400}" > "$out" 2>&1 &
sleep 10
for i in 1 2 3; do
  before=$(charges); docker start seatrush-payment-worker-1 >/dev/null
  until [ "$(charges)" -gt "$before" ]; do sleep 0.05; done   # it is charging right now
  echo ">> kill -9 payment-worker mid-batch"; docker kill seatrush-payment-worker-1 >/dev/null
done
docker start seatrush-payment-worker-1 >/dev/null
wait; cat "$out"
sleep 10
docker exec seatrush-postgres-1 psql -U seatrush -tAc \
  "select 'redelivered charges deduplicated: ' || coalesce(sum(attempts - 1), 0) from provider_charges"
npm run -s check
