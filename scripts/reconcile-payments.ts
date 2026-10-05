// Reconciler: re-enqueues payments that have been pending longer than
// OLDER_THAN seconds, through the same outbox the API uses. A safety net for
// any path that loses a message (a crash, a skipped batch, an operator
// mistake). Duplicates are harmless: the worker and provider are idempotent.
// Usage: node scripts/reconcile-payments.ts [olderThanSeconds=30]
import { shards, endAll } from '../src/shards.ts';
import { PAYMENT_REQUESTS } from '../src/kafka.ts';

const olderThan = Number(process.argv[2] ?? 30);
const counts = await Promise.all(shards.map(async (s) => (await s.primary.query(
  `INSERT INTO outbox (topic, key, payload)
   SELECT $1, event_id || ':' || seat_no,
          json_build_object('key', idempotency_key, 'eventId', event_id, 'seatNo', seat_no, 'userId', user_id)
   FROM payments WHERE status = 'pending' AND created_at < now() - make_interval(secs => $2)`,
  [PAYMENT_REQUESTS, olderThan],
)).rowCount));
console.log(`re-enqueued pending payments per shard: ${counts.join(' / ')}`);
await endAll();
