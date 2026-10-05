// Payment worker: consumes payment requests, charges the provider, and turns
// the held seat into a booking (or frees it on decline).
//
// Kafka delivers at least once, so every step here must be safe to repeat:
//  - the provider charge is keyed by the idempotency key (charged once)
//  - the outcome is applied only while the payment is still 'pending'
import { createHash } from 'node:crypto';
import { catalog, route, startPartitionMapRefresh } from './shards.ts';
import { kafka, ensureTopics, PAYMENT_REQUESTS } from './kafka.ts';
import { prom, serveMetrics } from './metrics.ts';

const processed = new prom.Counter({ name: 'payments_processed_total', help: 'Payment messages handled', labelNames: ['outcome'] });
const providerSeconds = new prom.Histogram({ name: 'provider_charge_seconds', help: 'Payment provider call latency', buckets: [0.05, 0.1, 0.15, 0.25, 0.5, 1] });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Simulated provider: 50 to 150ms per call, declines about 10% of cards.
// The decision is derived from the key, so a retry gets the same answer.
async function charge(key: string): Promise<boolean> {
  await sleep(50 + Math.random() * 100);
  const approved = createHash('sha256').update(key).digest()[0] >= 26;
  // Same key again: the provider returns the original result instead of charging.
  // The simulated provider's ledger lives in the catalog, outside our shards.
  const { rows } = await catalog.query(
    `INSERT INTO provider_charges (idempotency_key, approved) VALUES ($1, $2)
     ON CONFLICT (idempotency_key) DO UPDATE SET attempts = provider_charges.attempts + 1
     RETURNING approved`,
    [key, approved],
  );
  return rows[0].approved;
}

async function handle(msg: { key: string; eventId: number; seatNo: number; userId: string }) {
  const stopTimer = providerSeconds.startTimer();
  const approved = await charge(msg.key);
  stopTimer();
  const { primary, part } = route(msg.eventId, msg.seatNo);
  const client = await primary.connect();
  try {
    await client.query('BEGIN');
    const { rowCount } = await client.query(
      `UPDATE payments SET status = $2 WHERE idempotency_key = $1 AND status = 'pending'`,
      [msg.key, approved ? 'captured' : 'declined'],
    );
    if (rowCount && approved) {
      await client.query(`UPDATE seats SET status = 'booked' WHERE event_id = $1 AND seat_no = $2 AND status = 'paying'`, [msg.eventId, msg.seatNo]);
      await client.query('INSERT INTO bookings (event_id, seat_no, part, user_id) VALUES ($1, $2, $3, $4)', [msg.eventId, msg.seatNo, part, msg.userId]);
    } else if (rowCount) {
      await client.query(
        `UPDATE seats SET status = 'free', held_by = NULL, held_until = NULL WHERE event_id = $1 AND seat_no = $2 AND status = 'paying'`,
        [msg.eventId, msg.seatNo],
      );
    }
    await client.query('COMMIT');
    // 'duplicate' = redelivered message whose outcome was already applied.
    processed.inc({ outcome: !rowCount ? 'duplicate' : approved ? 'captured' : 'declined' });
  } catch (err) {
    await client.query('ROLLBACK');
    throw err; // the batch is redelivered; handle() is safe to repeat
  } finally {
    client.release();
  }
}

serveMetrics();
await startPartitionMapRefresh();
await ensureTopics();
// Recovery tuning, found on the dashboard: after a 40s Kafka outage, payments
// stayed stalled ~34s after Kafka was back.
//  - sessionTimeout: a dead member (SIGKILL, or a consumer that crashed during
//    the outage) is only evicted after this. 30s default, now 10s. The risk is
//    a false rebalance if the process stalls longer, which Node should not.
//  - retry: caps kafkajs's reconnect backoff so the consumer retries sooner.
// Together: 34s to 15s after an outage, and 30s+ to 11s after a SIGKILL.
const consumer = kafka.consumer({
  groupId: 'payment-workers', sessionTimeout: 10_000, heartbeatInterval: 3_000,
  retry: { initialRetryTime: 300, maxRetryTime: 2_000, retries: 10 },
});
await consumer.connect();
await consumer.subscribe({ topic: PAYMENT_REQUESTS, fromBeginning: true });

// Messages in a batch are independent seats, so charge them concurrently.
// Offsets are committed only after the whole batch succeeds.
await consumer.run({
  eachBatch: async ({ batch }) => {
    await Promise.all(batch.messages.map((m) => handle(JSON.parse(m.value!.toString()))));
  },
});
console.log('payment worker running');
