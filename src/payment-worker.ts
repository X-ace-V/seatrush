// Payment worker: consumes payment requests, charges the provider, and turns
// the held seat into a booking (or frees it on decline).
//
// Kafka delivers at least once, so every step here must be safe to repeat:
//  - the provider charge is keyed by the idempotency key (charged once)
//  - the outcome is applied only while the payment is still 'pending'
import { createHash } from 'node:crypto';
import { pool } from './db.ts';
import { kafka, ensureTopics, PAYMENT_REQUESTS } from './kafka.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Simulated provider: 50 to 150ms per call, declines about 10% of cards.
// The decision is derived from the key, so a retry gets the same answer.
async function charge(key: string): Promise<boolean> {
  await sleep(50 + Math.random() * 100);
  const approved = createHash('sha256').update(key).digest()[0] >= 26;
  // Same key again: the provider returns the original result instead of charging.
  const { rows } = await pool.query(
    `INSERT INTO provider_charges (idempotency_key, approved) VALUES ($1, $2)
     ON CONFLICT (idempotency_key) DO UPDATE SET attempts = provider_charges.attempts + 1
     RETURNING approved`,
    [key, approved],
  );
  return rows[0].approved;
}

async function handle(msg: { key: string; seatId: number; userId: string }) {
  const approved = await charge(msg.key);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rowCount } = await client.query(
      `UPDATE payments SET status = $2 WHERE idempotency_key = $1 AND status = 'pending'`,
      [msg.key, approved ? 'captured' : 'declined'],
    );
    if (rowCount && approved) {
      await client.query(`UPDATE seats SET status = 'booked' WHERE id = $1 AND status = 'paying'`, [msg.seatId]);
      await client.query('INSERT INTO bookings (seat_id, user_id) VALUES ($1, $2)', [msg.seatId, msg.userId]);
    } else if (rowCount) {
      await client.query(
        `UPDATE seats SET status = 'free', held_by = NULL, held_until = NULL WHERE id = $1 AND status = 'paying'`,
        [msg.seatId],
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err; // the batch is redelivered; handle() is safe to repeat
  } finally {
    client.release();
  }
}

await ensureTopics();
const consumer = kafka.consumer({ groupId: 'payment-workers' });
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
