import Fastify from 'fastify';
import { pool, replica } from './db.ts';
import { cached, redis } from './cache.ts';
import { waitingRoom, waitingRoomOn, hasPass } from './waiting-room.ts';
import { PAYMENT_REQUESTS } from './kafka.ts';
import { instrument, watchPools } from './metrics.ts';

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } });

instrument(app);
watchPools({ primary: pool, replica });
app.get('/health', async () => ({ ok: true }));
waitingRoom(app);

// Seat maps read from the replica so browse traffic cannot slow down bookings
// on the primary. READS_FROM=primary switches back, for comparison runs.
const reader = process.env.READS_FROM === 'primary' ? pool : replica;

// Read-your-writes: a client that just booked sends back the `lsn` it got
// (x-min-lsn). If the replica has not replayed up to that point yet, read
// from the primary instead so the user always sees their own booking.
// This must be a separate query BEFORE the read: checking inside the same
// query would race, because the read's snapshot is taken first.
async function freshReader(minLsn: string) {
  if (reader === pool) return reader;
  const { rows } = await replica.query('SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn AS fresh', [minLsn]);
  return rows[0].fresh ? replica : pool;
}

// Seat maps are served one 1000-seat section at a time, like real ticketing
// sites: a 50K-seat stadium map would be megabytes of JSON.
// An expired hold is reported as free: it is free, nobody has reclaimed it yet.
const SECTION_SIZE = 1000;
const seatMap = (db: typeof pool, eventId: number, section: number) =>
  db.query(
    `SELECT seat_no AS "seatNo", label,
            CASE WHEN status = 'held' AND held_until < now() THEN 'free' ELSE status END AS status
     FROM seats WHERE event_id = $1 AND seat_no BETWEEN $2 AND $3 ORDER BY seat_no`,
    [eventId, section * SECTION_SIZE + 1, (section + 1) * SECTION_SIZE],
  ).then((r) => r.rows);

// Browsers get a seat map up to CACHE_TTL_MS stale. That is safe because the
// booking itself is checked atomically on the primary. We do not invalidate
// on each booking: on a hot event that would empty the cache constantly.
// A user holding an x-min-lsn token skips the cache to see their own booking.
app.get<{ Params: { id: string; section: string } }>('/events/:id/sections/:section/seats', async (req, reply) => {
  const [eventId, section] = [Number(req.params.id), Number(req.params.section)];
  const minLsn = req.headers['x-min-lsn'] as string | undefined;
  if (minLsn) return seatMap(await freshReader(minLsn), eventId, section);

  const json = await cached(`seats:${eventId}:${section}`, async () => {
    const seats = await seatMap(reader, eventId, section);
    // Record sold-out sections so the waiting room can tell queued users the
    // event is gone. TTL stays below the hold expiry, because expired holds free seats again.
    if (seats.length && !seats.some((s) => s.status === 'free')) {
      await redis.multi().sadd(`soldout:${eventId}`, section).pexpire(`soldout:${eventId}`, 60_000).exec();
    }
    return seats;
  });
  return reply.type('application/json').send(json);
});

const holdSeconds = Number(process.env.HOLD_SECONDS ?? 300);
const maxDbQueue = Number(process.env.MAX_DB_QUEUE ?? Infinity);

// Hold a seat while the user pays. One atomic UPDATE, same idea as stage 0:
// the row lock lets exactly one concurrent request win.
// Expired holds are reclaimed lazily right here (status = 'held' AND
// held_until < now()), so no background job is needed to free them.
app.post<{ Body: { eventId: number; seatNo: number; userId: string } }>('/holds', async (req, reply) => {
  const { eventId, seatNo, userId } = req.body;

  if (waitingRoomOn && !hasPass(req.headers['x-queue-pass'] as string, userId)) {
    return reply.code(403).send({ error: 'join the waiting room first' });
  }

  // Load shedding: if this process already has MAX_DB_QUEUE requests waiting
  // for a DB connection, a new one would only wait longer and push everyone's
  // latency up. Reject it now, cheaply, and tell the client when to retry.
  if (pool.waitingCount >= maxDbQueue) {
    return reply.code(503).header('retry-after', '1').send({ error: 'busy, retry shortly' });
  }

  const { rows } = await pool.query(
    `UPDATE seats SET status = 'held', held_by = $2, held_until = now() + make_interval(secs => $3)
     WHERE event_id = $4 AND seat_no = $1 AND (status = 'free' OR (status = 'held' AND held_until < now()))
     RETURNING held_until`,
    [seatNo, userId, holdSeconds, eventId],
  );
  if (!rows[0]) return reply.code(409).send({ error: 'seat unavailable' });

  // WAL position after our commit. Any replica that has replayed this far
  // can see the hold. Clients pass it back as x-min-lsn.
  const { rows: [{ lsn }] } = await pool.query('SELECT pg_current_wal_lsn()::text AS lsn');
  return reply.code(201).send({ eventId, seatNo, heldUntil: rows[0].held_until, lsn });
});

// Pay for a held seat. Returns 202 at once; the payment worker charges the
// provider in the background, and GET /payments/:key reports the outcome.
// Idempotency-Key is required: a client that times out and retries gets the
// same payment back instead of starting a second one.
app.post<{ Body: { eventId: number; seatNo: number; userId: string } }>('/payments', async (req, reply) => {
  const key = req.headers['idempotency-key'] as string | undefined;
  if (!key) return reply.code(400).send({ error: 'Idempotency-Key header required' });
  const { eventId, seatNo, userId } = req.body;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // The primary key serializes duplicates: a concurrent retry blocks here
    // until the first commits, then conflicts and returns the existing payment.
    const inserted = await client.query(
      'INSERT INTO payments (idempotency_key, event_id, seat_no, user_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING',
      [key, eventId, seatNo, userId],
    );
    if (!inserted.rowCount) {
      await client.query('ROLLBACK');
      // Reuse `client`. Calling pool.query here while holding a client deadlocks
      // once every connection in the pool is held by a request doing the same.
      const { rows } = await client.query('SELECT status FROM payments WHERE idempotency_key = $1', [key]);
      return reply.code(200).send({ status: rows[0].status });
    }
    // Only the user holding an unexpired hold can pay for the seat.
    const claimed = await client.query(
      `UPDATE seats SET status = 'paying'
       WHERE event_id = $3 AND seat_no = $1 AND status = 'held' AND held_by = $2 AND held_until > now()`,
      [seatNo, userId, eventId],
    );
    if (!claimed.rowCount) {
      await client.query('ROLLBACK');
      return reply.code(409).send({ error: 'no active hold on this seat' });
    }
    // Outbox: the Kafka message commits atomically with the payment. Either
    // both exist or neither does; the relay publishes it (src/outbox-relay.ts).
    await client.query(
      'INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)',
      [PAYMENT_REQUESTS, `${eventId}:${seatNo}`, { key, eventId, seatNo, userId }],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
  return reply.code(202).send({ status: 'pending' });
});

app.get<{ Params: { key: string } }>('/payments/:key', async (req, reply) => {
  const { rows } = await pool.query('SELECT status FROM payments WHERE idempotency_key = $1', [req.params.key]);
  return rows[0] ?? reply.code(404).send({ error: 'unknown payment' });
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
