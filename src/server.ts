import Fastify from 'fastify';
import { pool, replica } from './db.ts';
import { cached } from './cache.ts';
import { waitingRoom, waitingRoomOn, hasPass } from './waiting-room.ts';

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } });

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

// An expired hold is reported as free: it is free, nobody has reclaimed it yet.
const seatMap = (db: typeof pool, eventId: string) =>
  db.query(
    `SELECT id, label,
            CASE WHEN status = 'held' AND held_until < now() THEN 'free' ELSE status END AS status
     FROM seats WHERE event_id = $1 ORDER BY id`,
    [eventId],
  ).then((r) => r.rows);

// Browsers get a seat map up to CACHE_TTL_MS stale. That is safe because the
// booking itself is checked atomically on the primary. We do not invalidate
// on each booking: on a hot event that would empty the cache constantly.
// A user holding an x-min-lsn token skips the cache to see their own booking.
app.get<{ Params: { id: string } }>('/events/:id/seats', async (req, reply) => {
  const minLsn = req.headers['x-min-lsn'] as string | undefined;
  if (minLsn) return seatMap(await freshReader(minLsn), req.params.id);

  const json = await cached(`seats:${req.params.id}`, () => seatMap(reader, req.params.id));
  return reply.type('application/json').send(json);
});

const holdSeconds = Number(process.env.HOLD_SECONDS ?? 300);
const maxDbQueue = Number(process.env.MAX_DB_QUEUE ?? Infinity);

// Hold a seat while the user pays. One atomic UPDATE, same idea as stage 0:
// the row lock lets exactly one concurrent request win.
// Expired holds are reclaimed lazily right here (status = 'held' AND
// held_until < now()), so no background job is needed to free them.
app.post<{ Body: { seatId: number; userId: string } }>('/holds', async (req, reply) => {
  const { seatId, userId } = req.body;

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
     WHERE id = $1 AND (status = 'free' OR (status = 'held' AND held_until < now()))
     RETURNING held_until`,
    [seatId, userId, holdSeconds],
  );
  if (!rows[0]) return reply.code(409).send({ error: 'seat unavailable' });

  // WAL position after our commit. Any replica that has replayed this far
  // can see the hold. Clients pass it back as x-min-lsn.
  const { rows: [{ lsn }] } = await pool.query('SELECT pg_current_wal_lsn()::text AS lsn');
  return reply.code(201).send({ seatId, heldUntil: rows[0].held_until, lsn });
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
