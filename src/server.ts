import Fastify from 'fastify';
import { pool, replica } from './db.ts';

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } });

app.get('/health', async () => ({ ok: true }));

// Seat maps read from the replica so browse traffic cannot slow down bookings
// on the primary. READS_FROM=primary switches back, for comparison runs.
const reader = process.env.READS_FROM === 'primary' ? pool : replica;

// Read-your-writes: a client that just booked sends back the `lsn` it got
// (x-min-lsn). If the replica has not replayed up to that point yet, read
// from the primary instead so the user always sees their own booking.
// This must be a separate query BEFORE the read: checking inside the same
// query would race, because the read's snapshot is taken first.
async function freshReader(minLsn?: string) {
  if (!minLsn || reader === pool) return reader;
  const { rows } = await replica.query('SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn AS fresh', [minLsn]);
  return rows[0].fresh ? replica : pool;
}

app.get<{ Params: { id: string } }>('/events/:id/seats', async (req) => {
  const db = await freshReader(req.headers['x-min-lsn'] as string | undefined);
  const { rows } = await db.query(
    'SELECT id, label, status FROM seats WHERE event_id = $1 ORDER BY id',
    [req.params.id],
  );
  return rows;
});

// Claim and record the seat in ONE atomic statement.
// The UPDATE takes a row lock. A concurrent request for the same seat waits
// on that lock, then re-checks status = 'free', finds it false, and matches
// zero rows. So exactly one request wins and the rest get 409.
app.post<{ Body: { seatId: number; userId: string } }>('/bookings', async (req, reply) => {
  const { seatId, userId } = req.body;

  const { rows } = await pool.query(
    `WITH claimed AS (
       UPDATE seats SET status = 'booked' WHERE id = $1 AND status = 'free' RETURNING id
     )
     INSERT INTO bookings (seat_id, user_id) SELECT id, $2 FROM claimed RETURNING id`,
    [seatId, userId],
  );

  if (!rows[0]) return reply.code(409).send({ error: 'seat unavailable' });

  // WAL position after our commit. Any replica that has replayed this far
  // can see the booking. Clients pass it back as x-min-lsn.
  const { rows: [{ lsn }] } = await pool.query('SELECT pg_current_wal_lsn()::text AS lsn');
  return reply.code(201).send({ bookingId: rows[0].id, lsn });
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
