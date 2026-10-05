import Fastify from 'fastify';
import { pool } from './db.ts';

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } });

app.get('/health', async () => ({ ok: true }));

app.get<{ Params: { id: string } }>('/events/:id/seats', async (req) => {
  const { rows } = await pool.query(
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
  return reply.code(201).send({ bookingId: rows[0].id });
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
