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

// NAIVE: check-then-act with no lock. Two requests can both see "free"
// before either one writes, and both get a booking.
app.post<{ Body: { seatId: number; userId: string } }>('/bookings', async (req, reply) => {
  const { seatId, userId } = req.body;

  const { rows } = await pool.query('SELECT status FROM seats WHERE id = $1', [seatId]);
  if (!rows[0]) return reply.code(404).send({ error: 'seat not found' });
  if (rows[0].status !== 'free') return reply.code(409).send({ error: 'seat taken' });

  const booking = await pool.query(
    'INSERT INTO bookings (seat_id, user_id) VALUES ($1, $2) RETURNING id',
    [seatId, userId],
  );
  await pool.query(`UPDATE seats SET status = 'booked' WHERE id = $1`, [seatId]);

  return reply.code(201).send({ bookingId: booking.rows[0].id });
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
