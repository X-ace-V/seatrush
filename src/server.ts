import Fastify, { type FastifyRequest, type FastifyReply } from 'fastify';
import { shards, route, routeSection, startPartitionMapRefresh, isTransient, REGION } from './shards.ts';
import { cached, redis } from './cache.ts';
import { waitingRoom, waitingRoomOn, hasPass } from './waiting-room.ts';
import { PAYMENT_REQUESTS } from './kafka.ts';
import { instrument, watchPools } from './metrics.ts';

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } });

instrument(app);
watchPools(Object.fromEntries(shards.flatMap((s, i) => [[`shard${i}`, s.primary], ...(s.replica ? [[`shard${i}-replica`, s.replica]] : [])])));
await startPartitionMapRefresh();
app.get('/health', async () => ({ ok: true }));

// Transient failures (a partition moving, a restarting shard; see isTransient)
// get 503 + Retry-After, not 500: the router's map catches up within a second.
app.setErrorHandler((err: Error & { code?: string }, _req, reply) => {
  if (isTransient(err.code)) return reply.code(503).header('retry-after', '1').send({ error: 'temporarily unavailable, retry' });
  reply.log.error(err);
  return reply.code(500).send({ error: 'internal error' });
});
waitingRoom(app);

type Shard = ReturnType<typeof route>;

// Seat maps read from the shard's replica (if it has one) so browse traffic
// cannot slow down bookings on the primary. READS_FROM=primary switches back.
const reader = (s: Shard) => (process.env.READS_FROM === 'primary' ? undefined : s.replica) ?? s.primary;

// Read-your-writes: a client that just booked sends back the `lsn` it got
// (x-min-lsn). If the replica has not replayed up to that point yet, read
// from the primary instead so the user always sees their own booking.
// This must be a separate query BEFORE the read: checking inside the same
// query would race, because the read's snapshot is taken first.
// The LSN belongs to the shard that took the hold, which is the same shard
// that serves this seat's section.
async function freshReader(s: Shard, minLsn: string) {
  const r = reader(s);
  if (r === s.primary) return r;
  const { rows } = await r.query('SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn AS fresh', [minLsn]);
  return rows[0].fresh ? r : s.primary;
}

// Seat maps are served one 1000-seat section at a time, like real ticketing
// sites: a 50K-seat stadium map would be megabytes of JSON.
// An expired hold is reported as free: it is free, nobody has reclaimed it yet.
const SECTION_SIZE = 1000;
const seatMap = (db: Shard['primary'], eventId: number, section: number) =>
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
  const shard = routeSection(eventId, section);
  const minLsn = req.headers['x-min-lsn'] as string | undefined;
  if (minLsn) return seatMap(await freshReader(shard, minLsn), eventId, section);

  const json = await cached(`seats:${eventId}:${section}`, async () => {
    const seats = await seatMap(reader(shard), eventId, section);
    // Record sold-out sections so the waiting room can tell queued users the
    // event is gone. TTL stays below the hold expiry, because expired holds free seats again.
    if (seats.length && !seats.some((s) => s.status === 'free')) {
      await redis.multi().sadd(`soldout:${eventId}`, section).pexpire(`soldout:${eventId}`, 60_000).exec();
    }
    return seats;
  });
  return reply.type('application/json').send(json);
});

// Writes for an event homed in another region are forwarded to that region's
// API as ONE request: one cross-region round trip. The alternative, running
// our transaction against the remote database, costs a round trip per
// statement and holds row locks across the ocean. FORWARD_WRITES=off
// switches to that, for comparison. A forwarded request is never forwarded
// again, so two regions with briefly different partition maps cannot loop;
// the fence on the shard still protects the data.
const regionApis: Record<string, string> = JSON.parse(process.env.REGION_APIS ?? '{}');
const forwardWrites = process.env.FORWARD_WRITES !== 'off';

// Circuit breaker per remote region. Without one, a cut link left every
// forwarded request hanging: thousands piled up, ate the region's CPU and
// memory, and even local requests saw 20s stalls. Now a forward gives up
// after 2s, and after any failure the region is treated as down for 5s:
// requests for its events get an instant 503 instead of each waiting 2s.
// Then one request is let through to probe whether the link is back.
const FORWARD_TIMEOUT_MS = 2000, OPEN_MS = 5000;
// openUntil 0 = closed (healthy). While open, fail fast. Once it expires,
// exactly one request (the probe) goes through; success closes it.
const breakers: Record<string, { openUntil: number; probing: boolean }> = {};

async function forwarded(req: FastifyRequest, reply: FastifyReply, eventId: number, seatNo: number) {
  const home = route(eventId, seatNo).region;
  if (!forwardWrites || home === REGION || req.headers['x-forwarded-region']) return false;
  const breaker = (breakers[home] ??= { openUntil: 0, probing: false });
  if (breaker.openUntil && (Date.now() < breaker.openUntil || breaker.probing)) {
    reply.code(503).header('retry-after', '5').send({ error: `region ${home} unreachable, retry` });
    return true;
  }
  if (breaker.openUntil) breaker.probing = true;
  const headers: Record<string, string> = { 'content-type': 'application/json', 'x-forwarded-region': REGION };
  for (const h of ['idempotency-key', 'x-queue-pass']) if (req.headers[h]) headers[h] = req.headers[h] as string;
  let res: Response, body: string;
  try {
    res = await fetch(regionApis[home] + req.url, {
      method: req.method, headers, body: req.method === 'POST' ? JSON.stringify(req.body) : undefined,
      signal: AbortSignal.timeout(FORWARD_TIMEOUT_MS),
    });
    body = await res.text(); // a stalled body must trip the breaker too
    Object.assign(breaker, { openUntil: 0, probing: false });
  } catch {
    Object.assign(breaker, { openUntil: Date.now() + OPEN_MS, probing: false });
    reply.code(503).header('retry-after', '5').send({ error: `region ${home} unreachable, retry` });
    return true;
  }
  reply.code(res.status).header('x-home-region', home).type('application/json');
  if (res.headers.get('retry-after')) reply.header('retry-after', res.headers.get('retry-after')!);
  reply.send(body);
  return true;
}

const holdSeconds = Number(process.env.HOLD_SECONDS ?? 300);
const maxDbQueue = Number(process.env.MAX_DB_QUEUE ?? Infinity);

// Hold a seat while the user pays. One atomic UPDATE, same idea as stage 0:
// the row lock lets exactly one concurrent request win.
// Expired holds are reclaimed lazily right here (status = 'held' AND
// held_until < now()), so no background job is needed to free them.
app.post<{ Body: { eventId: number; seatNo: number; userId: string } }>('/holds', async (req, reply) => {
  const { eventId, seatNo, userId } = req.body;
  if (await forwarded(req, reply, eventId, seatNo)) return reply;
  const { primary } = route(eventId, seatNo);

  if (waitingRoomOn && !hasPass(req.headers['x-queue-pass'] as string, userId)) {
    return reply.code(403).send({ error: 'join the waiting room first' });
  }

  // Load shedding: if this process already has MAX_DB_QUEUE requests waiting
  // for a DB connection, a new one would only wait longer and push everyone's
  // latency up. Reject it now, cheaply, and tell the client when to retry.
  if (primary.waitingCount >= maxDbQueue) {
    return reply.code(503).header('retry-after', '1').send({ error: 'busy, retry shortly' });
  }

  const { rows } = await primary.query(
    `UPDATE seats SET status = 'held', held_by = $2, held_until = now() + make_interval(secs => $3)
     WHERE event_id = $4 AND seat_no = $1 AND (status = 'free' OR (status = 'held' AND held_until < now()))
     RETURNING held_until`,
    [seatNo, userId, holdSeconds, eventId],
  );
  if (!rows[0]) return reply.code(409).send({ error: 'seat unavailable' });

  // WAL position after our commit. Any replica that has replayed this far
  // can see the hold. Clients pass it back as x-min-lsn.
  const { rows: [{ lsn }] } = await primary.query('SELECT pg_current_wal_lsn()::text AS lsn');
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
  if (await forwarded(req, reply, eventId, seatNo)) return reply;
  const { primary, part } = route(eventId, seatNo);

  const client = await primary.connect();
  try {
    await client.query('BEGIN');
    // The primary key serializes duplicates: a concurrent retry blocks here
    // until the first commits, then conflicts and returns the existing payment.
    const inserted = await client.query(
      'INSERT INTO payments (idempotency_key, event_id, seat_no, part, user_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING',
      [key, eventId, seatNo, part, userId],
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

// Payments live on the seat's shard, so the status lookup needs the seat too.
app.get<{ Params: { key: string }; Querystring: { eventId: string; seatNo: string } }>('/payments/:key', async (req, reply) => {
  if (await forwarded(req, reply, Number(req.query.eventId), Number(req.query.seatNo))) return reply;
  const { primary } = route(Number(req.query.eventId), Number(req.query.seatNo));
  const { rows } = await primary.query('SELECT status FROM payments WHERE idempotency_key = $1', [req.params.key]);
  return rows[0] ?? reply.code(404).send({ error: 'unknown payment' });
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
