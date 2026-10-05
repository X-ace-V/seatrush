// Virtual waiting room. Users join a per-event FIFO queue and are let in at
// ADMIT_PER_SEC, so the booking path sees a steady rate instead of the spike.
//
// Stateless admission: nothing advances the queue. A position is admitted
// once position <= seconds since the queue opened x ADMIT_PER_SEC.
// Tickets and passes are HMAC-signed, so any app replica can verify them
// without a lookup, and users cannot forge a better position.
import { createHmac, timingSafeEqual } from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { redis } from './cache.ts';
import { client } from './metrics.ts';

const queueEvents = new client.Counter({ name: 'queue_events_total', help: 'Waiting room joins and admissions', labelNames: ['event'] });

export const waitingRoomOn = process.env.WAITING_ROOM === 'on';
const admitPerSec = Number(process.env.ADMIT_PER_SEC ?? 2000);
const passTtlMs = 10 * 60_000;
const secret = process.env.QUEUE_SECRET ?? 'dev-only-secret';

const sign = (s: string) => `${s}.${createHmac('sha256', secret).update(s).digest('base64url')}`;

// Returns the signed payload, or null if the signature is wrong.
function verify(signed = ''): string | null {
  const payload = signed.slice(0, signed.lastIndexOf('.'));
  const [a, b] = [Buffer.from(sign(payload)), Buffer.from(signed)];
  return a.length === b.length && timingSafeEqual(a, b) ? payload : null;
}

// A pass proves this user was admitted. Required by POST /holds when the room is on.
export function hasPass(pass: string | undefined, userId: string) {
  const [user, expires] = verify(pass)?.split(':') ?? [];
  return user === userId && Number(expires) > Date.now();
}

export function waitingRoom(app: FastifyInstance) {
  app.post<{ Params: { eventId: string }; Body: { userId: string } }>('/queue/:eventId', async (req) => {
    const key = `queue:${req.params.eventId}`;
    await redis.set(`${key}:opened`, Date.now(), 'NX'); // first joiner opens the queue
    const position = await redis.incr(`${key}:size`);
    queueEvents.inc({ event: 'joined' });
    return { position, ticket: sign(`${req.params.eventId}:${req.body.userId}:${position}`) };
  });

  app.get<{ Params: { eventId: string } }>('/queue/:eventId/status', async (req, reply) => {
    const [eventId, userId, position] = verify(req.headers['x-queue-ticket'] as string)?.split(':') ?? [];
    if (eventId !== req.params.eventId) return reply.code(401).send({ error: 'bad ticket' });
    if (await redis.exists(`soldout:${eventId}`)) return { admitted: false, soldOut: true };

    const opened = Number(await redis.get(`queue:${eventId}:opened`));
    const ahead = Number(position) - Math.floor(((Date.now() - opened) / 1000) * admitPerSec);
    if (ahead <= 0) {
      queueEvents.inc({ event: 'admitted' }); // counts admitted polls, so a user polling twice counts twice
      return { admitted: true, pass: sign(`${userId}:${Date.now() + passTtlMs}`) };
    }

    // Tell the client when to poll again, so waiting users do not hammer us.
    const retryAfter = Math.min(10, Math.max(1, Math.ceil(ahead / admitPerSec)));
    return reply.header('retry-after', String(retryAfter)).send({ admitted: false, ahead });
  });
}
