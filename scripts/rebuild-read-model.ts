// Rebuilds the CDC read model (sold:<eventId> in every region's Redis) from
// the shards, the source of truth. For a read model that drifted, e.g. from a
// consumer bug. Stop the consumer first (docker compose stop cdc) and start it
// after: it replays from its slot, and replays are idempotent, so changes
// made during the rebuild are applied on top instead of lost.
// Usage: node scripts/rebuild-read-model.ts
import { Redis } from 'ioredis';
import { shards, endAll } from '../src/shards.ts';

const sold = new Map<number, number[]>();
for (const s of shards) {
  const { rows } = await s.primary.query('SELECT event_id, array_agg(seat_no) AS seats FROM bookings GROUP BY event_id');
  for (const r of rows) sold.set(r.event_id, [...(sold.get(r.event_id) ?? []), ...r.seats]);
}
for (const url of ['redis://localhost:6379', 'redis://localhost:6380']) {
  const redis = new Redis(url);
  const stale = await redis.keys('sold:*');
  const tx = redis.multi();
  if (stale.length) tx.del(...stale);
  for (const [event, seats] of sold) tx.sadd(`sold:${event}`, ...seats);
  await tx.exec();
  redis.disconnect();
}
console.log(`rebuilt read model: ${sold.size} events, ${[...sold.values()].reduce((n, s) => n + s.length, 0)} sold seats`);
await endAll();
