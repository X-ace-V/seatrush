// Resets the catalog and every shard, then creates EVENTS events with SEATS
// seats each (event 1 gets HOT_SEATS, e.g. a stadium). Partition p starts on
// shard p % SHARDS_USED (default: all configured shards).
// Usage: [SHARDS_USED=n] node scripts/seed.ts [events] [seats] [hotSeats]
//   default: 1 event x 1000 seats
//   sweeps:  1000 events x 1000 seats = 1M
import { readFileSync } from 'node:fs';
import { Redis } from 'ioredis';
import { shards, catalog, partitionOf, PARTITIONS, SECTION_SIZE, endAll } from '../src/shards.ts';

const [events = 1, seats = 1000, hotSeats = seats] = process.argv.slice(2).map(Number);
const used = Number(process.env.SHARDS_USED ?? shards.length);
const sql = (file: string) => readFileSync(new URL(`../db/${file}`, import.meta.url), 'utf8');

await catalog.query('DROP TABLE IF EXISTS events, partitions, provider_charges');
await catalog.query(sql('catalog.sql'));
await Promise.all(shards.map(async (s) => {
  await s.primary.query('DROP TABLE IF EXISTS owned_partitions, outbox, payments, bookings, seats');
  await s.primary.query(sql('schema.sql'));
}));

await catalog.query('INSERT INTO partitions SELECT p, p % $2 FROM generate_series(0, $1 - 1) p', [PARTITIONS, used]);
await Promise.all(shards.map((s, i) => s.primary.query(
  'INSERT INTO owned_partitions SELECT p FROM generate_series(0, $1 - 1) p WHERE p % $2 = $3', [PARTITIONS, used, i])));
await catalog.query(
  `INSERT INTO events (name, seat_count)
   SELECT 'Event ' || n, CASE WHEN n = 1 THEN $2::int ELSE $3::int END FROM generate_series(1, $1) n`,
  [events, hotSeats, seats],
);

// Each (event, section) goes to its partition's shard: one INSERT per shard.
const batches = shards.map(() => ({ event: [] as number[], from: [] as number[], to: [] as number[], part: [] as number[] }));
for (let e = 1; e <= events; e++) {
  const count = e === 1 ? hotSeats : seats;
  for (let s = 0; s * SECTION_SIZE < count; s++) {
    const part = partitionOf(e, s), b = batches[part % used];
    b.event.push(e); b.from.push(s * SECTION_SIZE + 1); b.to.push(Math.min((s + 1) * SECTION_SIZE, count)); b.part.push(part);
  }
}
await Promise.all(batches.map((b, i) => b.event.length && shards[i].primary.query(
  `INSERT INTO seats (event_id, seat_no, part, label)
   SELECT u.event, n, u.part, 'S' || (n - 1) / 1000 || '-' || (n - 1) % 1000 + 1
   FROM unnest($1::int[], $2::int[], $3::int[], $4::int[]) AS u(event, f, t, part), generate_series(u.f, u.t) n`,
  [b.event, b.from, b.to, b.part],
)));

// The CDC read model describes the old data: clear it in both regions.
for (const url of ['redis://localhost:6379', 'redis://localhost:6380']) {
  const r = new Redis(url);
  const keys = await r.keys('sold:*');
  if (keys.length) await r.del(...keys);
  r.disconnect();
}

console.log(`seeded ${events} event(s), ${seats} seats each, event 1 has ${hotSeats}; ${PARTITIONS} partitions on ${used} shard(s): ` +
  batches.map((b, i) => `shard${i}=${b.event.length} sections`).join(' '));
await endAll();
