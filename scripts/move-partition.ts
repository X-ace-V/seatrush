// Moves one logical partition to another shard while traffic keeps flowing.
// Only that partition is unavailable, and only while its rows are copied.
//   1. fence:  the source stops accepting writes for the partition (waits for
//              in-flight writes to commit first, via the advisory lock)
//   2. copy:   seats, bookings and payments go to the target, which then owns it
//   3. flip:   the catalog points at the target; routers refresh within 1s, and
//              stale ones hit the fence on the source (503, client retries)
//   4. clean:  delete the old copies from the source
// Outbox rows stay where they are: the relay publishes them from any shard.
// The copy and the cleanup run under the replication origin 'rebalance', so
// CDC can tell them apart from real bookings (src/cdc.ts skips them).
//
// Every step is idempotent, so a move that died halfway (leaving the
// partition owned by no shard, writes failing with 503) is finished by
// simply running the same command again.
// Usage: node scripts/move-partition.ts <partition> <toShard>
import type pg from 'pg';
import { shards, catalog, endAll } from '../src/shards.ts';

const [part, to] = process.argv.slice(2).map(Number);
const { rows: [{ shard: from }] } = await catalog.query('SELECT shard FROM partitions WHERE id = $1', [part]);
if (from === to) { console.log(`partition ${part} is already on shard ${to}`); await endAll(); process.exit(0); }
const [src, dst] = [shards[from].primary, shards[to].primary];
const ms = (t: number) => `${Math.round(performance.now() - t)}ms`;
const t0 = performance.now();

// Runs `work` in a transaction tagged with the 'rebalance' replication origin.
// Logical decoding reports that origin with each change, which is how CDC
// tells a partition move from real business activity. Session-level, so it
// is reset before the connection goes back to the pool.
async function asRebalance(pool: typeof src, work: (c: pg.PoolClient) => Promise<void>) {
  const c = await pool.connect();
  try {
    await c.query(`SELECT pg_replication_origin_create('rebalance')
                   WHERE NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = 'rebalance')`);
    await c.query("SELECT pg_replication_origin_session_setup('rebalance')");
    await c.query('BEGIN');
    await work(c);
    await c.query('COMMIT');
  } finally {
    await c.query("SELECT pg_replication_origin_session_reset()").catch(() => {});
    c.release();
  }
}

// 1. Fence the source.
const s = await src.connect();
await s.query('BEGIN');
await s.query('SELECT pg_advisory_xact_lock(4242, $1)', [part]);
await s.query('DELETE FROM owned_partitions WHERE part = $1', [part]);
await s.query('COMMIT');
s.release();
const fenced = performance.now();

// 2. Copy. json_agg / json_populate_recordset moves whole rows in one round
// trip per table. bookings.id is left out so the target's sequence assigns it.
// The target claims ownership FIRST, inside the same transaction: its own
// fence would otherwise reject the copied bookings and payments, and a failed
// copy rolls the claim back with it.
const tables = { seats: '*', bookings: 'event_id, seat_no, part, user_id, created_at', payments: '*' };
const counts: Record<string, number> = {};
await asRebalance(dst, async (d) => {
  await d.query('INSERT INTO owned_partitions VALUES ($1) ON CONFLICT DO NOTHING', [part]);
  for (const [table, cols] of Object.entries(tables)) {
    const { rows: [{ data }] } = await src.query(`SELECT coalesce(json_agg(t), '[]') AS data FROM ${table} t WHERE part = $1`, [part]);
    const into = cols === '*' ? table : `${table} (${cols})`;
    await d.query(`INSERT INTO ${into} SELECT ${cols} FROM json_populate_recordset(null::${table}, $1) ON CONFLICT DO NOTHING`, [JSON.stringify(data)]);
    counts[table] = data.length;
  }
});

// 3. Flip the catalog. Writes for this partition were blocked from fence to here.
await catalog.query('UPDATE partitions SET shard = $2 WHERE id = $1', [part, to]);
const unavailable = ms(fenced);

// 4. Clean up the source.
await asRebalance(src, async (c) => {
  for (const table of Object.keys(tables)) await c.query(`DELETE FROM ${table} WHERE part = $1`, [part]);
});

console.log(`moved partition ${part}: shard ${from} -> ${to}, ` +
  `${Object.entries(counts).map(([t, n]) => `${n} ${t}`).join(', ')}; writes blocked ${unavailable}, total ${ms(t0)}`);
await endAll();
