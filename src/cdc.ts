// Change data capture. Streams committed changes to `bookings` from every
// shard's WAL (logical decoding, pgoutput) and keeps a read model in each
// region's Redis: sold:<eventId> = the set of sold seat numbers. The write
// path is untouched: no dual writes, nothing extra in the transaction.
//
// At least once: a WAL position is acknowledged only after every Redis has
// the changes up to it, so a crash replays changes instead of losing them.
// Set adds and removes are idempotent, so replays are harmless.
import pg from 'pg';
import { Redis } from 'ioredis';
import { LogicalReplicationService, PgoutputPlugin } from 'pg-logical-replication';
import { prom, serveMetrics } from './metrics.ts';

const SLOT = 'seatrush_cdc';
// Direct connections: PgBouncer in transaction mode cannot carry a replication stream.
const sources: string[] = JSON.parse(process.env.CDC_SOURCES ?? JSON.stringify(
  [5432, 5441, 5442].map((port) => `postgres://seatrush:seatrush@localhost:${port}/seatrush`)));
const sinks = (process.env.CDC_SINKS ?? 'redis://localhost:6379').split(',').map((url) => new Redis(url));
const applied = new prom.Counter({ name: 'cdc_changes_total', help: 'Booking changes applied to the read model', labelNames: ['shard', 'op'] });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function follow(url: string, shard: number) {
  for (;;) {
    try {
      // The slot remembers our position on the shard across restarts.
      const admin = new pg.Client(url);
      await admin.connect();
      await admin.query(`SELECT pg_create_logical_replication_slot($1, 'pgoutput')
                         WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, [SLOT]);
      await admin.end();

      const service = new LogicalReplicationService(
        { connectionString: url },
        { acknowledge: { auto: false, timeoutSeconds: 0 }, flowControl: { enabled: true } }, // handle one change at a time
      );
      // Changes are buffered and written as one Redis pipeline per region
      // every 50ms (or at 1000 pending): one round trip per batch. Applying
      // them one at a time cost a cross-region round trip each, about 12
      // changes/s: a 3000-booking burst took 246s to reach the read model.
      type Op = { add: boolean; key: string; seat: number };
      let pending: Op[] = [], commitLsn: string | null = null, flushing = Promise.resolve();
      const flush = () => (flushing = flushing.then(async () => {
        if (!pending.length && !commitLsn) return;
        const ops = pending, lsn = commitLsn;
        pending = []; commitLsn = null;
        await Promise.all(sinks.map((r) => {
          const p = r.pipeline();
          for (const o of ops) (o.add ? p.sadd(o.key, o.seat) : p.srem(o.key, o.seat));
          return p.exec();
        }));
        // Ack the last commit in this batch only once every region has it.
        if (lsn) await service.acknowledge(lsn);
        for (const o of ops) applied.inc({ shard, op: o.add ? 'insert' : 'delete' });
      }));
      const timer = setInterval(() => flush().catch(() => {}), 50);

      // CDC sees physical row changes, not business events. A partition move
      // inserts copies on one shard and deletes the originals on another; read
      // literally, the deletes un-sold every seat in the moved partition (an
      // event with 268 seats sold showed 0). Moves run under the replication
      // origin 'rebalance', which pgoutput reports at the start of each such
      // transaction, so those changes are skipped.
      let fromRebalance = false;
      service.on('data', async (lsn: string, msg: any) => {
        if (msg.tag === 'begin') fromRebalance = false;
        if (msg.tag === 'origin') fromRebalance = msg.originName === 'rebalance';
        if (fromRebalance && (msg.tag === 'insert' || msg.tag === 'delete')) applied.inc({ shard, op: 'rebalance_skipped' });
        else if (msg.relation?.name === 'bookings' && (msg.tag === 'insert' || msg.tag === 'delete')) {
          const row = msg.tag === 'insert' ? msg.new : msg.key;
          pending.push({ add: msg.tag === 'insert', key: `sold:${row.event_id}`, seat: row.seat_no });
        }
        if (msg.tag === 'commit') commitLsn = lsn;
        if (pending.length >= 1000) await flush(); // backpressure: stop reading until Redis catches up
      });
      // Also confirm keepalive positions. A slot pins ALL WAL, not just the
      // published table, and pgoutput skips transactions that do not touch
      // bookings, so under hold-only traffic no commit ever arrived to ack:
      // the slot held 25MB and kept growing with a healthy consumer. Only when
      // nothing is buffered, so we never confirm a change Redis does not have.
      service.on('heartbeat', async (lsn: string, _ts: number, shouldRespond: boolean) => {
        if (!shouldRespond) return;
        await flush();
        if (!pending.length) await service.acknowledge(lsn);
      });
      await new Promise((_, reject) => {
        service.on('error', reject);
        service.subscribe(new PgoutputPlugin({ protoVersion: 1, publicationNames: [SLOT] }), SLOT).catch(reject);
      }).finally(() => clearInterval(timer));
    } catch (err) {
      console.error(`cdc shard${shard}: ${(err as Error).message}, reconnecting`);
      await sleep(2000);
    }
  }
}

serveMetrics();
await Promise.all(sources.map(follow));
