// Change data capture. Streams committed changes to `bookings` from every
// shard's WAL (logical decoding, pgoutput) and keeps a read model in each
// region's Redis: sold:<eventId> = the set of sold seat numbers. The write
// path is untouched: no dual writes, nothing extra in the transaction.
//
// At least once: a WAL position is acknowledged only after every Redis has
// the change, so a crash replays changes instead of losing them. Set adds and
// removes are idempotent, so replays are harmless.
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
      service.on('data', async (lsn: string, msg: any) => {
        if (msg.relation?.name === 'bookings' && (msg.tag === 'insert' || msg.tag === 'delete')) {
          const row = msg.tag === 'insert' ? msg.new : msg.key;
          const key = `sold:${row.event_id}`;
          await Promise.all(sinks.map((r) => (msg.tag === 'insert' ? r.sadd(key, row.seat_no) : r.srem(key, row.seat_no))));
          applied.inc({ shard, op: msg.tag });
        }
        if (msg.tag === 'commit') await service.acknowledge(lsn);
      });
      // Also confirm keepalive positions. A slot pins ALL WAL, not just the
      // published table, and pgoutput skips transactions that do not touch
      // bookings, so under hold-only traffic no commit ever arrived to ack:
      // the slot held 25MB and kept growing with a healthy consumer. Flow
      // control means every earlier change is already applied when this runs.
      service.on('heartbeat', async (lsn: string, _ts: number, shouldRespond: boolean) => {
        if (shouldRespond) await service.acknowledge(lsn);
      });
      await new Promise((_, reject) => {
        service.on('error', reject);
        service.subscribe(new PgoutputPlugin({ protoVersion: 1, publicationNames: [SLOT] }), SLOT).catch(reject);
      });
    } catch (err) {
      console.error(`cdc shard${shard}: ${(err as Error).message}, reconnecting`);
      await sleep(2000);
    }
  }
}

serveMetrics();
await Promise.all(sources.map(follow));
