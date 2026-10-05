// Partitioning. Every seat belongs to a logical partition (0..PARTITIONS-1),
// and each partition lives on one physical shard (a Postgres primary, with an
// optional read replica). The partition -> shard map lives in the catalog
// (shard 0) so partitions can move between shards without rehashing.
import { createHash } from 'node:crypto';
import pg from 'pg';

export const PARTITIONS = 12;
export const SECTION_SIZE = 1000;

// PARTITION_BY=event puts a whole event in one partition. The default keys by
// (event, section), which spreads one huge event across many partitions.
const byEvent = process.env.PARTITION_BY === 'event';

type Shard = { primary: pg.Pool; replica?: pg.Pool };
const local = 'postgres://seatrush:seatrush@localhost';
const config: { primary: string; replica?: string }[] = JSON.parse(process.env.SHARDS ?? JSON.stringify([
  { primary: `${local}:5432/seatrush`, replica: `${local}:5433/seatrush` },
  { primary: `${local}:5441/seatrush` },
  { primary: `${local}:5442/seatrush` },
]));
const max = Number(process.env.PG_POOL_SIZE ?? 10);
const newPool = (connectionString: string) => new pg.Pool({ connectionString, max });

export const shards: Shard[] = config.map((c) => ({ primary: newPool(c.primary), replica: c.replica ? newPool(c.replica) : undefined }));
export const catalog = shards[0].primary;

export const sectionOf = (seatNo: number) => Math.floor((seatNo - 1) / SECTION_SIZE);

// Hash, not ranges: consecutive event ids (and the sections of one event)
// land on unrelated partitions, so new events do not all pile onto one shard.
export function partitionOf(eventId: number, section: number) {
  const key = byEvent ? `${eventId}` : `${eventId}:${section}`;
  return createHash('md5').update(key).digest().readUInt32BE(0) % PARTITIONS;
}

// partition -> shard index, loaded from the catalog and refreshed every second.
let map: number[] = [];
export async function loadPartitionMap() {
  const { rows } = await catalog.query('SELECT id, shard FROM partitions');
  const next: number[] = [];
  for (const r of rows) next[r.id] = r.shard;
  map = next;
}
// Does not block startup: on a fresh stack the catalog only exists after the
// first seed, and crashing until then helps nobody.
export function startPartitionMapRefresh() {
  const refresh = () => loadPartitionMap().catch(() => {});
  setInterval(refresh, 1000).unref();
  return refresh();
}

export function routeSection(eventId: number, section: number) {
  const part = partitionOf(eventId, section);
  return { part, shard: map[part], ...shards[map[part]] };
}
export const route = (eventId: number, seatNo: number) => routeSection(eventId, sectionOf(seatNo));

export const endAll = () => Promise.all(shards.flatMap((s) => [s.primary.end(), s.replica?.end()]));
