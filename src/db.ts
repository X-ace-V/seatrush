import pg from 'pg';

const local = 'postgres://seatrush:seatrush@localhost';

// One pool per database per process. Each app instance opens up to `max`
// connections per pool, so PgBouncer sits in front of both.
// primary: every write, and reads that must be fresh.
// replica: seat map reads. Async replication, so it can lag behind.
export const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL ?? `${local}:5432/seatrush`,
  max: Number(process.env.PG_POOL_SIZE ?? 10),
});

export const replica = new pg.Pool({
  connectionString: process.env.REPLICA_URL ?? `${local}:5433/seatrush`,
  max: Number(process.env.PG_POOL_SIZE ?? 10),
});
