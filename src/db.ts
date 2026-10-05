import pg from 'pg';

// One shared pool per process. Every app instance opens up to `max`
// connections, so total DB connections = instances x PG_POOL_SIZE.
export const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://seatrush:seatrush@localhost:5432/seatrush',
  max: Number(process.env.PG_POOL_SIZE ?? 10),
});
