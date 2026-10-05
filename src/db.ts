import pg from 'pg';

// One shared pool per process. Default size is 10 connections.
export const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://seatrush:seatrush@localhost:5432/seatrush',
});
