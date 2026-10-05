// Resets the DB and creates one event with ROWS x COLS seats.
// Usage: node scripts/seed.ts [rows] [cols]   (default 40 x 25 = 1000 seats)
import { readFileSync } from 'node:fs';
import { pool } from '../src/db.ts';

const rows = Number(process.argv[2] ?? 40);
const cols = Number(process.argv[3] ?? 25);

await pool.query('DROP TABLE IF EXISTS bookings, seats, events');
await pool.query(readFileSync(new URL('../db/schema.sql', import.meta.url), 'utf8'));

const { rows: [event] } = await pool.query(
  `INSERT INTO events (name) VALUES ('Launch Concert') RETURNING id`,
);

// generate_series builds all seats in one round trip.
await pool.query(
  `INSERT INTO seats (event_id, label)
   SELECT $1, r || '-' || c FROM generate_series(1, $2) r, generate_series(1, $3) c`,
  [event.id, rows, cols],
);

console.log(`seeded event ${event.id} with ${rows * cols} seats`);
await pool.end();
