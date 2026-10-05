// Resets the DB and creates EVENTS events, each with ROWS x COLS seats.
// Usage: node scripts/seed.ts [events] [rows] [cols]
//   default: 1 event x 40 x 25 = 1000 seats
//   sweeps:  1000 events x 40 x 25 = 1M seats
import { readFileSync } from 'node:fs';
import { pool } from '../src/db.ts';

const [events = 1, rows = 40, cols = 25] = process.argv.slice(2).map(Number);

await pool.query('DROP TABLE IF EXISTS bookings, seats, events');
await pool.query(readFileSync(new URL('../db/schema.sql', import.meta.url), 'utf8'));

// generate_series builds everything in two round trips.
await pool.query(`INSERT INTO events (name) SELECT 'Event ' || n FROM generate_series(1, $1) n`, [events]);
await pool.query(
  `INSERT INTO seats (event_id, label)
   SELECT e.id, r || '-' || c FROM events e, generate_series(1, $1) r, generate_series(1, $2) c`,
  [rows, cols],
);

console.log(`seeded ${events} event(s) with ${rows * cols} seats each`);
await pool.end();
