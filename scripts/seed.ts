// Resets the DB and creates EVENTS events with SEATS seats each. Event 1 can
// be made bigger (a stadium) with HOT_SEATS. Seats come in 1000-seat sections.
// Usage: node scripts/seed.ts [events] [seats] [hotSeats]
//   default: 1 event x 1000 seats
//   sweeps:  1000 events x 1000 seats = 1M
import { readFileSync } from 'node:fs';
import { pool } from '../src/db.ts';

const [events = 1, seats = 1000, hotSeats = seats] = process.argv.slice(2).map(Number);

await pool.query('DROP TABLE IF EXISTS outbox, provider_charges, payments, bookings, seats, events');
await pool.query(readFileSync(new URL('../db/schema.sql', import.meta.url), 'utf8'));

// generate_series builds everything in two round trips.
await pool.query(
  `INSERT INTO events (name, seat_count)
   SELECT 'Event ' || n, CASE WHEN n = 1 THEN $2::int ELSE $3::int END FROM generate_series(1, $1) n`,
  [events, hotSeats, seats],
);
await pool.query(
  `INSERT INTO seats (event_id, seat_no, label)
   SELECT e.id, n, 'S' || (n - 1) / 1000 || '-' || (n - 1) % 1000 + 1
   FROM events e, generate_series(1, e.seat_count) n`,
);

console.log(`seeded ${events} event(s), ${seats} seats each, event 1 has ${hotSeats}`);
await pool.end();
