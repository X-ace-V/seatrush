// Invariant checker, across every shard. Exits 1 on any violation so a chaos
// run can fail on it.
//  - seats: every booked seat has exactly one booking; no other seat has any
//  - payments: no provider charge without a payment, every capture has its
//    booking, nothing stuck pending (run after the system has drained)
//  - placement: every row sits on the shard that owns its partition
import { shards, catalog, endAll } from '../src/shards.ts';

const { rows: parts } = await catalog.query('SELECT id, shard FROM partitions');
const perShard = await Promise.all(shards.map(async (s, i) => {
  const owned = parts.filter((p) => p.shard === i).map((p) => p.id);
  const { rows: [r] } = await s.primary.query(`
    SELECT
      (SELECT count(*) FROM (SELECT 1 FROM bookings GROUP BY event_id, seat_no HAVING count(*) > 1) d)::int AS double_booked,
      (SELECT count(*) FROM seats s WHERE status = 'booked'
         AND NOT EXISTS (SELECT 1 FROM bookings b WHERE (b.event_id, b.seat_no) = (s.event_id, s.seat_no)))::int AS booked_without_booking,
      (SELECT count(*) FROM seats s WHERE status <> 'booked'
         AND EXISTS (SELECT 1 FROM bookings b WHERE (b.event_id, b.seat_no) = (s.event_id, s.seat_no)))::int AS unbooked_with_booking,
      (SELECT count(*) FROM payments p WHERE status = 'captured'
         AND NOT EXISTS (SELECT 1 FROM bookings b WHERE (b.event_id, b.seat_no) = (p.event_id, p.seat_no) AND b.user_id = p.user_id))::int AS captured_without_booking,
      (SELECT count(*) FROM payments WHERE status = 'pending')::int AS stuck_pending,
      ((SELECT count(*) FROM seats WHERE part <> ALL($1::int[])) + (SELECT count(*) FROM bookings WHERE part <> ALL($1::int[]))
         + (SELECT count(*) FROM payments WHERE part <> ALL($1::int[])))::int AS misplaced_rows,
      (SELECT count(*) FROM payments WHERE status = 'captured')::int AS captured,
      (SELECT count(*) FROM bookings)::int AS total_bookings,
      (SELECT count(*) FROM seats)::int AS seats`, [owned]);
  const { rows: keys } = await s.primary.query('SELECT idempotency_key FROM payments');
  return { ...r, keys: keys.map((k) => k.idempotency_key) };
}));

const paymentKeys = new Set(perShard.flatMap((r) => r.keys));
const { rows: charges } = await catalog.query('SELECT idempotency_key FROM provider_charges');
const total: Record<string, number> = { charge_without_payment: charges.filter((c) => !paymentKeys.has(c.idempotency_key)).length };
for (const r of perShard) for (const [k, v] of Object.entries(r)) if (k !== 'keys') total[k] = (total[k] ?? 0) + (v as number);
console.table(total);
console.log('seats per shard:', perShard.map((r) => r.seats).join(' / '));
await endAll();

const bad = ['double_booked', 'booked_without_booking', 'unbooked_with_booking', 'charge_without_payment',
  'captured_without_booking', 'stuck_pending', 'misplaced_rows'].filter((k) => total[k] !== 0);
console.log(bad.length ? `INVARIANT VIOLATED: ${bad.join(', ')}` : 'INVARIANT OK');
process.exit(bad.length ? 1 : 0);
