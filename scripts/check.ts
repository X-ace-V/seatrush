// Invariant checker: every booked seat has exactly one booking, and no
// seat in any other state (free, held, paying) has one. Payments: nobody is
// charged without a payment record, every capture has its booking, and no
// payment is stuck pending (run after the system has drained). Exits 1 on violation so CI or a chaos run can fail on it.
import { pool } from '../src/db.ts';

const { rows: [r] } = await pool.query(`
  SELECT
    (SELECT count(*) FROM (SELECT 1 FROM bookings GROUP BY event_id, seat_no HAVING count(*) > 1) d)::int AS double_booked,
    (SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) n FROM bookings GROUP BY event_id, seat_no) x)::int AS extra_bookings,
    (SELECT count(*) FROM seats s WHERE status = 'booked'
       AND NOT EXISTS (SELECT 1 FROM bookings b WHERE (b.event_id, b.seat_no) = (s.event_id, s.seat_no)))::int AS booked_without_booking,
    (SELECT count(*) FROM seats s WHERE status <> 'booked'
       AND EXISTS (SELECT 1 FROM bookings b WHERE (b.event_id, b.seat_no) = (s.event_id, s.seat_no)))::int AS unbooked_with_booking,
    (SELECT count(*) FROM provider_charges c
       WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.idempotency_key = c.idempotency_key))::int AS charge_without_payment,
    (SELECT count(*) FROM payments p WHERE status = 'captured'
       AND NOT EXISTS (SELECT 1 FROM bookings b WHERE (b.event_id, b.seat_no) = (p.event_id, p.seat_no) AND b.user_id = p.user_id))::int AS captured_without_booking,
    (SELECT count(*) FROM payments WHERE status = 'pending')::int AS stuck_pending,
    (SELECT count(*) FROM payments WHERE status = 'captured')::int AS captured,
    (SELECT count(*) FROM bookings)::int AS total_bookings
`);

console.table(r);
await pool.end();

const ok = r.double_booked === 0 && r.booked_without_booking === 0 && r.unbooked_with_booking === 0
  && r.charge_without_payment === 0 && r.captured_without_booking === 0 && r.stuck_pending === 0;
console.log(ok ? 'INVARIANT OK' : 'INVARIANT VIOLATED');
process.exit(ok ? 0 : 1);
