// Read-your-writes check: book a seat, then immediately load the seat map
// and see whether our own booking shows up. Against an async replica it
// often won't. If the booking response carries an `lsn`, we send it back
// as x-min-lsn so the API can guarantee a fresh read.
//
// Usage: node scripts/lag-check.ts [attempts]   (expects `npm run seed` data)
const BASE = process.env.BASE_URL ?? 'http://localhost:8080';
const attempts = Number(process.argv[2] ?? 200);
let stale = 0, booked = 0;

for (let seatId = 1; seatId <= attempts; seatId++) {
  const res = await fetch(`${BASE}/bookings`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ seatId, userId: 'lag-check' }),
  });
  if (res.status === 409) continue; // a concurrent load test took it first
  if (res.status !== 201) throw new Error(`booking seat ${seatId} failed: ${res.status}`);
  booked++;
  const { lsn } = await res.json();

  const seats: { id: number; status: string }[] = await fetch(`${BASE}/events/1/seats`, {
    headers: lsn ? { 'x-min-lsn': lsn } : {},
  }).then((r) => r.json());
  if (seats.find((s) => s.id === seatId)?.status !== 'booked') stale++;
}

console.log(`${stale} of ${booked} reads did not show the user's own booking (${(100 * stale / booked).toFixed(1)}%)`);
