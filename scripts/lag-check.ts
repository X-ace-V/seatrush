// Read-your-writes check: hold a seat, then immediately load the seat map
// and see whether our own hold shows up. Against an async replica it
// often won't. If the hold response carries an `lsn`, we send it back
// as x-min-lsn so the API can guarantee a fresh read.
//
// Usage: node scripts/lag-check.ts [attempts] [eventId]   (pick an event on a shard with a replica)
const BASE = process.env.BASE_URL ?? 'http://localhost:8080';
const attempts = Number(process.argv[2] ?? 200);
const eventId = Number(process.argv[3] ?? 1);
let stale = 0, held = 0;

for (let seatNo = 1; seatNo <= attempts; seatNo++) {
  const res = await fetch(`${BASE}/holds`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ eventId, seatNo, userId: 'lag-check' }),
  });
  if (res.status === 409) continue; // a concurrent load test took it first
  if (res.status !== 201) throw new Error(`holding seat ${seatNo} failed: ${res.status}`);
  held++;
  const { lsn } = await res.json();

  const seats: { seatNo: number; status: string }[] = await fetch(`${BASE}/events/${eventId}/sections/0/seats`, {
    headers: lsn ? { 'x-min-lsn': lsn } : {},
  }).then((r) => r.json());
  if (seats.find((s) => s.seatNo === seatNo)?.status !== 'held') stale++;
}

console.log(`${stale} of ${held} reads did not show the user's own hold (${(100 * stale / held).toFixed(1)}%)`);
