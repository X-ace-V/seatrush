// End-to-end payment check: USERS users each hold a seat on event 1, then
// send the payment 3 times at once with the same Idempotency-Key (a client
// retrying after a timeout), then wait for the outcome.
// Usage: node scripts/pay-check.ts [users]   then: npm run check
const BASE = process.env.BASE_URL ?? 'http://localhost:8080';
const USERS = Number(process.argv[2] ?? 500);
const json = { 'content-type': 'application/json' };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const tally: Record<string, number> = {};
const count = (k: string) => (tally[k] = (tally[k] ?? 0) + 1);

async function user(i: number) {
  const seatId = i + 1, userId = `payer${i}`, key = `pay-${i}`;
  const hold = await fetch(`${BASE}/holds`, { method: 'POST', headers: json, body: JSON.stringify({ seatId, userId }) });
  if (hold.status !== 201) return count(`hold ${hold.status}`);

  const pay = () => fetch(`${BASE}/payments`, {
    method: 'POST', headers: { ...json, 'idempotency-key': key }, body: JSON.stringify({ seatId, userId }),
  }).then((r) => r.status).catch(() => 'network error');
  for (const status of await Promise.all([pay(), pay(), pay()])) count(`pay ${status}`);

  for (let t = 0; t < 60; t++) {
    const res = await fetch(`${BASE}/payments/${key}`);
    const { status } = res.ok ? await res.json() : { status: `http ${res.status}` };
    if (status !== 'pending') return count(`final ${status}`);
    await sleep(500);
  }
  count('final still pending after 30s');
}

// 50 users at a time
for (let i = 0; i < USERS; i += 50) {
  await Promise.all(Array.from({ length: Math.min(50, USERS - i) }, (_, j) => user(i + j)));
}
console.table(tally);
