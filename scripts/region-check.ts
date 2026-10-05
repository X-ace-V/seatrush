// Latency of one user's writes through a given region's API: hold a seat,
// then pay for it, one user at a time (latency, not throughput).
// Usage: node scripts/region-check.ts <apiBase> <eventId> [users=40]
//   e.g. node scripts/region-check.ts http://localhost:8081 7
const [base, eventArg, usersArg] = process.argv.slice(2);
const eventId = Number(eventArg), users = Number(usersArg ?? 40);
const json = { 'content-type': 'application/json' };
const holdMs: number[] = [], payMs: number[] = [];
const time = async (f: () => Promise<Response>) => {
  const t = performance.now(); const res = await f(); await res.text(); return [performance.now() - t, res.status] as const;
};

for (let i = 0; i < users; i++) {
  const seatNo = 500 + i, userId = `rc${i}-${Date.now()}`, body = JSON.stringify({ eventId, seatNo, userId });
  const [h, hs] = await time(() => fetch(`${base}/holds`, { method: 'POST', headers: json, body }));
  if (hs !== 201) { console.log(`hold ${seatNo}: ${hs}`); continue; }
  const [p, ps] = await time(() => fetch(`${base}/payments`, { method: 'POST', headers: { ...json, 'idempotency-key': userId }, body }));
  if (ps !== 202) console.log(`pay ${seatNo}: ${ps}`);
  holdMs.push(h); payMs.push(p);
}
const pct = (xs: number[], q: number) => Math.round(xs.sort((a, b) => a - b)[Math.floor((xs.length - 1) * q)] ?? 0);
console.log(`${base} event ${eventId}: hold p50 ${pct(holdMs, 0.5)}ms p95 ${pct(holdMs, 0.95)}ms | pay p50 ${pct(payMs, 0.5)}ms p95 ${pct(payMs, 0.95)}ms`);
