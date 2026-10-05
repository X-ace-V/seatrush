// Load sweep: runs the k6 flash-sale test at increasing VU counts against
// N app replicas and prints one row per level, so you can watch latency
// degrade as load grows. Reseeds before each level so seats never run out.
//
// Usage: [SCRIPT=browse.js] [HOT_SEATS=50000 EVENTS=1 SEATS=50000] node scripts/sweep.ts <replicas> [vu levels]
//   node scripts/sweep.ts 1
//   node scripts/sweep.ts 4 100,400,1600
import { execSync } from 'node:child_process';
import { readFileSync, mkdirSync } from 'node:fs';

const replicas = Number(process.argv[2] ?? 1);
const levels = (process.argv[3] ?? '25,50,100,200,400,800').split(',').map(Number);
const duration = process.env.DURATION ?? '15s';
const script = process.env.SCRIPT ?? 'book.js'; // or browse.js for the read path
const sh = (cmd: string) => execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'] }).toString();

sh(`docker compose up -d --wait --scale app=${replicas}`);
mkdirSync('load/out', { recursive: true });

console.log(`\n${script}, ${replicas} replica(s), ${duration} per level\n`);
// served/s excludes 503s from load shedding: those are fast "come back later" answers.
console.log('| VUs | req/s | served/s | shed | p50 ms | p95 ms | p99 ms | errors |');
console.log('|---|---|---|---|---|---|---|---|');

// Wait until holds really go through on every shard. After a database
// restart PgBouncer rejects logins for server_login_retry (15s) with a cached
// error, and a level measured inside that window is meaningless. 20 events
// hash across all 12 partitions, so every shard gets probed.
async function waitUntilServing() {
  const hold = (eventId: number, seatNo: number) => fetch('http://localhost:8080/holds', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ eventId, seatNo, userId: 'warmup' }),
  }).then((r) => r.status, () => 0);
  for (let i = 1; i <= 60; i++) {
    const statuses = await Promise.all(Array.from({ length: 20 }, (_, e) => hold(e + 1, i)));
    if (statuses.every((s) => s === 201 || s === 409)) return;
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error('stack never became ready');
}

for (const vus of levels) {
  // 1000 events x 1000 seats = 1M, collisions are rare. HOT_SEATS makes event 1 a stadium.
  sh(`node scripts/seed.ts 1000 1000 ${process.env.HOT_SEATS ?? ''}`);
  await waitUntilServing();
  try {
    sh(`docker run --rm --network seatrush_default -v ${process.cwd()}/load:/load ` +
      `-e VUS=${vus} -e DURATION=${duration} -e EVENTS -e SEATS ` + // EVENTS=1 SEATS=50000: all load on the stadium
      `grafana/k6 run --quiet --summary-export /load/out/summary.json /load/${script}`);
  } catch {} // k6 exits non-zero when the error threshold is crossed; we still want the row

  const m = JSON.parse(readFileSync('load/out/summary.json', 'utf8')).metrics;
  const d = m.http_req_duration;
  const f = (n: number) => n.toFixed(1);
  const shedShare = (m.shed?.count ?? 0) / m.http_reqs.count;
  console.log(`| ${vus} | ${Math.round(m.http_reqs.rate)} | ${Math.round(m.http_reqs.rate * (1 - shedShare))} | ${(shedShare * 100).toFixed(0)}% | ${f(d['p(50)'])} | ${f(d['p(95)'])} | ${f(d['p(99)'])} | ${(m.http_req_failed.value * 100).toFixed(2)}% |`);
}
