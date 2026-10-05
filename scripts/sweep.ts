// Load sweep: runs the k6 flash-sale test at increasing VU counts against
// N app replicas and prints one row per level, so you can watch latency
// degrade as load grows. Reseeds before each level so seats never run out.
//
// Usage: [SCRIPT=browse.js] node scripts/sweep.ts <replicas> [vu levels, comma separated]
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
console.log('| VUs | req/s | p50 ms | p95 ms | p99 ms | errors |');
console.log('|---|---|---|---|---|---|');

for (const vus of levels) {
  sh('node scripts/seed.ts 1000 40 25'); // 1000 events x 1000 seats = 1M, collisions are rare
  try {
    sh(`docker run --rm --network seatrush_default -v ${process.cwd()}/load:/load ` +
      `-e VUS=${vus} -e DURATION=${duration} -e SEATS=1000000 ` +
      `grafana/k6 run --quiet --summary-export /load/out/summary.json /load/${script}`);
  } catch {} // k6 exits non-zero when the error threshold is crossed; we still want the row

  const m = JSON.parse(readFileSync('load/out/summary.json', 'utf8')).metrics;
  const d = m.http_req_duration;
  const f = (n: number) => n.toFixed(1);
  console.log(`| ${vus} | ${Math.round(m.http_reqs.rate)} | ${f(d['p(50)'])} | ${f(d['p(95)'])} | ${f(d['p(99)'])} | ${(m.http_req_failed.value * 100).toFixed(2)}% |`);
}
