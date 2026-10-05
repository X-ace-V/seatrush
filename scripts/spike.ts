// Runs load/spike.js at RATE users/sec against N app replicas and prints
// what users experienced: how many got a seat, how fast, how many failed.
// Usage: node scripts/spike.ts <replicas> <rate> [duration]
import { execSync } from 'node:child_process';
import { readFileSync, mkdirSync } from 'node:fs';

const [replicas = '1', rate = '20000', duration = '20s'] = process.argv.slice(2);
const sh = (cmd: string) => execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'] }).toString();

sh(`docker compose up -d --wait --scale app=${replicas}`);
sh('node scripts/seed.ts 1000 40 25');
mkdirSync('load/out', { recursive: true });
try {
  sh(`docker run --rm --network seatrush_default -v ${process.cwd()}/load:/load ` +
    `-e RATE=${rate} -e DURATION=${duration} grafana/k6 run --quiet --summary-export /load/out/spike.json /load/spike.js`);
} catch {}

const m = JSON.parse(readFileSync('load/out/spike.json', 'utf8')).metrics;
const n = (k: string) => m[k]?.count ?? 0;
const secs = parseInt(duration) + 2;
const d = m.http_req_duration;
console.table({
  [`${replicas} replica(s), ${rate}/s offered`]: {
    'held/s': Math.round(n('held') / secs),
    taken: n('taken'),
    'rejected 503': n('rejected'),
    'failed/timeout': n('failed'),
    'not sent (k6 out of VUs)': n('dropped_iterations'),
    'p50 ms': Math.round(d['p(50)']),
    'p99 ms': Math.round(d['p(99)']),
  },
});
