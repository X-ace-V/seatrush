// On-sale simulator: USERS people arrive within ARRIVAL_MS, all wanting a
// seat on event 1 (seed it with `npm run seed`: 1000 seats). Each user loads
// the seat map, picks a seat shown as free, tries to hold it, picks another
// on 409, and honors Retry-After on 503. With WAITING_ROOM=on on the app,
// users queue first. Reports fairness, wait times and request volume.
//
// Users are split across WORKERS threads: one JS thread parsing thousands of
// 42KB seat maps was itself the bottleneck (165% CPU, servers idle).
// Runs inside the compose network so 20K concurrent users are cheap:
//   docker run --rm --network seatrush_default -v $PWD:/app -w /app node:22-slim node scripts/onsale.ts
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

const BASE = process.env.BASE_URL ?? 'http://nginx';
const USERS = Number(process.env.USERS ?? 20000);
const ARRIVAL_MS = Number(process.env.ARRIVAL_MS ?? 1000);
const QUEUE = process.env.QUEUE === 'on';
const WORKERS = Number(process.env.WORKERS ?? 4);
const MAX_TRIES = 10; // hold attempts before a user gives up

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const calls = { queue: 0, map: 0, hold: 0, rejected503: 0 };
const json = { 'content-type': 'application/json' };
const t0: number = isMainThread ? Date.now() + 500 : workerData.t0; // shared start time

async function user(rank: number) {
  await sleep(t0 - Date.now() + (rank / USERS) * ARRIVAL_MS);
  const userId = `u${rank}`;
  const headers: Record<string, string> = { ...json };

  if (QUEUE) {
    calls.queue++;
    const { ticket } = await fetch(`${BASE}/queue/1`, { method: 'POST', headers, body: JSON.stringify({ userId }) }).then((r) => r.json());
    for (;;) {
      calls.queue++;
      const res = await fetch(`${BASE}/queue/1/status`, { headers: { 'x-queue-ticket': ticket } });
      const body = await res.json();
      if (body.soldOut) return { rank, won: false, ms: Date.now() - t0 };
      if (body.admitted) { headers['x-queue-pass'] = body.pass; break; }
      await sleep(Number(res.headers.get('retry-after')) * 1000);
    }
  }

  for (let tries = 0; tries < MAX_TRIES; ) {
    calls.map++;
    const seats: { seatNo: number; status: string }[] = await fetch(`${BASE}/events/1/sections/0/seats`).then((r) => r.json());
    const free = seats.filter((s) => s.status === 'free');
    if (!free.length) return { rank, won: false, ms: Date.now() - t0 }; // sold out

    calls.hold++;
    const res = await fetch(`${BASE}/holds`, {
      method: 'POST', headers,
      body: JSON.stringify({ eventId: 1, seatNo: free[Math.floor(Math.random() * free.length)].seatNo, userId }),
    });
    await res.arrayBuffer();
    if (res.status === 201) return { rank, won: true, ms: Date.now() - t0 };
    if (res.status === 503) { calls.rejected503++; await sleep(1000 * Number(res.headers.get('retry-after') ?? 1) + Math.random() * 1000); continue; }
    tries++; // 409: someone beat us to that seat
  }
  return { rank, won: false, ms: Date.now() - t0 };
}

if (!isMainThread) {
  // Worker w simulates ranks w, w + WORKERS, ... so arrivals stay interleaved.
  const ranks = Array.from({ length: Math.ceil(USERS / WORKERS) }, (_, i) => workerData.w + i * WORKERS).filter((r) => r < USERS);
  parentPort!.postMessage({ results: await Promise.all(ranks.map(user)), calls });
} else {
  type Out = { results: Awaited<ReturnType<typeof user>>[]; calls: typeof calls };
  const outs = await Promise.all(
    Array.from({ length: WORKERS }, (_, w) => new Promise<Out>((done) =>
      new Worker(new URL(import.meta.url), { workerData: { w, t0 } }).once('message', done))),
  );
  const results = outs.flatMap((o) => o.results);
  for (const o of outs) for (const k in calls) calls[k as keyof typeof calls] += o.calls[k as keyof typeof calls];

  const winners = results.filter((r) => r.won);
  const pct = (xs: number[], p: number) => xs.sort((a, b) => a - b)[Math.floor((xs.length - 1) * p)] ?? 0;

  console.log(JSON.stringify({
    mode: QUEUE ? 'waiting room' : 'no waiting room',
    seatsSold: winners.length,
    // Fairness: in a fair sale the earliest arrivals get the seats.
    firstThousandArrivalsWhoGotSeats: winners.filter((r) => r.rank < 1000).length,
    medianArrivalRankOfWinners: pct(winners.map((r) => r.rank), 0.5),
    winnerSecondsToSeat: { p50: pct(winners.map((r) => r.ms), 0.5) / 1000, p99: pct(winners.map((r) => r.ms), 0.99) / 1000 },
    everyoneDoneSeconds: (Date.now() - t0) / 1000,
    requests: calls,
  }, null, 2));
}
