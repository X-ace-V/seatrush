// k6 on-sale spike: arrivals jump from 0 to RATE users/sec in 2s and stay
// there. Unlike a fixed VU count, arrival rate does not slow down when the
// server does, which is what real traffic at 10:00:00 looks like.
// Each user tries to hold one random seat and gives up after 10s.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://nginx';
const SEATS = Number(__ENV.SEATS || 1000000);
const RATE = Number(__ENV.RATE || 20000);

const outcome = {
  held: new Counter('held'),        // 201: got a seat
  taken: new Counter('taken'),      // 409: seat already gone
  rejected: new Counter('rejected'),// 503: server shed the request
  failed: new Counter('failed'),    // timeout, connection error, 5xx
};

export const options = {
  summaryTrendStats: ['p(50)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    onsale: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 2000,
      maxVUs: 20000,
      stages: [{ target: RATE, duration: '2s' }, { target: RATE, duration: __ENV.DURATION || '20s' }],
    },
  },
};

http.setResponseCallback(http.expectedStatuses(201, 409, 503));

export default function () {
  const res = http.post(
    `${BASE}/holds`,
    JSON.stringify({ seatId: 1 + Math.floor(Math.random() * SEATS), userId: `u${__VU}-${__ITER}` }),
    { headers: { 'Content-Type': 'application/json' }, timeout: '10s' },
  );
  const bucket = { 201: 'held', 409: 'taken', 503: 'rejected' }[res.status] || 'failed';
  outcome[bucket].add(1);
}
