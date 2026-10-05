// k6 flash-sale test: many virtual users fight over the same event's seats.
// Each iteration tries to hold a random seat. 201 = got it, 409 = taken.
// Run via: npm run load   (k6 runs in Docker, no install needed)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const shed = new Counter('shed'); // 503: load shedding turned the request away

// Default targets nginx on the compose network, skipping Docker Desktop's host port proxy.
const BASE = __ENV.BASE_URL || 'http://nginx';
const EVENTS = Number(__ENV.EVENTS || 1000);
const EVENT_IDS = __ENV.EVENT_IDS ? __ENV.EVENT_IDS.split(',').map(Number) : null; // e.g. only one region's events
const SEATS = Number(__ENV.SEATS || 1000); // per event

export const options = {
  vus: Number(__ENV.VUS || 200),
  duration: __ENV.DURATION || '30s',
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: { http_req_failed: [{ threshold: 'rate<0.01' }] },
};

// 409 (taken) and 503 (shed) are expected outcomes in a sale, not failures.
http.setResponseCallback(http.expectedStatuses(201, 409, 503));

export default function () {
  const eventId = EVENT_IDS ? EVENT_IDS[Math.floor(Math.random() * EVENT_IDS.length)] : 1 + Math.floor(Math.random() * EVENTS);
  const seatNo = 1 + Math.floor(Math.random() * SEATS);
  const res = http.post(
    `${BASE}/holds`,
    JSON.stringify({ eventId, seatNo, userId: `u${__VU}-${__ITER}` }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (res.status === 503) { shed.add(1); sleep(Number(res.headers['Retry-After'] || 1)); } // like a real client
  check(res, { 'held, taken or shed': (r) => [201, 409, 503].includes(r.status) });
}
