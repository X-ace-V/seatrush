// k6 flash-sale test: many virtual users fight over the same event's seats.
// Each iteration tries to book a random seat. 201 = got it, 409 = taken.
// Run via: npm run load   (k6 runs in Docker, no install needed)
import http from 'k6/http';
import { check } from 'k6';

// Default targets nginx on the compose network, skipping Docker Desktop's host port proxy.
const BASE = __ENV.BASE_URL || 'http://nginx';
const SEATS = Number(__ENV.SEATS || 1000);

export const options = {
  vus: Number(__ENV.VUS || 200),
  duration: __ENV.DURATION || '30s',
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: { http_req_failed: [{ threshold: 'rate<0.01' }] },
};

// 409 is an expected outcome in a sale, not a failure.
http.setResponseCallback(http.expectedStatuses(201, 409));

export default function () {
  const seatId = 1 + Math.floor(Math.random() * SEATS);
  const res = http.post(
    `${BASE}/bookings`,
    JSON.stringify({ seatId, userId: `u${__VU}-${__ITER}` }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'booked or taken': (r) => r.status === 201 || r.status === 409 });
}
