// k6 read test: users loading seat maps during a flash sale.
// HOT share of requests go to event 1 (the big on-sale), the rest spread
// across all EVENTS. Run via: SCRIPT=browse.js npm run sweep -- <replicas>
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://nginx';
const EVENTS = Number(__ENV.EVENTS || 1000);
const HOT = Number(__ENV.HOT || 0.8);

export const options = {
  vus: Number(__ENV.VUS || 200),
  duration: __ENV.DURATION || '30s',
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const eventId = Math.random() < HOT ? 1 : 1 + Math.floor(Math.random() * EVENTS);
  const res = http.get(`${BASE}/events/${eventId}/seats`);
  check(res, { 'ok': (r) => r.status === 200 });
}
