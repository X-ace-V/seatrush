// Prometheus metrics shared by the API, payment worker and outbox relay.
// Every process also exports Node defaults (event loop lag, heap, CPU).
import http from 'node:http';
import client from 'prom-client';
import type { FastifyInstance } from 'fastify';
import type pg from 'pg';

client.collectDefaultMetrics();
const { register } = client;
export { client as prom };

const httpDuration = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'API latency by route and status',
  labelNames: ['route', 'status'],
  buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
});

export const cacheRequests = new client.Counter({
  name: 'cache_requests_total',
  help: 'Seat map cache lookups: hit, miss (loaded from DB) or coalesced (joined an in-flight load)',
  labelNames: ['result'],
});

// Gauges read from the pools at scrape time, so they cost nothing per request.
export function watchPools(pools: Record<string, pg.Pool>) {
  new client.Gauge({
    name: 'db_pool_waiting',
    help: 'Requests waiting for a DB connection (what load shedding looks at)',
    labelNames: ['pool'],
    collect() { for (const [name, p] of Object.entries(pools)) this.set({ pool: name }, p.waitingCount); },
  });
}

// Times every request and serves /metrics on the API's own port.
export function instrument(app: FastifyInstance) {
  app.addHook('onResponse', async (req, reply) => {
    const route = req.routeOptions.url;
    if (route && route !== '/metrics' && route !== '/health') {
      httpDuration.observe({ route: `${req.method} ${route}`, status: reply.statusCode }, reply.elapsedTime / 1000);
    }
  });
  app.get('/metrics', async (_req, reply) => reply.type(register.contentType).send(await register.metrics()));
}

// Background processes (worker, relay) have no HTTP server, so give them one.
export function serveMetrics(port = 9100) {
  http.createServer(async (_req, res) => {
    res.setHeader('content-type', register.contentType);
    res.end(await register.metrics());
  }).listen(port);
}
