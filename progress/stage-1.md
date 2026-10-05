# Stage 1: horizontal scaling and connection pooling

**Goal:** measure how latency degrades as load grows on one server, then check whether more servers behind a load balancer help.

## What we built
- `Dockerfile`: the API runs as a stateless container, scaled with `--scale app=N`.
- nginx round-robin load balancer in front of all replicas (`localhost:8080`).
- `npm run sweep -- <replicas> [vus]`: runs k6 at 25, 50, 100, 200, 400, 800 VUs and prints req/s and p50/p95/p99 per level. It reseeds 1M seats before each level so seats never run out.
- PgBouncer between the apps and Postgres.

Setup for every number below: Apple M5, 10 cores, all containers plus k6 on the same machine, 15s per level, 1M seats.

## 1. One server: how latency degrades

| VUs | req/s | p50 | p95 | p99 |
|---|---|---|---|---|
| 25 | 14,534 | 1.6ms | 2.5ms | 3.1ms |
| 100 | 13,161 | 7.3ms | 9.6ms | 11.8ms |
| 200 | 15,066 | 12.4ms | 18.1ms | 23.2ms |
| 400 | 17,050 | 21.4ms | 37.9ms | 46.8ms |
| 800 | 17,131 | 45.0ms | 56.0ms | 66.6ms |

**Reading it:** throughput stops growing at about 17K req/s. Past that point, extra users do not get more work done. They only wait longer in a queue. That is Little's law: `latency = concurrency / throughput`, so 800 / 17,131 = 46ms, which matches the measured p50.

Side note: stage 0 measured 8.5K req/s for one server. The difference is that k6 then went through Docker Desktop's host port proxy (`host.docker.internal`), which was itself a bottleneck. k6 now joins the compose network and talks to nginx directly. Lesson: the benchmark path must not be slower than the system under test.

## 2. More servers: the throughput ceiling

| replicas | peak req/s | p99 at 800 VUs |
|---|---|---|
| 1 | 17,131 | 66.6ms |
| 2 | 25,183 | 76.5ms |
| 4 | 27,269 | 102.2ms |
| 8 | 25,123 | 83.3ms |

Going from 1 to 2 replicas gives +47%. After that, nothing. **Adding app servers stopped helping because the app was no longer the bottleneck.**

### Finding the real bottleneck
Sampled during a live run (8 replicas, 400 VUs):

| container | CPU |
|---|---|
| each app replica | ~36% (mostly idle) |
| nginx | 92% |
| postgres | **295%** |
| k6 | 177% |

`pg_stat_activity` at the same moment: **51 of ~60 active connections waiting on `LWLock:WALWrite`**. Every booking commit must flush the write-ahead log to disk before Postgres acknowledges it, and the commits queue up behind each other.

### Proving it: turn off the flush wait
`ALTER SYSTEM SET synchronous_commit = off` (4 replicas):

| | peak req/s |
|---|---|
| synchronous_commit on | 27,269 |
| synchronous_commit off | **33,994 (+25%)** |

**We reverted it.** With it off, Postgres acknowledges a booking before it is durable. A crash can lose the last few hundred ms of confirmed bookings, those seats go back to `free`, and get sold again to someone else. For a ticketing system that is a double sale with extra steps. Durability stays on. Scaling writes beyond one Postgres is what partitioning (stage 4) is for.

## 3. What broke: connection exhaustion

Each replica opens its own pool. 8 replicas x `PG_POOL_SIZE=20` = 160 connections, while Postgres's default `max_connections` is 100.

| 8 replicas, pool 20 | req/s at 100 VUs | errors |
|---|---|---|
| direct to Postgres | 15,307 | **10.17%** `sorry, too many clients already` |

Even the seed script could not connect. In a real incident this means an operator cannot get a psql session to investigate.

Raising `max_connections` is the wrong fix. Each Postgres connection is a separate OS process with its own memory, and more concurrent connections means more contention on the same WAL lock we already saw.

### Fix: PgBouncer in transaction mode
Apps connect to PgBouncer, which holds a small fixed pool of real Postgres connections and lends one out per transaction. This is safe here because each booking is a single statement with no session state.

| 8 replicas, pool 20 | req/s at 400 VUs | p99 at 800 VUs | errors | Postgres connections |
|---|---|---|---|---|
| direct | n/a | n/a | 10.17% | capped at 100 |
| via PgBouncer (pool 40) | 22,363 | 62.7ms | **0%** | **41** |

### Tuning the PgBouncer pool size (8 replicas, 400 VUs)

| server pool | req/s | p99 |
|---|---|---|
| 10 | 16,502 | 34.3ms |
| 20 | 20,039 | 31.6ms |
| 40 | 22,363 | 43.3ms |

The gains flatten out: past ~40 connections, Postgres cannot do more useful work at once. We kept 40.

Invariant after all stage 1 runs: 256,246 bookings, **0 double sold**.

## Takeaways
1. Past saturation, more load only buys more latency (Little's law). Watch p99, not averages.
2. Scaling the stateless tier only helps while it is the bottleneck. Measure before adding boxes.
3. Here the ceiling is the single Postgres primary's commit path. Durability has a price, and we pay it on purpose.
4. Connections are a scarce resource. Pool them centrally instead of letting every replica open its own.

## Caveat
Everything runs on one laptop. k6, nginx and Postgres compete for the same 10 cores, so absolute numbers are lower than real separate hosts would give. The shape of each curve and the bottleneck diagnosis are what transfer.

## Next: stage 2
`GET /events/:id/seats` still hits the primary every time, and in a real sale seat-map reads outnumber bookings 100 to 1. We add a Redis cache and a read replica, then hit the replication lag bug.

## Run it
```bash
docker compose up -d --wait
npm run sweep -- 1              # 1 replica, default VU levels
npm run sweep -- 8 100,400,800
PG_POOL_SIZE=20 npm run sweep -- 8 400   # per-replica pool
PGB_POOL_SIZE=20 docker compose up -d pgbouncer   # PgBouncer server pool
npm run check
```
