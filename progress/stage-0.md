# Stage 0: correct monolith, measured

**Goal:** one Node process, one Postgres. Never sell a seat twice. Record a baseline that later stages are compared against.

## What we built
- Fastify + raw SQL (`pg`), no ORM, so the locking is visible.
- Node 22 runs `.ts` directly (type stripping). No build step, `tsc` only type-checks.
- Postgres 16 in Docker Compose.
- Schema: `events`, `seats` (status `free` / `booked`), `bookings`.
- `GET /events/:id/seats`, `POST /bookings`.
- `npm run check`: invariant checker (no seat with >1 booking, seat status agrees with bookings). Exits 1 on violation.
- `npm run load`: k6 in Docker, 200 virtual users booking random seats on one event.

## What failed and how we fixed it

### 1. The naive booking double-sold seats
First version: `SELECT status`, if free then `INSERT booking` and `UPDATE seat`. Three separate queries, no lock.

Two requests read "free" before either writes, and both win. This is the classic check-then-act race (DDIA ch 7, write skew / lost update family).

| | naive | atomic fix |
|---|---|---|
| bookings for 1000 seats | **1208** | **1000** |
| seats sold twice | **193** | **0** |
| invariant | VIOLATED | OK |

**Fix:** one statement does both steps:
```sql
WITH claimed AS (
  UPDATE seats SET status = 'booked' WHERE id = $1 AND status = 'free' RETURNING id
)
INSERT INTO bookings (seat_id, user_id) SELECT id, $2 FROM claimed RETURNING id
```
The `UPDATE` takes a row lock. A competing request blocks on it, then re-evaluates `status = 'free'` under READ COMMITTED, matches zero rows, and gets 409.

We chose this over `BEGIN; SELECT ... FOR UPDATE; INSERT; UPDATE; COMMIT` because it is the same guarantee in one round trip instead of five, and less code.

**Defense in depth:** added `UNIQUE(seat_id)` on `bookings`. If app code ever regresses, the database refuses the second sale.

### 2. First load test showed 100% failures
A leftover server from an earlier smoke test (health route only) was still holding port 3000, so every booking got a 404. Lesson: always confirm what is listening before trusting a benchmark (`lsof -iTCP:3000 -sTCP:LISTEN`).

## Baseline numbers
Apple M5 (10 cores), everything local, 1 Node process, pool of 10 connections, 200 VUs, 15s.

| scenario | req/s | p50 | p95 | p99 | invariant |
|---|---|---|---|---|---|
| 1000 seats (sells out in under a second, rest are 409s) | 10,834 | 17ms | 25ms | 35ms | OK |
| 100,000 seats (mostly successful writes) | 8,518 | 23ms | 29ms | 36ms | OK |

Note: the 1000-seat number mostly measures the rejection path. The 100K-seat number is the honest write throughput.

## Known limits (fixed in later stages)
- One Node process uses one CPU core. The other 9 sit idle.
- Pool is 10 connections. More app processes will exhaust Postgres connections (stage 1, PgBouncer).
- Every seat-map read hits Postgres (stage 2, Redis + replicas).
- No waiting room. A real spike would hit the DB directly (stage 3).

## Run it
```bash
docker compose up -d --wait
npm install
npm run seed                 # 1000 seats; or: npm run seed 400 250 for 100K
LOG_LEVEL=warn npm start
npm run load                 # VUS, DURATION, SEATS env vars override defaults
npm run check
```
