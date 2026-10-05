# Stage 2: read replica, read-your-writes, caching

**Goal:** in a real sale, seat map reads outnumber bookings about 100 to 1. Take that read load off the primary without ever showing a user a wrong view of their own booking.

## What we built
- Seed now creates many events (`npm run seed 1000 40 25` = 1000 events x 1000 seats).
- `load/browse.js`: seat map reads, 80% to event 1 to model one big on-sale.
- Streaming read replica (`postgres-replica`), cloned with `pg_basebackup`, with its own PgBouncer.
- `REPLICA_DELAY=100ms` injects replication lag (`recovery_min_apply_delay`).
- Read-your-writes using the WAL LSN.
- Redis cache-aside for seat maps (1s TTL) with single-flight.
- `scripts/lag-check.ts`: books a seat, reads the map, counts how often the user's own booking is missing.

Setup: Apple M5, 10 cores, everything on one machine, 4 app replicas, 15s per level unless noted.

## 1. Read baseline: the primary serves everything

| VUs | req/s | p50 | p99 |
|---|---|---|---|
| 50 | 7,891 | 5.7ms | 16.1ms |
| 200 | 7,134 | 7.7ms | 104.2ms |
| 800 | 6,331 | 136.6ms | 273.2ms |

A seat map is 1000 rows and 42KB of JSON. Postgres used 2.2 cores, and every app process was close to 100% of its single core building JSON. Reads cost far more than bookings.

## 2. Read replica

### Throughput: no gain on one machine

| reads from | req/s at 800 VUs |
|---|---|
| primary | 6,331 |
| replica | 6,075 |

The load did move: during the run the primary sat at 2% CPU and the replica at 222%. But the whole machine was 94% busy, and the replica shares the same cores, so moving work between containers adds no capacity. On real hardware the replica is a separate box.

### What it does buy: isolation
400 VUs browsing while 50 VUs book, at the same time:

| seat maps served by | booking req/s | booking p99 |
|---|---|---|
| primary | 688 | 231ms |
| replica | **3,633** | **44ms** |

Browse traffic no longer competes with bookings for the primary. Bookings are where the money is.

### Cost
Booking throughput with the replica running vs stopped: 18.2K vs 20.0K req/s (about 10%). The primary now ships WAL, and the replica replays every write on the same CPUs.

## 3. What broke: users could not see their own booking

Replication is async. A user books a seat, the page reloads the seat map from the replica, and the replica has not replayed the booking yet, so the seat still shows as free. DDIA ch 5 calls the missing guarantee **read-your-writes**.

| replica lag | stale reads (`lag-check`) |
|---|---|
| natural, idle | 0 / 200 |
| natural, under 400 VU booking load | 0 / 255 (`replay_lag` was 0.4ms) |
| injected 100ms | **200 / 200** |

Locally there is no network distance, so natural lag stays sub-millisecond. In production it comes from cross-region links, replicas busy with long queries, vacuum, or bursts. Injecting the delay is the honest way to reproduce it here.

### Fix: LSN tokens
1. After a booking commits, the API returns `pg_current_wal_lsn()` from the primary.
2. The client sends it back on reads as `x-min-lsn`.
3. The API asks the replica `pg_last_wal_replay_lsn() >= token`. If yes, it reads the replica; if not, the primary.

| replica lag | stale reads after fix |
|---|---|
| 100ms | **0 / 200** |
| 1s | **0 / 50** |

Only users who just wrote pay for a primary read. Everyone else stays on the replica.

**Subtle part:** the freshness check must be its own query that runs before the read. Folding it into the read (`WHERE ... AND pg_last_wal_replay_lsn() >= $2`) races: the read's snapshot is taken at the start of the query, replay can advance while it runs, so the check can pass against a snapshot that is still too old.

**Rejected alternative:** "send a user's reads to the primary for N seconds after they write." Simpler, but N is a guess. Too short and the bug returns during a lag spike; too long and the primary gets read load back.

**Cost:** one extra `SELECT pg_current_wal_lsn()` round trip per booking.

## 4. Redis cache

Design choices:
- Cache key `seats:<eventId>`, value is the **serialized JSON string**, so a hit skips both the query and `JSON.stringify`.
- **1s TTL, no invalidation on booking.** On the hot event, seats sell every millisecond, so invalidating on each booking would keep the cache permanently empty. A seat map up to ~1s stale is fine, because the booking itself is checked atomically on the primary. The worst case is a user clicking a seat that just sold and getting a 409.
- Clients holding an `x-min-lsn` token skip the cache, so read-your-writes still holds.

| VUs | req/s (no cache) | req/s (cache) | p99 (no cache) | p99 (cache) |
|---|---|---|---|---|
| 50 | 7,891 | 26,792 | 16.1ms | 6.5ms |
| 200 | 7,134 | 30,109 | 104.2ms | 26.4ms |
| 800 | 6,331 | **27,947** | 273.2ms | **91.6ms** |

### What broke: cache stampede
Every second the hot key expires, and every request that misses at that moment queries the replica.

Measured as replica `xact_commit` delta during 15s at 800 VUs, all traffic on event 1 (ideal is about 15, one per second):

| | replica queries | req/s |
|---|---|---|
| plain cache-aside | **2,105** | 31,478 |
| with single-flight | **65** | 33,024 |

**Fix: single-flight.** Inside each app process, concurrent misses for the same key await one shared in-flight load (a `Map<key, Promise>`, about 10 lines). The remaining ~4 per expiry are one per app process. A distributed lock in Redis could reduce that to 1, but it adds lock timeouts and failure modes for little gain at this size.

## 5. What broke: stale service discovery in nginx

While redeploying, `docker compose up -d` without `--scale` silently dropped the app from 4 replicas to 1. nginx had resolved `app` once at startup and kept sending traffic to the 3 dead IPs: 278 upstream errors, **14 req/s, p99 40s**.

Fix: `server app:3000 resolve` with Docker's DNS (`resolver 127.0.0.11 valid=2s`) plus `proxy_next_upstream error timeout`. Scaling 4 to 2 to 4 replicas during a 200 VU run: **0 failed requests**, and the sweep script no longer restarts nginx.

Lesson: a load balancer that caches addresses forever turns every scale-down into an outage.

## Final state: mixed load
400 VUs browsing + 50 VUs booking at the same time:

| | req/s | p50 | p99 |
|---|---|---|---|
| bookings | 4,290 | 9.6ms | 37.2ms |
| browse | 21,977 | 11.6ms | 66.7ms |

Invariant after all runs: 62,362 bookings, **0 double sold**.

## Takeaways
1. A replica on the same hardware adds no capacity. It adds isolation, which protects the write path.
2. Async replication means stale reads. Decide per request who needs fresh data, and route only them to the primary.
3. Caching is a staleness budget. The authoritative check (atomic booking) is what makes a stale cache safe.
4. Short TTLs on hot keys stampede. Coalesce misses.
5. Service discovery is part of scaling. A load balancer must notice replicas leaving.

## Next: stage 3
A real on-sale is a spike, not a ramp: hundreds of thousands of users at 10:00:00. Even an atomic booking path melts when they all arrive in the same second. We add a waiting room, temporary seat holds, async payments through Kafka, and idempotency keys.

## Run it
```bash
docker compose down -v && docker compose up -d --wait   # fresh volume so the replication role is created
npm run seed
SCRIPT=browse.js npm run sweep -- 4 50,200,800
REPLICA_DELAY=100ms docker compose up -d postgres-replica
node scripts/lag-check.ts
READS_FROM=primary docker compose up -d app            # compare reads on the primary
npm run check
```

Stampede measurement:
```bash
docker exec seatrush-postgres-replica-1 psql -U seatrush -tAc \
  "select xact_commit from pg_stat_database where datname='seatrush'"   # before and after a HOT=1 browse run
```
