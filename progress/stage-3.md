# Stage 3: the on-sale spike, waiting room, async payments

**Goal:** survive 10:00:00, when everyone arrives in the same second, and take payments without losing one or charging anyone twice.

## What we built
- **Seat holds:** `POST /holds` holds a seat for `HOLD_SECONDS` (300). Expired holds are reclaimed lazily inside the same atomic UPDATE, so there is no sweeper job.
- **Load shedding:** `POST /holds` returns 503 + `Retry-After` when `MAX_DB_QUEUE` requests are already waiting for a DB connection.
- **Waiting room** (`src/waiting-room.ts`): Redis FIFO queue, admission at `ADMIT_PER_SEC`, HMAC-signed tickets and passes, and a sold-out signal.
- **Async payments:**
  ```
  POST /payments ──(1 transaction)──> payments row + seat 'paying' + outbox row
  outbox-relay ── polls outbox ──> Kafka topic payment-requests (6 partitions)
  payment-worker ── consumes ──> provider charge (idempotency key) ──> booking, or free the seat
  ```
- Test tools: `load/spike.js` + `scripts/spike.ts` (arrival-rate spike), `scripts/onsale.ts` (20K-user fairness simulator), `scripts/pay-check.ts`, `scripts/chaos/*.sh`, and payment invariants in `npm run check`.

Setup: Apple M5, 10 cores, everything on one machine.

## 1. The spike: what collapse looks like
`scripts/spike.ts 1 20000`: arrivals jump to 20K users/s in 2s and do not slow down when the server does. One replica can hold about 7.5K seats/s.

| shedding | held/s | p50 | p99 | arrivals never served |
|---|---|---|---|---|
| none | 7,472 | 2,295ms | 3,960ms | **184,138** |
| queue limit 10 | 8,638 | 1ms | 7ms | 0 |

Without shedding, the server accepts work it cannot finish, every request waits behind every other one, and k6 runs out of its 20,000 VUs (in reality: users staring at a spinner). With shedding, every arrival gets an answer in milliseconds, and throughput goes *up* because no effort is spent on requests that would wait seconds anyway. (p99 here includes the fast 503s.)

### What broke: shedding halved steady-state throughput
The steady-load sweep after shedding landed (4 replicas, 400 VUs): **8,589 served/s, down from 17,338.**
1. k6 VUs retried a 503 instantly, a mini retry storm: 32K/s of rejected requests ate the CPU real work needed.
2. A limit of 10 waiting requests per process is about a 2ms waiting budget. It shed 49% of traffic even at 100 VUs, where the system was healthy.

Fix: the test client honors `Retry-After` like a real client, and the sweep reports served/s separately. Then re-tuned:

| 4 replicas, 400 VUs | served/s | p99 | spike p99 (1 replica, 20K/s) |
|---|---|---|---|
| no shedding | 17,338 | 64ms | 3,960ms |
| queue limit 10 | 15,742 | 9ms | 9ms |
| queue limit 50 (default) | 16,027 | 23ms | 16ms |

Shedding costs about 8% of peak throughput and buys 3x better tail latency plus immunity to the spike collapse. Lesson: the shedding threshold is a latency budget. Size it from the latency you want, not from a round number.

## 2. Waiting room: fairness
Shedding keeps the server alive, but who gets the seats is random, and everyone who got a 503 hammers refresh.

`scripts/onsale.ts`: 20,000 users arrive within 1 second for event 1 (1000 seats). Each loads the seat map, picks a seat shown as free, tries to hold it, picks another on 409, and honors `Retry-After`. These runs used `MAX_DB_QUEUE=10`, the default at the time.

| | first 1000 arrivals who got seats | winners' median arrival rank | hold requests | 503s | everyone done |
|---|---|---|---|---|---|
| no waiting room | 560 | 828 | 24,803 | 6,986 | 11s |
| admit 2000/s | 265 | 1,511 | 15,143 | 4,668 | 11s |
| admit 500/s | 607 | 830 | 8,951 | 656 | 40s |
| admit 250/s | 801 | 535 | 2,604 | 0 | **81s** |
| **admit 250/s + sold-out signal** | 706 to 792 | **499 to 509** | 2,475 to 4,786 | ~100 | **13s** |

A perfectly fair sale gives a median winner rank of 500. Admitting faster than seats can sell (2000/s for 1000 seats) just moves the stampede from the front door to the seat picker, where a random race decides. Admitting at about the selling rate makes it close to FIFO, cuts DB writes up to 10x, and stops shedding.

### What broke: sold out, but 19,000 people kept waiting
At 250/s, everyone behind the first ~1000 waited up to 81s to be told the event was gone. The fix: the seat map loader sets `soldout:<event>` when no seat is free, and queue status returns `soldOut` at once.

It still took 63s on the first try, for two reasons together: users at the back polled every 30s, and the flag expired after 5s. It is only refreshed when someone loads the seat map, and while everyone waits nobody does. Final values: flag TTL 60s (still below the 300s hold expiry, so seats freed by expired holds reopen admission), poll cap 10s. Result: everyone resolved in 13s.

### Design notes
- **Stateless admission:** position `p` is admitted when `p <= seconds_since_open x rate`. No worker advances the queue, and any replica can answer. Limitation: after a quiet period, admission "credit" builds up and a later burst is let in all at once. A production queue would advance a counter on a tick.
- **HMAC tickets:** positions cannot be forged, and a pass cannot be reused by another user (both tested). Not done: binding the pass to a specific event.

## 3. Payments

### Idempotency keys
`POST /payments` requires `Idempotency-Key`. The payments primary key serializes duplicates: a concurrent retry blocks on the first insert, then conflicts and gets the existing payment back. 500 users each sending the same payment 3 times at once: **500 payments, 1000 retries answered with the original, 0 double charges.**

### What broke: connection-pool deadlock
The first version, on the duplicate path, held a pooled `client` and then called `pool.query(...)` for the existing payment, which needs a second connection from the same pool of 10. Under 150 concurrent payment requests, 10 handlers took all 10 connections and each waited for an 11th: the app process hung forever. Fix: reuse the held `client`. Rule: never take a second connection from a pool while holding one.

### What broke: the dual write lost payments
The first version committed the payment, then called `producer.send()`.
- **Kafka down for 10s:** 0 lost. kafkajs retried with backoff and rode out the outage, hiding the bug.
- **Kafka down, and 2 API replicas killed during the outage:** **50 of 400 payments stuck `pending` forever.** The only copy of the message was in the memory of the process that died. Client retries found the existing payment and got "pending" back. The seats sat in `paying`: the customer never got charged and nobody else could buy the seat.

**Fix: transactional outbox.** The message is inserted into `outbox` in the same transaction as the payment, so both exist or neither does. A separate relay publishes rows and deletes them (`FOR UPDATE SKIP LOCKED`, safe to run several). The API no longer talks to Kafka at all.

Same chaos run (`scripts/chaos/kafka-outage.sh`), 3 times: all 400 payments accepted with 202 even while Kafka was down, **0 stuck, 0 double charges** after draining.

The relay can publish a row twice (crash after send, before delete). That is fine only because consumers are idempotent.

### Kafka redelivery and the provider key
Kafka is at least once: a worker that dies mid-batch has its uncommitted batch redelivered. The worker is safe to repeat:
- The provider charge carries our idempotency key (as Stripe's API does), so a repeat returns the first result.
- The outcome is applied only `WHERE status = 'pending'`.

`scripts/chaos/worker-kill.sh`: build a backlog, start the worker, SIGKILL it the moment it starts charging, 3 times. **9 charges were redelivered, and the provider deduplicated all 9.** Without the key, 9 customers would have been charged twice. Invariants: 0 double charges, 0 stuck after draining.

Two attempts at this test did nothing (0 redeliveries) because the kill landed before the worker had joined the consumer group or while it was idle. The script now waits until the worker has actually charged something, then kills it.

**Caveat:** after a SIGKILL, the dead consumer never leaves its group, so the broker waits up to the session timeout (30s default) before giving its partitions to the restarted worker. Payments are delayed, not lost. Lower session timeouts or static membership trade faster recovery for more false rebalances.

## Other things that broke
- **Load generator was the bottleneck.** The first on-sale runs said the waiting room made everything worse (winners waited 44s). During the run, the simulator was at 165% CPU and every server at 0%: one JS thread parsing 40K seat maps of 42KB each. Those numbers measured the simulator, so they were thrown away, and users now run across worker threads. Also, 20K connections from one container exhausted ephemeral ports (`EADDRNOTAVAIL`), fixed with a wider `ip_local_port_range`. Real users come from 20K different IPs.
- **No readiness check.** Changing app config recreated all 4 replicas at once, and traffic arrived before they were serving (`no live upstreams`). The app service now has an HTTP healthcheck, so `up --wait` blocks until `/health` answers.

## Invariants (`npm run check`)
On top of the stage 0 seat checks: no provider charge without a payment, every captured payment has its booking, and no payment is stuck pending after draining. All runs above end with `INVARIANT OK` except the deliberate dual-write repro.

## Takeaways
1. Past capacity, a server must say no quickly. Queueing everything is how a spike becomes an outage.
2. Shedding needs clients that back off, or it turns into a retry storm.
3. A waiting room turns a stampede into a line. Admit at the rate you can actually sell.
4. Two writes to two systems is not a transaction. Use an outbox.
5. At-least-once delivery means every consumer step must be idempotent, including calls to third parties.
6. Check what your benchmark is measuring: the load generator and the test harness both lied to me in this stage.

## Next: stage 4
One Postgres primary is the ceiling: every hold and payment for every event commits on it. We partition by `event_id`, then hit the hot partition (one event taking all the traffic).

## Run it
```bash
docker compose down -v && docker compose up -d --wait --scale app=4
node scripts/spike.ts 1 20000 15s                   # spike, default shedding
MAX_DB_QUEUE=1000000 node scripts/spike.ts 1 20000   # spike, no shedding

# fair on-sale: 20K users, 1000 seats
WAITING_ROOM=on ADMIT_PER_SEC=250 docker compose up -d --wait --scale app=4
docker compose exec redis redis-cli FLUSHALL && npm run seed
docker run --rm --sysctl net.ipv4.ip_local_port_range="1024 65535" --network seatrush_default \
  -v $PWD:/app -w /app -e QUEUE=on node:22-slim node scripts/onsale.ts

npm run seed && node scripts/pay-check.ts 500 && npm run check
scripts/chaos/kafka-outage.sh
scripts/chaos/worker-kill.sh
```
