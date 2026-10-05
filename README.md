# seatrush

A flash-sale ticket booking system, scaled one stage at a time, from a single Node process to sharded, multi-region Postgres with Kafka and CDC. Each stage is load tested, deliberately broken, and fixed, with the numbers recorded. The rule that must never break: **no seat is sold twice.**

![dashboard during a region partition](progress/img/dashboard-regions.png)

## Architecture
```
            us region                                    eu region
  nginx :8080 -> 4 x API (Fastify)              nginx :8081 -> 2 x API
        |  forwards writes to the home region  <-------->  |
        |        (toxiproxy: latency, partitions)          |
   Redis (cache, waiting room, CDC read model)     Redis (cache, CDC read model)
        |                                                   |
  shard 0 (catalog) + replica   shard 1             shard 2 + replica of shard 0
     \___ outbox ___/                                   (12 logical partitions,
  outbox relay -> Kafka -> payment worker -> provider    hash(event, section))
  CDC (logical decoding) -> sold-seats read model in both regions
  Prometheus + Grafana :3001
```

## Stages
Each stage has notes in [`progress/`](progress): what was built, what broke, how it was fixed, and the numbers.

| stage | built | headline result |
|---|---|---|
| [0](progress/stage-0.md) | monolith, atomic booking, invariant checker | naive code sold 1208 tickets for 1000 seats; one atomic UPDATE fixed it |
| [1](progress/stage-1.md) | replicas behind nginx, PgBouncer | found the real bottleneck (WAL flush, not the app); 10% errors from connection exhaustion fixed |
| [2](progress/stage-2.md) | read replica, Redis cache | reads 6.3K to 28K/s; read-your-writes via LSN tokens; cache stampede 2105 to 65 queries |
| [3](progress/stage-3.md) | holds, load shedding, waiting room, Kafka payments | spike p99 4s to 7ms; fair queue (median winner rank 499 of ideal 500); outbox fixed 50 lost payments |
| [obs](progress/observability.md) | Prometheus + Grafana | dashboard exposed a 34s payment stall after Kafka outages; fixed to 15s |
| [4](progress/stage-4.md) | 3 shards, hot partition, live rebalancing, fencing | hot event 10K to 31K holds/s; live partition move with 0 failed requests |
| [5](progress/stage-5.md) | two regions, write forwarding, circuit breaker, CDC | remote payment 415 to 88ms; region cut leaves local traffic at 0 errors; CDC burst 246s to 1.5s |

## Things that broke (and the invariant checker caught)
- A naive dual write lost 50 payments when Kafka and two API replicas failed together (stage 3, outbox).
- kafkajs committed offsets past a failed batch and silently dropped 25 payments (stage 4).
- A partition move that died halfway left a partition owned by no shard (stage 4, idempotent moves).
- A replica was dead for an hour while the lag graph showed 0 (stage 5, replication slots).
- An unreachable region stalled the healthy one for 20s (stage 5, timeouts + circuit breaker).
- CDC read a partition move as 268 refunds (stage 5, replication origins).

## Run it
```bash
docker compose up -d --wait --scale app=4 --scale app-eu=2
npm install
npm run seed -- 1000 1000 50000      # 1000 events + a 50K-seat stadium
npm run sweep -- 4                   # latency vs load, holds
npm run check                        # every invariant, across shards and regions
open http://localhost:3001           # dashboard
scripts/chaos/kafka-outage.sh && scripts/chaos/worker-kill.sh && scripts/chaos/region-cut.sh
```

Stack: Node 22 + TypeScript (no build step), Fastify, Postgres 16 (3 shards, 2 replicas), PgBouncer, Redis, Kafka (KRaft), nginx, toxiproxy, Prometheus, Grafana, k6. About 800 lines of application code.
