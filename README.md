# seatrush

A flash-sale ticket booking system, scaled one stage at a time. Each stage is load tested, deliberately broken, and fixed. The rule that must never break: **no seat is sold twice.**

Stack: Node 22 + TypeScript, Fastify, Postgres (later Redis, Kafka), k6.

Per-stage notes (what we built, what failed, numbers) live in [`progress/`](progress).

| stage | focus | status |
|---|---|---|
| 0 | correct monolith + baseline | done |
| 1 | stateless app servers, connection pooling | |
| 2 | caching, read replicas | |
| 3 | waiting room, queues, idempotency | |
| 4 | partitioning, hot partitions | |
| 5 | multi-region, CDC | |

Quick start: see [progress/stage-0.md](progress/stage-0.md#run-it).
