-- Catalog: lives on shard 0 only. Small, read-mostly metadata every
-- process needs, plus the simulated payment provider.

-- events.seat_count lets the API know how many 1000-seat sections an event has.
CREATE TABLE IF NOT EXISTS events (
  id          serial PRIMARY KEY,
  name        text NOT NULL,
  seat_count  int  NOT NULL
);

-- Which physical shard owns each logical partition. Data is split into a
-- FIXED number of partitions (DDIA ch 6), so adding a shard means moving a
-- few whole partitions, never rehashing every row.
CREATE TABLE IF NOT EXISTS partitions (
  id     smallint PRIMARY KEY,
  shard  smallint NOT NULL
);

-- Stands in for the payment provider's own ledger (Stripe's side, not ours).
-- Keyed by our idempotency key, like a real provider, so a retried charge
-- returns the first result instead of charging again.
CREATE TABLE IF NOT EXISTS provider_charges (
  idempotency_key  text PRIMARY KEY,
  approved         boolean NOT NULL,
  attempts         int  NOT NULL DEFAULT 1,  -- >1 means a redelivery that would have double charged without the key
  created_at       timestamptz NOT NULL DEFAULT now()
);

