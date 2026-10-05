-- One row per sellable seat. status is the source of truth for availability.
CREATE TABLE IF NOT EXISTS events (
  id    serial PRIMARY KEY,
  name  text NOT NULL
);

CREATE TABLE IF NOT EXISTS seats (
  id        serial PRIMARY KEY,
  event_id  int  NOT NULL REFERENCES events(id),
  label     text NOT NULL,                       -- e.g. "A-12"
  -- free -> held (user picked it, expires at held_until) -> paying -> booked
  status      text NOT NULL DEFAULT 'free' CHECK (status IN ('free', 'held', 'paying', 'booked')),
  held_by     text,
  held_until  timestamptz,
  UNIQUE (event_id, label)
);
CREATE INDEX IF NOT EXISTS seats_event_idx ON seats (event_id);

-- UNIQUE(seat_id) is the last line of defense: even buggy app code
-- cannot sell a seat twice, the database rejects it.
CREATE TABLE IF NOT EXISTS bookings (
  id          bigserial PRIMARY KEY,
  seat_id     int  NOT NULL UNIQUE REFERENCES seats(id),
  user_id     text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per payment attempt. The client sends an Idempotency-Key; a retry
-- with the same key finds this row instead of creating a second payment.
CREATE TABLE IF NOT EXISTS payments (
  idempotency_key  text PRIMARY KEY,
  seat_id          int  NOT NULL REFERENCES seats(id),
  user_id          text NOT NULL,
  status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'captured', 'declined')),
  created_at       timestamptz NOT NULL DEFAULT now()
);

-- Stands in for the payment provider's own ledger (Stripe's side, not ours).
-- Keyed by our idempotency key, like a real provider, so a retried charge
-- returns the first result instead of charging again.
CREATE TABLE IF NOT EXISTS provider_charges (
  idempotency_key  text PRIMARY KEY,
  approved         boolean NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now()
);

-- Transactional outbox. The API inserts the Kafka message here in the SAME
-- transaction as the payment, so a committed payment always has its message.
-- The outbox relay publishes rows to Kafka and deletes them.
CREATE TABLE IF NOT EXISTS outbox (
  id       bigserial PRIMARY KEY,
  topic    text  NOT NULL,
  key      text  NOT NULL,
  payload  jsonb NOT NULL
);
