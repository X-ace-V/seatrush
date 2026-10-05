-- events.seat_count lets the API know how many 1000-seat sections an event has.
CREATE TABLE IF NOT EXISTS events (
  id          serial PRIMARY KEY,
  name        text NOT NULL,
  seat_count  int  NOT NULL
);

-- One row per sellable seat, keyed by (event_id, seat_no). A compound key
-- instead of a global serial id, so seats can later be partitioned by event
-- and section without coordinating ids across databases. No foreign key to
-- events for the same reason. status is the source of truth for availability.
CREATE TABLE IF NOT EXISTS seats (
  event_id    int  NOT NULL,
  seat_no     int  NOT NULL,                     -- 1..seat_count; section = (seat_no - 1) / 1000
  label       text NOT NULL,                     -- e.g. "S0-12"
  -- free -> held (user picked it, expires at held_until) -> paying -> booked
  status      text NOT NULL DEFAULT 'free' CHECK (status IN ('free', 'held', 'paying', 'booked')),
  held_by     text,
  held_until  timestamptz,
  PRIMARY KEY (event_id, seat_no)
);

-- UNIQUE(event_id, seat_no) is the last line of defense: even buggy app code
-- cannot sell a seat twice, the database rejects it.
CREATE TABLE IF NOT EXISTS bookings (
  id          bigserial PRIMARY KEY,
  event_id    int  NOT NULL,
  seat_no     int  NOT NULL,
  user_id     text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (event_id, seat_no)
);

-- One row per payment attempt. The client sends an Idempotency-Key; a retry
-- with the same key finds this row instead of creating a second payment.
CREATE TABLE IF NOT EXISTS payments (
  idempotency_key  text PRIMARY KEY,
  event_id         int  NOT NULL,
  seat_no          int  NOT NULL,
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
  attempts         int  NOT NULL DEFAULT 1,  -- >1 means a redelivery that would have double charged without the key
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
