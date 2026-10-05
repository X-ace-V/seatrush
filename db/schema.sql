-- Per-shard tables. Every shard has all of these and holds the rows for the
-- partitions it owns. `part` is the logical partition (0..11) of the row's
-- (event_id, section), computed by src/shards.ts; it never changes, and lets
-- a whole partition be found and moved to another shard.

-- One row per sellable seat, keyed by (event_id, seat_no). A compound key
-- instead of a global serial id, so seats can be partitioned by event
-- and section without coordinating ids across databases. No foreign key to
-- events for the same reason. status is the source of truth for availability.
CREATE TABLE IF NOT EXISTS seats (
  event_id    int  NOT NULL,
  seat_no     int  NOT NULL,                     -- 1..seat_count; section = (seat_no - 1) / 1000
  part        smallint NOT NULL,
  label       text NOT NULL,                     -- e.g. "S0-12"
  -- free -> held (user picked it, expires at held_until) -> paying -> booked
  status      text NOT NULL DEFAULT 'free' CHECK (status IN ('free', 'held', 'paying', 'booked')),
  held_by     text,
  held_until  timestamptz,
  PRIMARY KEY (event_id, seat_no)
);
CREATE INDEX IF NOT EXISTS seats_part_idx ON seats (part);

-- UNIQUE(event_id, seat_no) is the last line of defense: even buggy app code
-- cannot sell a seat twice, the database rejects it.
CREATE TABLE IF NOT EXISTS bookings (
  id          bigserial PRIMARY KEY,
  event_id    int  NOT NULL,
  seat_no     int  NOT NULL,
  part        smallint NOT NULL,
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
  part             smallint NOT NULL,
  user_id          text NOT NULL,
  status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'captured', 'declined')),
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

-- Fencing. A shard only accepts writes for partitions it owns. Routers cache
-- the partition map for up to a second, so right after a partition moves a
-- stale router may still send writes here; this turns them into an error the
-- API maps to 503 (retry), instead of a write silently landing on the old shard.
CREATE TABLE IF NOT EXISTS owned_partitions (part smallint PRIMARY KEY);

CREATE OR REPLACE FUNCTION fence() RETURNS trigger AS $$
BEGIN
  -- Writers share this lock. A partition move takes it exclusively, so the
  -- move waits for in-flight writes to commit and blocks new ones until
  -- ownership has changed. (4242 namespaces our advisory locks.)
  PERFORM pg_advisory_xact_lock_shared(4242, NEW.part);
  IF NOT EXISTS (SELECT 1 FROM owned_partitions WHERE part = NEW.part) THEN
    RAISE EXCEPTION 'partition % is not on this shard', NEW.part USING ERRCODE = 'SR001';
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER seats_fence BEFORE UPDATE ON seats FOR EACH ROW EXECUTE FUNCTION fence();
CREATE OR REPLACE TRIGGER bookings_fence BEFORE INSERT ON bookings FOR EACH ROW EXECUTE FUNCTION fence();
CREATE OR REPLACE TRIGGER payments_fence BEFORE INSERT OR UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION fence();
