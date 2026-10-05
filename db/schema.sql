-- One row per sellable seat. status is the source of truth for availability.
CREATE TABLE IF NOT EXISTS events (
  id    serial PRIMARY KEY,
  name  text NOT NULL
);

CREATE TABLE IF NOT EXISTS seats (
  id        serial PRIMARY KEY,
  event_id  int  NOT NULL REFERENCES events(id),
  label     text NOT NULL,                       -- e.g. "A-12"
  status    text NOT NULL DEFAULT 'free' CHECK (status IN ('free', 'booked')),
  UNIQUE (event_id, label)
);
CREATE INDEX IF NOT EXISTS seats_event_idx ON seats (event_id);

-- Deliberately NO unique constraint on seat_id yet.
-- Stage 0 first proves the app logic alone can double-sell, then fixes it.
CREATE TABLE IF NOT EXISTS bookings (
  id          bigserial PRIMARY KEY,
  seat_id     int  NOT NULL REFERENCES seats(id),
  user_id     text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
