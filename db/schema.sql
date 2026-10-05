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
