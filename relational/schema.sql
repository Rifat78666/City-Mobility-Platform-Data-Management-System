-- =============================================================
-- Advanced Data Management Project - Relational Schema (PostgreSQL)
-- =============================================================

-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS stations CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- -------------------------------------------------------------
-- USERS
-- -------------------------------------------------------------
CREATE TABLE users (
    user_id     SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    surname     VARCHAR(100) NOT NULL,
    birthdate   DATE         NOT NULL,
    country     VARCHAR(100) NOT NULL
);

-- -------------------------------------------------------------
-- STATIONS
-- -------------------------------------------------------------
CREATE TABLE stations (
    station_id  SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    city        VARCHAR(100) NOT NULL,
    capacity    INTEGER      NOT NULL CHECK (capacity > 0)
);

-- -------------------------------------------------------------
-- TRIPS
-- -------------------------------------------------------------
CREATE TABLE trips (
    trip_id         SERIAL PRIMARY KEY,
    user_id         INTEGER      NOT NULL REFERENCES users(user_id)    ON DELETE CASCADE,
    start_station   INTEGER      NOT NULL REFERENCES stations(station_id) ON DELETE RESTRICT,
    end_station     INTEGER      NOT NULL REFERENCES stations(station_id) ON DELETE RESTRICT,
    start_time      TIMESTAMP    NOT NULL,
    end_time        TIMESTAMP    NOT NULL,
    total_cost      NUMERIC(8,2) NOT NULL CHECK (total_cost >= 0),
    CONSTRAINT chk_trip_times CHECK (end_time > start_time)
);

-- -------------------------------------------------------------
-- EVENTS
-- -------------------------------------------------------------
CREATE TABLE events (
    event_id    SERIAL PRIMARY KEY,
    trip_id     INTEGER     NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    timestamp   TIMESTAMP   NOT NULL,
    event_type  VARCHAR(10) NOT NULL CHECK (event_type IN ('GPS','ERROR','BATTERY','DELAY')),
    value       TEXT        NOT NULL
);

-- -------------------------------------------------------------
-- INDEXES  (for query performance)
-- -------------------------------------------------------------
CREATE INDEX idx_trips_user         ON trips(user_id);
CREATE INDEX idx_trips_start_station ON trips(start_station);
CREATE INDEX idx_trips_end_station   ON trips(end_station);
CREATE INDEX idx_events_trip        ON events(trip_id);
CREATE INDEX idx_events_type        ON events(event_type);
