-- =============================================================
-- Schema Evolution (Part 1.3) - PostgreSQL
-- =============================================================
-- Requirement: all events of type BATTERY must include a new
-- field "battery_level" (an integer in the range [0, 100]).
--
-- In the relational model, events live in a fixed-schema table,
-- so adding a field requires a DDL migration (ALTER TABLE).
-- Run with:
--   psql -U postgres -d adm_mobility -f Schema_Evolution/postgres_add_battery_level.sql
-- =============================================================

-- Step 1: add the new column (nullable, because non-BATTERY
--         events will not have a value).
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS battery_level INTEGER;

-- Step 2: enforce the valid range [0, 100] for the new field.
ALTER TABLE events
    DROP CONSTRAINT IF EXISTS chk_battery_range;
ALTER TABLE events
    ADD CONSTRAINT chk_battery_range
    CHECK (battery_level IS NULL OR battery_level BETWEEN 0 AND 100);

-- Step 3: backfill existing BATTERY events. Our data generator stores
--         the battery percentage in the 'value' column as a string
--         (e.g. '85'), so we copy it into the new typed column.
UPDATE events
    SET battery_level = value::INTEGER
    WHERE event_type = 'BATTERY'
      AND battery_level IS NULL;

-- Step 4 (optional, stricter): make sure ONLY BATTERY events carry
--         a battery_level, and that every BATTERY event has one.
--         This conditional constraint is the extra complexity the
--         relational model needs compared to MongoDB.
ALTER TABLE events
    DROP CONSTRAINT IF EXISTS chk_battery_only;
ALTER TABLE events
    ADD CONSTRAINT chk_battery_only
    CHECK (
        (event_type =  'BATTERY' AND battery_level IS NOT NULL)
        OR
        (event_type <> 'BATTERY' AND battery_level IS NULL)
    );

-- NOTE:
--   * The new column is added to EVERY event row; non-BATTERY rows
--     store NULL, which wastes space.
--   * On a very large events table, ALTER TABLE may lock the table
--     and rewrite rows, which is costly at scale.
--   * This is why the document model (MongoDB) is more evolvable:
--     it adds the field only to the relevant embedded events with
--     no migration of existing data.

-- Verify the new column exists:
--   \d events
