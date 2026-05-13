"""
Advanced Data Management Project
Part 1.2 — Query Implementation: PostgreSQL
============================================
Queries:
  Q1 - All trips with user info, start/end station names
  Q2 - All users with number of trips and average trip duration
  Q3 - All stations with trips starting/ending there
  Q4 - All trips containing at least one ERROR event
"""

import time
import pg8000

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "adm_mobility",
    "user": "postgres",
    "password": "Rifat@786",
}


def connect():
    return pg8000.connect(**PG_CONFIG)


def timed_query(cur, sql: str, label: str, params=None):
    """Execute a query and print execution time."""
    start = time.perf_counter()
    cur.execute(sql, params or [])
    rows = cur.fetchall()
    elapsed = time.perf_counter() - start
    print(f"[{label}] rows={len(rows)}, time={elapsed:.4f}s")
    return rows, elapsed


# ────────────────────────────────────────────────────────────────
# Q1 — All trips with user info and station names
# ────────────────────────────────────────────────────────────────
Q1 = """
SELECT
    t.trip_id,
    u.user_id,
    u.name        AS user_name,
    u.surname     AS user_surname,
    u.birthdate,
    u.country,
    ss.name       AS start_station,
    es.name       AS end_station,
    t.start_time,
    t.end_time,
    t.total_cost
FROM trips t
JOIN users    u  ON t.user_id       = u.user_id
JOIN stations ss ON t.start_station = ss.station_id
JOIN stations es ON t.end_station   = es.station_id
ORDER BY t.trip_id;
"""

# ────────────────────────────────────────────────────────────────
# Q2 — All users with trip count and average duration (minutes)
# ────────────────────────────────────────────────────────────────
Q2 = """
SELECT
    u.user_id,
    u.name,
    u.surname,
    COUNT(t.trip_id)                                         AS num_trips,
    AVG(EXTRACT(EPOCH FROM (t.end_time - t.start_time))/60) AS avg_duration_minutes
FROM users u
LEFT JOIN trips t ON u.user_id = t.user_id
GROUP BY u.user_id, u.name, u.surname
ORDER BY u.user_id;
"""

# ────────────────────────────────────────────────────────────────
# Q3 — All stations with departing and arriving trip counts
# ────────────────────────────────────────────────────────────────
Q3 = """
SELECT
    s.station_id,
    s.name,
    s.city,
    COUNT(DISTINCT t_start.trip_id)  AS trips_starting,
    COUNT(DISTINCT t_end.trip_id)    AS trips_ending
FROM stations s
LEFT JOIN trips t_start ON s.station_id = t_start.start_station
LEFT JOIN trips t_end   ON s.station_id = t_end.end_station
GROUP BY s.station_id, s.name, s.city
ORDER BY s.station_id;
"""

# ────────────────────────────────────────────────────────────────
# Q4 — Trips with at least one ERROR event
# ────────────────────────────────────────────────────────────────
Q4 = """
SELECT DISTINCT
    t.trip_id,
    t.user_id,
    t.start_time,
    t.end_time,
    t.total_cost
FROM trips t
JOIN events e ON t.trip_id = e.trip_id
WHERE e.event_type = 'ERROR'
ORDER BY t.trip_id;
"""
# Alternative using EXISTS (often faster — stops at first match):
Q4_EXISTS = """
SELECT
    t.trip_id,
    t.user_id,
    t.start_time,
    t.end_time,
    t.total_cost
FROM trips t
WHERE EXISTS (
    SELECT 1 FROM events e
    WHERE e.trip_id = t.trip_id
      AND e.event_type = 'ERROR'
)
ORDER BY t.trip_id;
"""


def run_all_queries(verbose: bool = False):
    conn = connect()
    cur  = conn.cursor()

    print("\n=== PostgreSQL Queries ===\n")
    r1, t1 = timed_query(cur, Q1,         "Q1 - Trips with user & stations")
    r2, t2 = timed_query(cur, Q2,         "Q2 - Users with trip stats      ")
    r3, t3 = timed_query(cur, Q3,         "Q3 - Stations with trip counts  ")
    r4, t4 = timed_query(cur, Q4_EXISTS,  "Q4 - Trips with ERROR (EXISTS)  ")

    if verbose:
        print("\n--- Sample Q1 (first 3 rows) ---")
        for row in r1[:3]:
            print(row)
        print("\n--- Sample Q2 (first 3 rows) ---")
        for row in r2[:3]:
            print(row)

    cur.close()
    conn.close()
    return {"Q1": t1, "Q2": t2, "Q3": t3, "Q4": t4}


if __name__ == "__main__":
    run_all_queries(verbose=True)
