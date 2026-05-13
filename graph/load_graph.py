"""
Advanced Data Management Project
Part 2 — Graph Model: Neo4j
============================
Node types:   USERS, TRIPS, STATIONS
Edge types:   PERFORMED (user→trip), STARTS_AT (trip→station), ENDS_AT (trip→station)

This script:
  1. Clears the graph
  2. Creates constraints and indexes
  3. Loads data from MongoDB into Neo4j
"""

import time
from pymongo import MongoClient
from neo4j import GraphDatabase

# ── Config ────────────────────────────────────────────────────
MONGO_URI  = "mongodb://localhost:27017/"
MONGO_DB   = "adm_mobility"

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Rifat@786"   # ← your Neo4j password


def get_mongo():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB], client


def get_neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


# ── Setup constraints & indexes ───────────────────────────────

SETUP_QUERIES = [
    # Clear everything
    "MATCH (n) DETACH DELETE n",

    # Uniqueness constraints (also create indexes automatically)
    "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:USER) REQUIRE u.user_id IS UNIQUE",
    "CREATE CONSTRAINT trip_id IF NOT EXISTS FOR (t:TRIP) REQUIRE t.trip_id IS UNIQUE",
    "CREATE CONSTRAINT station_id IF NOT EXISTS FOR (s:STATION) REQUIRE s.station_id IS UNIQUE",
]


def setup_graph(driver):
    print("[Neo4j] Setting up constraints and clearing graph...")
    with driver.session() as session:
        for q in SETUP_QUERIES:
            session.run(q)
    print("[Neo4j] Setup complete.")


# ── Load data from MongoDB ────────────────────────────────────

def load_users(session, db):
    print("[Neo4j] Loading USERS...")
    users = list(db.users.find())
    batch = [
        {
            "user_id": str(u["_id"]),
            "name":    u["name"],
            "surname": u["surname"],
            "country": u["country"],
        }
        for u in users
    ]
    session.run("""
        UNWIND $batch AS u
        MERGE (n:USER {user_id: u.user_id})
        SET n.name    = u.name,
            n.surname = u.surname,
            n.country = u.country
    """, batch=batch)
    print(f"[Neo4j] {len(batch)} users loaded.")


def load_stations(session, db):
    print("[Neo4j] Loading STATIONS...")
    stations = list(db.stations.find())
    batch = [
        {
            "station_id": str(s["_id"]),
            "name":       s["name"],
            "city":       s["city"],
            "capacity":   s["capacity"],
        }
        for s in stations
    ]
    session.run("""
        UNWIND $batch AS s
        MERGE (n:STATION {station_id: s.station_id})
        SET n.name     = s.name,
            n.city     = s.city,
            n.capacity = s.capacity
    """, batch=batch)
    print(f"[Neo4j] {len(batch)} stations loaded.")


def load_trips(session, db, skip=0, limit=1000):
    """Load trips in batches to avoid memory issues."""
    trips = list(db.trips.find().skip(skip).limit(limit))
    if not trips:
        return 0

    batch = [
        {
            "trip_id":          str(t["_id"]),
            "user_id":          str(t["user_id"]),
            "start_station_id": str(t["start_station_id"]),
            "end_station_id":   str(t["end_station_id"]),
            "total_cost":       float(t["total_cost"]),
        }
        for t in trips
    ]

    session.run("""
        UNWIND $batch AS t
        MERGE (trip:TRIP {trip_id: t.trip_id})
        SET trip.total_cost = t.total_cost

        WITH trip, t
        MATCH (u:USER    {user_id:    t.user_id})
        MATCH (ss:STATION {station_id: t.start_station_id})
        MATCH (es:STATION {station_id: t.end_station_id})

        MERGE (u)-[:PERFORMED]->(trip)
        MERGE (trip)-[:STARTS_AT]->(ss)
        MERGE (trip)-[:ENDS_AT]->(es)
    """, batch=batch)

    return len(trips)


def load_all_trips(session, db):
    print("[Neo4j] Loading TRIPS (in batches of 500)...")
    total = 0
    skip  = 0
    BATCH = 500
    while True:
        n = load_trips(session, db, skip=skip, limit=BATCH)
        if n == 0:
            break
        total += n
        skip  += BATCH
        print(f"  Loaded {total} trips so far...")
    print(f"[Neo4j] {total} trips loaded.")


def load_graph():
    db, mongo_client = get_mongo()
    driver = get_neo4j()

    setup_graph(driver)

    with driver.session() as session:
        load_users(session, db)
        load_stations(session, db)
        load_all_trips(session, db)

    print("\n[Neo4j] Graph loaded successfully!")
    mongo_client.close()
    driver.close()


if __name__ == "__main__":
    load_graph()
