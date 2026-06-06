"""
Advanced Data Management Project
Part 2.1 — Neo4j Query Scalability Benchmark
=============================================
Tests Q1 and Q2 at all required scale combinations.
Reuses generate_mongo() to populate data, then reloads into Neo4j.
"""

import csv
import os
import sys
import time
import itertools

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pymongo import MongoClient
from neo4j import GraphDatabase
from utils.generate_data import generate_mongo

NEO4J_URI  = "bolt://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Rifat@786"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB  = "adm_mobility"

USERS_SIZES  = [1_000, 10_000, 50_000]
TRIPS_SIZES  = [10_000, 50_000, 100_000]
EVENTS_SIZES = [0, 2, 5, 10]

OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "neo4j_benchmark.csv")


def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


def reload_neo4j(driver):
    """Clear graph and reload from current MongoDB data."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    with driver.session() as session:
        # Clear
        session.run("MATCH (n) DETACH DELETE n")

        # Load users
        users = [{"uid": str(u["_id"]), "name": u["name"], "surname": u["surname"]}
                 for u in db.users.find()]
        session.run("UNWIND $b AS u MERGE (n:USER {user_id:u.uid}) SET n.name=u.name, n.surname=u.surname",
                    b=users)

        # Load stations
        stations = [{"sid": str(s["_id"]), "name": s["name"], "city": s["city"]}
                    for s in db.stations.find()]
        session.run("UNWIND $b AS s MERGE (n:STATION {station_id:s.sid}) SET n.name=s.name, n.city=s.city",
                    b=stations)

        # Load trips in batches
        BATCH = 500
        skip = 0
        while True:
            trips = list(db.trips.find().skip(skip).limit(BATCH))
            if not trips:
                break
            batch = [{"tid": str(t["_id"]), "uid": str(t["user_id"]),
                      "ssid": str(t["start_station_id"]), "esid": str(t["end_station_id"])}
                     for t in trips]
            session.run("""
                UNWIND $b AS t
                MERGE (trip:TRIP {trip_id:t.tid})
                WITH trip, t
                MATCH (u:USER {user_id:t.uid})
                MATCH (ss:STATION {station_id:t.ssid})
                MATCH (es:STATION {station_id:t.esid})
                MERGE (u)-[:PERFORMED]->(trip)
                MERGE (trip)-[:STARTS_AT]->(ss)
                MERGE (trip)-[:ENDS_AT]->(es)
            """, b=batch)
            skip += BATCH

    client.close()


Q1 = """
MATCH (u:USER)-[:PERFORMED]->(t:TRIP)-[:STARTS_AT|ENDS_AT]->(s:STATION)
WITH u LIMIT 1
MATCH (u)-[:PERFORMED]->(t:TRIP)-[:STARTS_AT|ENDS_AT]->(s:STATION)
RETURN DISTINCT s.station_id AS sid, s.name AS name
"""

Q2 = """
MATCH (s:STATION)
OPTIONAL MATCH (s)<-[:STARTS_AT]-(t_out:TRIP)
OPTIONAL MATCH (s)<-[:ENDS_AT]-(t_in:TRIP)
WITH s, COUNT(DISTINCT t_out) AS out, COUNT(DISTINCT t_in) AS inn
RETURN s.station_id, s.name, out+inn AS total
ORDER BY total DESC LIMIT 3
"""


def run_benchmark():
    driver = get_driver()
    combos = list(itertools.product(USERS_SIZES, TRIPS_SIZES, EVENTS_SIZES))
    fieldnames = ["users", "trips", "events_per_trip", "Q1_s", "Q2_s"]

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, (n_users, n_trips, n_events) in enumerate(combos, 1):
            print(f"\n[{i}/{len(combos)}] users={n_users}, trips={n_trips}, events={n_events}")

            print("  Generating MongoDB data...")
            generate_mongo(n_users, n_trips, n_events)

            print("  Loading into Neo4j...")
            reload_neo4j(driver)

            print("  Running Neo4j queries...")
            with driver.session() as session:
                t1 = time.perf_counter()
                session.run(Q1).data()
                q1_time = round(time.perf_counter() - t1, 4)

                t2 = time.perf_counter()
                session.run(Q2).data()
                q2_time = round(time.perf_counter() - t2, 4)

            print(f"  Q1={q1_time}s  Q2={q2_time}s")
            writer.writerow({"users": n_users, "trips": n_trips,
                             "events_per_trip": n_events,
                             "Q1_s": q1_time, "Q2_s": q2_time})
            f.flush()

    driver.close()
    print(f"\nDone! Results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    run_benchmark()