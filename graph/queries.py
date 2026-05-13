"""
Advanced Data Management Project
Part 2.1 — Graph Queries: Neo4j (Cypher)
=========================================
Q1 - Given a user, find all stations reachable through their trips
Q2 - Find the 3 most important stations by number of incoming + outgoing trips
"""

import time
from neo4j import GraphDatabase

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Rifat@786"


def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


def timed_query(session, cypher, params, label):
    start   = time.perf_counter()
    result  = session.run(cypher, **params)
    records = result.data()
    elapsed = time.perf_counter() - start
    print(f"[{label}] records={len(records)}, time={elapsed:.4f}s")
    return records, elapsed


# ─────────────────────────────────────────────────────────────
# Q1 — All stations reachable by a given user through their trips
# Pattern: (user)-[:PERFORMED]->(trip)-[:STARTS_AT|ENDS_AT]->(station)
# ─────────────────────────────────────────────────────────────
Q1_CYPHER = """
MATCH (u:USER {user_id: $user_id})-[:PERFORMED]->(t:TRIP)-[:STARTS_AT|ENDS_AT]->(s:STATION)
RETURN DISTINCT s.station_id AS station_id,
                s.name       AS station_name,
                s.city       AS city
ORDER BY s.name
"""

# ─────────────────────────────────────────────────────────────
# Q2 — Top 3 stations by total trip count (incoming + outgoing)
# ─────────────────────────────────────────────────────────────
Q2_CYPHER = """
MATCH (s:STATION)
OPTIONAL MATCH (s)<-[:STARTS_AT]-(t_out:TRIP)
OPTIONAL MATCH (s)<-[:ENDS_AT]-(t_in:TRIP)
WITH s,
     COUNT(DISTINCT t_out) AS outgoing_trips,
     COUNT(DISTINCT t_in)  AS incoming_trips
RETURN s.station_id                        AS station_id,
       s.name                              AS station_name,
       s.city                              AS city,
       outgoing_trips,
       incoming_trips,
       outgoing_trips + incoming_trips     AS total_trips
ORDER BY total_trips DESC
LIMIT 3
"""


def run_graph_queries():
    driver = get_driver()

    print("\n=== Neo4j Graph Queries ===\n")

    with driver.session() as session:

        # ── Q1: pick the first user in the graph ──────────────
        first_user = session.run(
            "MATCH (u:USER) RETURN u.user_id AS uid LIMIT 1"
        ).single()

        if not first_user:
            print("No users found in graph. Run load_graph.py first.")
            driver.close()
            return

        user_id = first_user["uid"]
        print(f"Running Q1 for user_id: {user_id}\n")

        r1, t1 = timed_query(
            session, Q1_CYPHER,
            {"user_id": user_id},
            "Q1 - Stations reachable by user"
        )

        r2, t2 = timed_query(
            session, Q2_CYPHER,
            {},
            "Q2 - Top 3 stations by trip count"
        )

        print("\n--- Q1 Results (stations visited by user) ---")
        for r in r1[:5]:
            print(f"  {r['station_name']} ({r['city']})")

        print("\n--- Q2 Results (top 3 stations) ---")
        for r in r2:
            print(f"  {r['station_name']} | in={r['incoming_trips']} "
                  f"out={r['outgoing_trips']} total={r['total_trips']}")

    driver.close()
    return {"Q1": t1, "Q2": t2}


if __name__ == "__main__":
    run_graph_queries()
