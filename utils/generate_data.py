"""
Advanced Data Management Project
Data Generator — PostgreSQL & MongoDB
======================================
Generates synthetic data for scalability benchmarks.

Usage:
    python generate_data.py --users 1000 --trips 10000 --events 2 --target both
"""

import argparse
import random
import string
from datetime import datetime, timedelta

import pg8000
from pymongo import MongoClient
from bson import ObjectId

# ── Configuration ─────────────────────────────────────────────
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "adm_mobility",
    "user": "postgres",
    "password": "Rifat@786",
}

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB  = "adm_mobility"

# ── Constants ─────────────────────────────────────────────────
ITALIAN_CITIES = [
    "Milano", "Roma", "Napoli", "Torino", "Bologna",
    "Firenze", "Venezia", "Genova", "Palermo", "Bari",
    "Catania", "Verona", "Padova", "Trieste", "Brescia",
]
COUNTRIES = ["Italy", "Germany", "France", "Spain", "UK", "USA", "Japan", "Brazil"]
EVENT_TYPES = ["GPS", "ERROR", "BATTERY", "DELAY"]
NAMES    = ["Luca", "Marco", "Sara", "Giulia", "Andrea", "Matteo", "Anna", "Elena",
            "Francesco", "Davide", "Alice", "Chiara", "Lorenzo", "Riccardo", "Valeria"]
SURNAMES = ["Rossi", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo",
            "Ricci", "Marino", "Greco", "Bruno", "Gallo", "Conti", "De Luca"]

BASE_DATE = datetime(2023, 1, 1)


def rand_date(start: datetime, days: int = 365) -> datetime:
    return start + timedelta(
        days=random.randint(0, days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


def rand_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def make_event_value(etype: str) -> str:
    if etype == "GPS":
        lat = round(random.uniform(37.0, 47.0), 6)
        lon = round(random.uniform(7.0, 15.0), 6)
        return f"{lat},{lon}"
    elif etype == "ERROR":
        return random.choice(["E001:BrakeFailure", "E002:MotorOverheat",
                              "E003:BatteryLow", "E004:ConnectionLost"])
    elif etype == "BATTERY":
        return str(random.randint(0, 100))
    else:  # DELAY
        return str(random.randint(1, 60))


# ════════════════════════════════════════════════════════════════
# PostgreSQL generation
# ════════════════════════════════════════════════════════════════

def generate_postgres(n_users: int, n_trips: int, events_per_trip: int):
    conn = pg8000.connect(**PG_CONFIG)
    cur  = conn.cursor()

    print(f"[PG] Clearing existing data...")
    cur.execute("DELETE FROM events")
    cur.execute("DELETE FROM trips")
    cur.execute("DELETE FROM stations")
    cur.execute("DELETE FROM users")
    cur.execute("ALTER SEQUENCE users_user_id_seq RESTART WITH 1")
    cur.execute("ALTER SEQUENCE stations_station_id_seq RESTART WITH 1")
    cur.execute("ALTER SEQUENCE trips_trip_id_seq RESTART WITH 1")
    cur.execute("ALTER SEQUENCE events_event_id_seq RESTART WITH 1")
    conn.commit()

    # ── Users ──────────────────────────────────────────────────
    print(f"[PG] Inserting {n_users} users...")
    users_data = []
    for _ in range(n_users):
        bd = BASE_DATE - timedelta(days=random.randint(18*365, 60*365))
        users_data.append((
            random.choice(NAMES),
            random.choice(SURNAMES),
            bd.date(),
            random.choice(COUNTRIES),
        ))
    cur.executemany(
        "INSERT INTO users (name, surname, birthdate, country) VALUES (%s, %s, %s, %s)",
        users_data,
    )
    conn.commit()

    # ── Stations ───────────────────────────────────────────────
    n_stations = max(20, n_users // 50)
    print(f"[PG] Inserting {n_stations} stations...")
    stations_data = []
    for i in range(n_stations):
        city = random.choice(ITALIAN_CITIES)
        stations_data.append((
            f"Station_{city}_{i:04d}",
            city,
            random.randint(5, 50),
        ))
    cur.executemany(
        "INSERT INTO stations (name, city, capacity) VALUES (%s, %s, %s)",
        stations_data,
    )
    conn.commit()

    # Get IDs
    cur.execute("SELECT user_id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT station_id FROM stations")
    station_ids = [r[0] for r in cur.fetchall()]

    # ── Trips + Events ─────────────────────────────────────────
    print(f"[PG] Inserting {n_trips} trips ({events_per_trip} events each)...")
    BATCH = 500
    for batch_start in range(0, n_trips, BATCH):
        trips_batch  = []
        events_batch = []
        for _ in range(min(BATCH, n_trips - batch_start)):
            uid    = random.choice(user_ids)
            sid    = random.choice(station_ids)
            eid    = random.choice(station_ids)
            st     = rand_date(BASE_DATE)
            et     = st + timedelta(minutes=random.randint(5, 180))
            cost   = round(random.uniform(0.5, 25.0), 2)
            trips_batch.append((uid, sid, eid, st, et, cost))

        cur.executemany(
            """INSERT INTO trips
               (user_id, start_station, end_station, start_time, end_time, total_cost)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            trips_batch,
        )
        conn.commit()

        if events_per_trip > 0:
            cur.execute(
                "SELECT trip_id FROM trips ORDER BY trip_id DESC LIMIT %s",
                (len(trips_batch),),
            )
            trip_ids = [r[0] for r in cur.fetchall()]
            for tid in trip_ids:
                for _ in range(events_per_trip):
                    etype = random.choice(EVENT_TYPES)
                    ts    = rand_date(BASE_DATE)
                    events_batch.append((tid, ts, etype, make_event_value(etype)))
            cur.executemany(
                "INSERT INTO events (trip_id, timestamp, event_type, value) VALUES (%s,%s,%s,%s)",
                events_batch,
            )
            conn.commit()

    cur.close()
    conn.close()
    print("[PG] Done.")


# ════════════════════════════════════════════════════════════════
# MongoDB generation
# ════════════════════════════════════════════════════════════════

def generate_mongo(n_users: int, n_trips: int, events_per_trip: int):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    print(f"[MongoDB] Clearing existing data...")
    db.users.drop()
    db.stations.drop()
    db.trips.drop()

    # ── Users ──────────────────────────────────────────────────
    print(f"[MongoDB] Inserting {n_users} users...")
    users_docs = []
    for _ in range(n_users):
        bd = BASE_DATE - timedelta(days=random.randint(18*365, 60*365))
        users_docs.append({
            "name":      random.choice(NAMES),
            "surname":   random.choice(SURNAMES),
            "birthdate": bd,
            "country":   random.choice(COUNTRIES),
        })
    user_ids = db.users.insert_many(users_docs).inserted_ids

    # ── Stations ───────────────────────────────────────────────
    n_stations = max(20, n_users // 50)
    print(f"[MongoDB] Inserting {n_stations} stations...")
    stations_docs = []
    for i in range(n_stations):
        city = random.choice(ITALIAN_CITIES)
        stations_docs.append({
            "name":     f"Station_{city}_{i:04d}",
            "city":     city,
            "capacity": random.randint(5, 50),
        })
    station_ids = db.stations.insert_many(stations_docs).inserted_ids

    # ── Trips (with embedded events) ───────────────────────────
    print(f"[MongoDB] Inserting {n_trips} trips ({events_per_trip} events each)...")
    BATCH = 500
    for batch_start in range(0, n_trips, BATCH):
        trips_batch = []
        for _ in range(min(BATCH, n_trips - batch_start)):
            uid  = random.choice(user_ids)
            ssid = random.choice(station_ids)
            esid = random.choice(station_ids)
            st   = rand_date(BASE_DATE)
            et   = st + timedelta(minutes=random.randint(5, 180))

            embedded_events = []
            for _ in range(events_per_trip):
                etype = random.choice(EVENT_TYPES)
                embedded_events.append({
                    "timestamp":  rand_date(st, days=0),
                    "event_type": etype,
                    "value":      make_event_value(etype),
                })

            trips_batch.append({
                "user_id":          uid,
                "start_station_id": ssid,
                "end_station_id":   esid,
                "start_time":       st,
                "end_time":         et,
                "total_cost":       round(random.uniform(0.5, 25.0), 2),
                "events":           embedded_events,
            })
        db.trips.insert_many(trips_batch)

    # Rebuild indexes after drop
    db.trips.create_index([("user_id", 1)])
    db.trips.create_index([("start_station_id", 1)])
    db.trips.create_index([("end_station_id", 1)])
    db.trips.create_index([("events.event_type", 1)])

    client.close()
    print("[MongoDB] Done.")


# ════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ADM Project Data Generator")
    parser.add_argument("--users",  type=int, default=1000,  help="Number of users")
    parser.add_argument("--trips",  type=int, default=10000, help="Number of trips")
    parser.add_argument("--events", type=int, default=2,     help="Events per trip")
    parser.add_argument("--target", choices=["pg", "mongo", "both"], default="both",
                        help="Target database(s)")
    args = parser.parse_args()

    print(f"\nGenerating: users={args.users}, trips={args.trips}, events/trip={args.events}")
    print(f"Target: {args.target}\n")

    if args.target in ("pg", "both"):
        generate_postgres(args.users, args.trips, args.events)
    if args.target in ("mongo", "both"):
        generate_mongo(args.users, args.trips, args.events)

    print("\nData generation complete.")
