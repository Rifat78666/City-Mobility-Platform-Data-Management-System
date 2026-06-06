"""
Advanced Data Management Project
MongoDB Schema Definition & Collection Setup
=============================================

Document schema design rationale:
- Users and Stations are REFERENCED (not embedded) because:
    * They are large collections queried independently
    * Many trips share the same user/station
    * Embedding would cause massive duplication
- Events are EMBEDDED inside each Trip document because:
    * Events have no independent meaning outside a trip
    * Queries on events always go through a trip
    * Embedding avoids expensive joins (lookups) and
      matches the natural one-to-many lifecycle of events
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid

# ── Connection ────────────────────────────────────────────────
client = MongoClient("mongodb://localhost:27017/")
db = client["adm_mobility"]

# ── Validators ────────────────────────────────────────────────

users_validator = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["name", "surname", "birthdate", "country"],
        "properties": {
            "name":      {"bsonType": "string"},
            "surname":   {"bsonType": "string"},
            "birthdate": {"bsonType": "date"},
            "country":   {"bsonType": "string"},
        },
    }
}

stations_validator = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["name", "city", "capacity"],
        "properties": {
            "name":     {"bsonType": "string"},
            "city":     {"bsonType": "string"},
            "capacity": {"bsonType": "int", "minimum": 1},
        },
    }
}

trips_validator = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "start_station_id", "end_station_id",
                     "start_time", "end_time", "total_cost", "events"],
        "properties": {
            "user_id":          {"bsonType": "objectId"},
            "start_station_id": {"bsonType": "objectId"},
            "end_station_id":   {"bsonType": "objectId"},
            "start_time":       {"bsonType": "date"},
            "end_time":         {"bsonType": "date"},
            "total_cost":       {"bsonType": ["double", "decimal"]},
            "events": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["timestamp", "event_type", "value"],
                    "properties": {
                        "timestamp":   {"bsonType": "date"},
                        "event_type":  {"enum": ["GPS", "ERROR", "BATTERY", "DELAY"]},
                        "value":       {"bsonType": "string"},
                        # Optional field — only for BATTERY events (Part 1.3)
                        "battery_level": {"bsonType": "int", "minimum": 0, "maximum": 100},
                    },
                },
            },
        },
    }
}


def create_collections():
    """Create collections with schema validators and indexes."""

    # ── users ─────────────────────────────────────────────────
    try:
        db.create_collection("users", validator=users_validator)
        print("Collection 'users' created.")
    except CollectionInvalid:
        db.command("collMod", "users", validator=users_validator)
        print("Collection 'users' already exists — validator updated.")

    db.users.create_index([("surname", ASCENDING), ("name", ASCENDING)])
    db.users.create_index([("country", ASCENDING)])

    # ── stations ──────────────────────────────────────────────
    try:
        db.create_collection("stations", validator=stations_validator)
        print("Collection 'stations' created.")
    except CollectionInvalid:
        db.command("collMod", "stations", validator=stations_validator)
        print("Collection 'stations' already exists — validator updated.")

    db.stations.create_index([("city", ASCENDING)])

    # ── trips (events embedded) ───────────────────────────────
    try:
        db.create_collection("trips", validator=trips_validator)
        print("Collection 'trips' created.")
    except CollectionInvalid:
        db.command("collMod", "trips", validator=trips_validator)
        print("Collection 'trips' already exists — validator updated.")

    db.trips.create_index([("user_id", ASCENDING)])
    db.trips.create_index([("start_station_id", ASCENDING)])
    db.trips.create_index([("end_station_id", ASCENDING)])
    db.trips.create_index([("events.event_type", ASCENDING)])   # for Q4
    db.trips.create_index([("start_time", DESCENDING)])

    print("\nAll collections and indexes created successfully.")


if __name__ == "__main__":
    create_collections()
    client.close()
