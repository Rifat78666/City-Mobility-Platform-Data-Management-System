"""
Schema Evolution (Part 1.3) - MongoDB
=====================================
Requirement: all events of type BATTERY must include a new field
"battery_level" (an integer in the range [0, 100]).

In the document model, events are EMBEDDED inside trip documents and
MongoDB has a flexible schema. Adding the field therefore requires:
  1. updating the collection validator (one line), and
  2. simply including the field on new BATTERY events.
No migration of existing documents is required - this is the core
evolvability advantage over the relational model.

Run with:
    python Schema_Evolution/mongodb_add_battery_level.py
"""

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB  = "adm_mobility"


def update_validator(db):
    """
    Update the trips collection validator so that embedded events may
    optionally carry a battery_level field (int, 0-100). Existing
    documents remain valid - nothing is migrated.
    """
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
                            "timestamp":  {"bsonType": "date"},
                            "event_type": {"enum": ["GPS", "ERROR", "BATTERY", "DELAY"]},
                            "value":      {"bsonType": "string"},
                            # NEW optional field - only meaningful for BATTERY events
                            "battery_level": {
                                "bsonType": "int",
                                "minimum": 0,
                                "maximum": 100,
                            },
                        },
                    },
                },
            },
        }
    }
    db.command("collMod", "trips", validator=trips_validator)
    print("[MongoDB] Validator updated: battery_level is now allowed on events.")


def backfill_existing_battery_events(db):
    """
    OPTIONAL: demonstrate how easy it is to add battery_level to the
    embedded BATTERY events that already exist. We derive the value
    from the event's existing 'value' string (which already stores a
    0-100 number for BATTERY events in our generator).

    This is a single update - no table rewrite, no column on unrelated
    rows. Non-BATTERY events are never touched.
    """
    result = db.trips.update_many(
        {"events.event_type": "BATTERY"},
        [
            {
                "$set": {
                    "events": {
                        "$map": {
                            "input": "$events",
                            "as": "e",
                            "in": {
                                "$cond": [
                                    {"$eq": ["$$e.event_type", "BATTERY"]},
                                    {
                                        "$mergeObjects": [
                                            "$$e",
                                            {"battery_level": {"$toInt": "$$e.value"}},
                                        ]
                                    },
                                    "$$e",
                                ]
                            },
                        }
                    }
                }
            }
        ],
    )
    print(f"[MongoDB] Backfilled battery_level on {result.modified_count} trips "
          f"that contained BATTERY events.")


def show_sample(db):
    """Print one BATTERY event to confirm the new field is present."""
    doc = db.trips.find_one({"events.event_type": "BATTERY"})
    if not doc:
        print("[MongoDB] No BATTERY events found in current data.")
        return
    for e in doc["events"]:
        if e["event_type"] == "BATTERY":
            print(f"[MongoDB] Sample BATTERY event: value={e.get('value')}, "
                  f"battery_level={e.get('battery_level')}")
            break


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    print("=== MongoDB Schema Evolution: add battery_level ===")
    update_validator(db)
    backfill_existing_battery_events(db)
    show_sample(db)
    print("Done. New BATTERY events can now include a battery_level field (0-100).")

    client.close()


if __name__ == "__main__":
    main()
