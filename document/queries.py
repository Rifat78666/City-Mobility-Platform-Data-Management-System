"""
Advanced Data Management Project
Part 1.2 — Query Implementation: MongoDB
=========================================
Queries:
  Q1 - All trips with user info, start/end station names
  Q2 - All users with number of trips and average trip duration
  Q3 - All stations with trips starting/ending there
  Q4 - All trips containing at least one ERROR event
"""

import time
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB  = "adm_mobility"


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB], client


def timed_pipeline(collection, pipeline: list, label: str):
    start = time.perf_counter()
    results = list(collection.aggregate(pipeline, allowDiskUse=True))
    elapsed = time.perf_counter() - start
    print(f"[{label}] docs={len(results)}, time={elapsed:.4f}s")
    return results, elapsed


# ────────────────────────────────────────────────────────────────
# Q1 — All trips with user info and station names
# Uses $lookup to join users and stations (referenced documents)
# ────────────────────────────────────────────────────────────────
Q1_PIPELINE = [
    # Join user
    {"$lookup": {
        "from":         "users",
        "localField":   "user_id",
        "foreignField": "_id",
        "as":           "user",
    }},
    {"$unwind": "$user"},
    # Join start station
    {"$lookup": {
        "from":         "stations",
        "localField":   "start_station_id",
        "foreignField": "_id",
        "as":           "start_station",
    }},
    {"$unwind": "$start_station"},
    # Join end station
    {"$lookup": {
        "from":         "stations",
        "localField":   "end_station_id",
        "foreignField": "_id",
        "as":           "end_station",
    }},
    {"$unwind": "$end_station"},
    # Project output fields
    {"$project": {
        "trip_id":           "$_id",
        "user_id":           "$user._id",
        "user_name":         "$user.name",
        "user_surname":      "$user.surname",
        "user_birthdate":    "$user.birthdate",
        "user_country":      "$user.country",
        "start_station_name":"$start_station.name",
        "end_station_name":  "$end_station.name",
        "start_time":        1,
        "end_time":          1,
        "total_cost":        1,
    }},
    {"$sort": {"trip_id": 1}},
]

# ────────────────────────────────────────────────────────────────
# Q2 — Users with trip count and average duration
# Start from users collection and lookup trips
# ────────────────────────────────────────────────────────────────
Q2_PIPELINE = [
    # For each user, lookup their trips
    {"$lookup": {
        "from":         "trips",
        "localField":   "_id",
        "foreignField": "user_id",
        "as":           "trips",
    }},
    # Compute stats
    {"$project": {
        "name":    1,
        "surname": 1,
        "num_trips": {"$size": "$trips"},
        "avg_duration_minutes": {
            "$cond": [
                {"$gt": [{"$size": "$trips"}, 0]},
                {
                    "$divide": [
                        {
                            "$avg": {
                                "$map": {
                                    "input": "$trips",
                                    "as":    "t",
                                    "in": {
                                        "$divide": [
                                            {"$subtract": ["$$t.end_time", "$$t.start_time"]},
                                            60000,  # ms → minutes
                                        ]
                                    },
                                }
                            }
                        },
                        1,
                    ]
                },
                0,
            ]
        },
    }},
    {"$sort": {"_id": 1}},
]

# ────────────────────────────────────────────────────────────────
# Q3 — Stations with departing and arriving trip counts
# ────────────────────────────────────────────────────────────────
Q3_PIPELINE = [
    {"$facet": {
        "starting": [
            {"$group": {"_id": "$start_station_id", "count": {"$sum": 1}}}
        ],
        "ending": [
            {"$group": {"_id": "$end_station_id", "count": {"$sum": 1}}}
        ],
    }},
    # Merge both facets into arrays we can lookup against stations
    {"$project": {
        "all": {"$concatArrays": [
            {"$map": {"input": "$starting", "as": "s",
                      "in": {"station_id": "$$s._id", "role": "start", "count": "$$s.count"}}},
            {"$map": {"input": "$ending",   "as": "e",
                      "in": {"station_id": "$$e._id", "role": "end",   "count": "$$e.count"}}},
        ]}
    }},
    {"$unwind": "$all"},
    {"$group": {
        "_id": "$all.station_id",
        "trips_starting": {"$sum": {"$cond": [{"$eq": ["$all.role", "start"]}, "$all.count", 0]}},
        "trips_ending":   {"$sum": {"$cond": [{"$eq": ["$all.role", "end"]},   "$all.count", 0]}},
    }},
    {"$lookup": {
        "from":         "stations",
        "localField":   "_id",
        "foreignField": "_id",
        "as":           "station",
    }},
    {"$unwind": "$station"},
    {"$project": {
        "station_name":   "$station.name",
        "city":           "$station.city",
        "trips_starting": 1,
        "trips_ending":   1,
    }},
    {"$sort": {"_id": 1}},
]

# Alternative simpler Q3 (two passes — clearer but two aggregations):
def q3_two_pass(db):
    """Two-pass approach: one for start, one for end, merge in Python."""
    start_counts = {
        d["_id"]: d["count"]
        for d in db.trips.aggregate([
            {"$group": {"_id": "$start_station_id", "count": {"$sum": 1}}}
        ])
    }
    end_counts = {
        d["_id"]: d["count"]
        for d in db.trips.aggregate([
            {"$group": {"_id": "$end_station_id", "count": {"$sum": 1}}}
        ])
    }
    result = []
    for station in db.stations.find():
        sid = station["_id"]
        result.append({
            "station_id":     sid,
            "name":           station["name"],
            "city":           station["city"],
            "trips_starting": start_counts.get(sid, 0),
            "trips_ending":   end_counts.get(sid, 0),
        })
    return result


# ────────────────────────────────────────────────────────────────
# Q4 — Trips with at least one ERROR event (embedded array filter)
# ────────────────────────────────────────────────────────────────
Q4_PIPELINE = [
    # Filter trips that have at least one embedded event with type ERROR
    {"$match": {"events.event_type": "ERROR"}},
    {"$project": {
        "trip_id":    "$_id",
        "user_id":    1,
        "start_time": 1,
        "end_time":   1,
        "total_cost": 1,
    }},
    {"$sort": {"trip_id": 1}},
]


def run_all_queries(verbose: bool = False):
    db, client = get_db()

    print("\n=== MongoDB Queries ===\n")
    r1, t1 = timed_pipeline(db.trips,  Q1_PIPELINE, "Q1 - Trips with user & stations")
    r2, t2 = timed_pipeline(db.users,  Q2_PIPELINE, "Q2 - Users with trip stats      ")
    r3, t3 = timed_pipeline(db.trips,  Q3_PIPELINE, "Q3 - Stations with trip counts  ")
    r4, t4 = timed_pipeline(db.trips,  Q4_PIPELINE, "Q4 - Trips with ERROR           ")

    if verbose:
        print("\n--- Sample Q1 (first 2 docs) ---")
        for doc in r1[:2]:
            print({k: str(v) for k, v in doc.items()})
        print("\n--- Sample Q4 (first 2 docs) ---")
        for doc in r4[:2]:
            print(doc)

    client.close()
    return {"Q1": t1, "Q2": t2, "Q3": t3, "Q4": t4}


if __name__ == "__main__":
    run_all_queries(verbose=True)
