"""
Advanced Data Management Project
Part 1.4 — Spark-based Implementation of Query 2
=================================================
Query 2: For each user, compute:
  - number of trips performed
  - average trip duration (in minutes)

Two Spark implementations:
  (A) RDD-based
  (B) DataFrame-based
"""

import time
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB  = "adm_mobility"


def get_spark():
    return (
        SparkSession.builder
        .appName("ADM_Query2_Spark")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.python.worker.reuse", "false")
        .getOrCreate()
    )


def load_data_from_mongo():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    print("[Spark] Loading data from MongoDB...")

    # Load as simple tuples — easier to serialize
    users = [
        (str(u["_id"]), u["name"], u["surname"])
        for u in db.users.find()
    ]
    trips = [
        (
            str(t["user_id"]),
            float((t["end_time"] - t["start_time"]).total_seconds() / 60.0)
        )
        for t in db.trips.find()
    ]
    client.close()
    print(f"[Spark] Loaded {len(users)} users and {len(trips)} trips.")
    return users, trips


# ════════════════════════════════════════════════════════════════
# (A) RDD-based implementation
# ════════════════════════════════════════════════════════════════

def query2_rdd(spark, trips):
    print("\n[RDD] Running Query 2...")
    start = time.perf_counter()

    # trips is a list of (user_id, duration_min) tuples
    trips_rdd = spark.sparkContext.parallelize(trips)

    # map to (user_id, (count, duration))
    mapped = trips_rdd.map(lambda x: (x[0], (1, x[1])))

    # reduce by key
    reduced = mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # compute average
    result = reduced.map(lambda x: (x[0], x[1][0], round(x[1][1] / x[1][0], 2))).collect()

    elapsed = time.perf_counter() - start
    print(f"[RDD] users={len(result)}, time={elapsed:.4f}s")
    print("Sample (first 3):")
    for uid, n, avg in result[:3]:
        print(f"  user={uid[:8]}... trips={n} avg_duration={avg}min")
    return result, elapsed


# ════════════════════════════════════════════════════════════════
# (B) DataFrame-based implementation
# ════════════════════════════════════════════════════════════════

def query2_dataframe(spark, users, trips):
    print("\n[DataFrame] Running Query 2...")
    start = time.perf_counter()

    trips_df = spark.createDataFrame(trips, ["user_id", "duration_min"])
    users_df = spark.createDataFrame(users, ["user_id", "name", "surname"])

    agg_df = trips_df.groupBy("user_id").agg(
        F.count("*").alias("num_trips"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
    )

    result_df = (
        users_df.join(agg_df, on="user_id", how="left")
        .fillna({"num_trips": 0, "avg_duration_min": 0.0})
        .orderBy("user_id")
    )

    results = result_df.collect()
    elapsed = time.perf_counter() - start
    print(f"[DataFrame] users={len(results)}, time={elapsed:.4f}s")
    print("Sample (first 3):")
    for row in results[:3]:
        print(f"  {row['name']} {row['surname']} | trips={row['num_trips']} avg={row['avg_duration_min']}min")
    return results, elapsed


# ════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════

def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    users, trips = load_data_from_mongo()

    _, t_rdd = query2_rdd(spark, trips)
    _, t_df  = query2_dataframe(spark, users, trips)

    print(f"\n{'='*40}")
    print(f"Summary:")
    print(f"  RDD-based:        {t_rdd:.4f}s")
    print(f"  DataFrame-based:  {t_df:.4f}s")
    print(f"{'='*40}")

    spark.stop()


if __name__ == "__main__":
    run()