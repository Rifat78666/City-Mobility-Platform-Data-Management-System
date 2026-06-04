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

# ── Environment setup (must happen BEFORE importing pyspark) ─────
# 1. cloudpickle shim: PySpark 4.x bundles an older cloudpickle that
#    recurses infinitely when serializing lambdas on Python 3.14.
#    Force PySpark to use the standalone pip-installed cloudpickle.
# 2. PYSPARK_PYTHON: tell Spark workers exactly which python.exe to
#    use (otherwise Windows routes `python` to the Microsoft Store
#    stub, and worker processes fail to start).
import os
import sys

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

try:
    import cloudpickle as _cloudpickle
    _cp_ver = getattr(_cloudpickle, "__version__", "0")
    sys.modules["pyspark.cloudpickle"] = _cloudpickle
    sys.modules["pyspark.cloudpickle.cloudpickle"] = _cloudpickle
    sys.modules["pyspark.cloudpickle.cloudpickle_fast"] = _cloudpickle
    print(f"[Patch] Using standalone cloudpickle {_cp_ver}")
    print(f"[Patch] PYSPARK_PYTHON = {sys.executable}")
except ImportError:
    print("[Patch] standalone cloudpickle not installed — "
          "RDD path may fail on Python 3.14")

import json
import tempfile
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
    """
    DataFrame path. To avoid PySpark's Python-worker IPC (which is broken
    on Python 3.14 + Windows), we serialize the data to JSON files first
    and let Spark read them via spark.read.json() — pure JVM, no Python
    workers in the data path. Aggregation runs as native Spark SQL.
    Results are displayed with .show() (JVM-side) instead of .collect().
    """
    print("\n[DataFrame] Running Query 2 (JSON-staged, JVM-native)...")
    start = time.perf_counter()

    tmp_dir = tempfile.mkdtemp(prefix="adm_q2_")
    users_path = tmp_dir + "/users.json"
    trips_path = tmp_dir + "/trips.json"

    with open(users_path, "w", encoding="utf-8") as f:
        for uid, name, surname in users:
            f.write(json.dumps({"user_id": uid, "name": name, "surname": surname}) + "\n")
    with open(trips_path, "w", encoding="utf-8") as f:
        for uid, dur in trips:
            f.write(json.dumps({"user_id": uid, "duration_min": dur}) + "\n")

    trips_df = spark.read.json(trips_path)
    users_df = spark.read.json(users_path)

    agg_df = trips_df.groupBy("user_id").agg(
        F.count("*").alias("num_trips"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
    )

    result_df = (
        users_df.join(agg_df, on="user_id", how="left")
        .fillna({"num_trips": 0, "avg_duration_min": 0.0})
        .orderBy("user_id")
    )

    n = result_df.count()
    print("Sample (first 3):")
    result_df.show(3, truncate=False)

    elapsed = time.perf_counter() - start
    print(f"[DataFrame] users={n}, time={elapsed:.4f}s")
    return n, elapsed


# ════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════

def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    users, trips = load_data_from_mongo()

    # The RDD path spawns Python worker processes that communicate with
    # the JVM over sockets. On Python 3.14 + Windows + PySpark 4.x the
    # worker crashes during socket finalization (WinError 10038). The
    # function definition is kept above for reference; we wrap the call
    # so the script still completes and the DataFrame path can run.
    t_rdd = None
    try:
        _, t_rdd = query2_rdd(spark, trips)
    except Exception as e:
        print(f"\n[RDD] skipped — PySpark worker IPC fails on "
              f"Python 3.14 + Windows: {type(e).__name__}")

    _, t_df = query2_dataframe(spark, users, trips)

    print(f"\n{'='*40}")
    print(f"Summary:")
    print(f"  RDD-based:        {t_rdd:.4f}s" if t_rdd is not None else "  RDD-based:        skipped")
    print(f"  DataFrame-based:  {t_df:.4f}s")
    print(f"{'='*40}")

    spark.stop()


if __name__ == "__main__":
    run()
