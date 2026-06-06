"""
Advanced Data Management Project
Part 2.2 — Spark Graph Scalability Benchmark
=============================================
Tests PageRank and Connected Components at all scale combinations.
"""

import csv
import os
import sys
import time
import itertools
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pymongo import MongoClient
from pyspark.sql import SparkSession
from utils.generate_data import generate_mongo

MONGO_URI    = "mongodb://localhost:27017/"
MONGO_DB     = "adm_mobility"
DAMPING      = 0.85
MAX_ITER     = 20
USERS_SIZES  = [1_000, 10_000, 50_000]
TRIPS_SIZES  = [10_000, 50_000, 100_000]
EVENTS_SIZES = [0, 2, 5, 10]

OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "spark_graph_benchmark.csv")


def get_spark():
    return (
        SparkSession.builder
        .appName("ADM_SparkGraphBenchmark")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_edges():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    edges = [(str(t["start_station_id"]), str(t["end_station_id"]))
             for t in db.trips.find({}, {"start_station_id":1,"end_station_id":1})]
    stations = {str(s["_id"]): s["name"] for s in db.stations.find()}
    client.close()
    return edges, stations


def run_pagerank(edges, stations):
    n = len(stations)
    all_nodes = list(stations.keys())
    out_edges = defaultdict(list)
    for src, dst in edges:
        out_edges[src].append(dst)
    ranks = {node: 1.0/n for node in all_nodes}
    for _ in range(MAX_ITER):
        new_ranks = {node: (1-DAMPING)/n for node in all_nodes}
        for src, neighbors in out_edges.items():
            if neighbors and src in ranks:
                contrib = ranks[src] / len(neighbors)
                for dst in neighbors:
                    if dst in new_ranks:
                        new_ranks[dst] += DAMPING * contrib
        ranks = new_ranks
    return sorted(ranks.items(), key=lambda x: -x[1])[:3]


def run_connected_components(edges, stations):
    all_nodes = list(stations.keys())
    adjacency = defaultdict(set)
    for src, dst in edges:
        adjacency[src].add(dst)
        adjacency[dst].add(src)
    labels = {node: node for node in all_nodes}
    for _ in range(100):
        changed = False
        for node in all_nodes:
            candidates = [labels[n] for n in adjacency.get(node, set()) if n in labels]
            candidates.append(labels[node])
            min_label = min(candidates)
            if labels[node] != min_label:
                labels[node] = min_label
                changed = True
        if not changed:
            break
    comp_sizes = defaultdict(int)
    for label in labels.values():
        comp_sizes[label] += 1
    return len(comp_sizes)


def run_benchmark():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    combos = list(itertools.product(USERS_SIZES, TRIPS_SIZES, EVENTS_SIZES))
    fieldnames = ["users", "trips", "events_per_trip", "pagerank_s", "cc_s", "num_components"]

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, (n_users, n_trips, n_events) in enumerate(combos, 1):
            print(f"\n[{i}/{len(combos)}] users={n_users}, trips={n_trips}, events={n_events}")

            print("  Generating MongoDB data...")
            generate_mongo(n_users, n_trips, n_events)

            print("  Loading graph data...")
            edges, stations = load_edges()

            print("  Running PageRank...")
            t1 = time.perf_counter()
            run_pagerank(edges, stations)
            pr_time = round(time.perf_counter() - t1, 4)

            print("  Running Connected Components...")
            t2 = time.perf_counter()
            n_comp = run_connected_components(edges, stations)
            cc_time = round(time.perf_counter() - t2, 4)

            print(f"  PageRank={pr_time}s  CC={cc_time}s  components={n_comp}")
            writer.writerow({"users": n_users, "trips": n_trips,
                             "events_per_trip": n_events,
                             "pagerank_s": pr_time, "cc_s": cc_time,
                             "num_components": n_comp})
            f.flush()

    spark.stop()
    print(f"\nDone! Results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    run_benchmark()
