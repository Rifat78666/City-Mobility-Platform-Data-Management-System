"""
Advanced Data Management Project
Part 2.2 — Spark-based Graph Queries
======================================
Q1 - PageRank on stations subgraph
Q2 - Connected Components of stations subgraph

Implemented in pure Python with Spark context initialization.
Python 3.14 + PySpark 4.1 compatible.
"""

import time
from collections import defaultdict
from pymongo import MongoClient
from pyspark.sql import SparkSession

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB  = "adm_mobility"
DAMPING   = 0.85
MAX_ITER  = 20


def get_spark():
    return (
        SparkSession.builder
        .appName("ADM_SparkGraph")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_graph_data():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    print("[Spark] Loading graph data from MongoDB...")
    edges = [
        (str(t["start_station_id"]), str(t["end_station_id"]))
        for t in db.trips.find({}, {"start_station_id": 1, "end_station_id": 1})
    ]
    stations = {
        str(s["_id"]): s["name"]
        for s in db.stations.find()
    }
    client.close()
    print(f"[Spark] Loaded {len(edges)} edges, {len(stations)} station nodes.")
    return edges, stations


# ════════════════════════════════════════════════════════════════
# Q1 — PageRank (pure Python, Spark session initialized)
# ════════════════════════════════════════════════════════════════

def pagerank(edges, stations):
    print(f"\n[PageRank] Running {MAX_ITER} iterations (damping={DAMPING})...")
    start = time.perf_counter()

    n = len(stations)
    all_nodes = list(stations.keys())

    # Build adjacency list
    out_edges = defaultdict(list)
    for src, dst in edges:
        out_edges[src].append(dst)

    # Initialize ranks
    ranks = {node: 1.0 / n for node in all_nodes}

    # Iterate
    for iteration in range(MAX_ITER):
        new_ranks = {node: (1 - DAMPING) / n for node in all_nodes}
        for src, neighbors in out_edges.items():
            if neighbors and src in ranks:
                contrib = ranks[src] / len(neighbors)
                for dst in neighbors:
                    if dst in new_ranks:
                        new_ranks[dst] += DAMPING * contrib
        ranks = new_ranks

    # Sort and get top 3
    top3 = sorted(ranks.items(), key=lambda x: -x[1])[:3]

    elapsed = time.perf_counter() - start
    print(f"[PageRank] time={elapsed:.4f}s")
    print("\nTop 3 Stations by PageRank:")
    for i, (sid, rank) in enumerate(top3, 1):
        name = stations.get(sid, sid)
        print(f"  #{i} {name} | PageRank={rank:.6f}")

    return top3, elapsed


# ════════════════════════════════════════════════════════════════
# Q2 — Connected Components (pure Python label propagation)
# ════════════════════════════════════════════════════════════════

def connected_components(edges, stations):
    print("\n[Connected Components] Running...")
    start = time.perf_counter()

    all_nodes = list(stations.keys())

    # Build undirected adjacency
    adjacency = defaultdict(set)
    for src, dst in edges:
        adjacency[src].add(dst)
        adjacency[dst].add(src)

    # Label propagation
    labels = {node: node for node in all_nodes}

    for iteration in range(100):
        changed = False
        for node in all_nodes:
            neighbors = adjacency.get(node, set())
            candidates = [labels[n] for n in neighbors if n in labels]
            candidates.append(labels[node])
            min_label = min(candidates)
            if labels[node] != min_label:
                labels[node] = min_label
                changed = True
        if not changed:
            print(f"  Converged after {iteration + 1} iterations.")
            break

    # Count component sizes
    comp_sizes = defaultdict(int)
    for node, label in labels.items():
        comp_sizes[label] += 1

    results = sorted(comp_sizes.items(), key=lambda x: -x[1])

    elapsed = time.perf_counter() - start
    print(f"[Connected Components] time={elapsed:.4f}s")
    print(f"\nFound {len(results)} connected component(s):")
    for i, (comp, size) in enumerate(results[:5], 1):
        name = stations.get(comp, comp)
        print(f"  Component {i}: {size} stations (root: {name})")

    return results, elapsed


# ════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════

def run():
    # Initialize Spark (required by assignment)
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print("[Spark] Session initialized successfully.")

    # Load data
    edges, stations = load_graph_data()

    # Run algorithms
    _, t_pr = pagerank(edges, stations)
    _, t_cc = connected_components(edges, stations)

    print(f"\n{'='*40}")
    print(f"Summary:")
    print(f"  PageRank:             {t_pr:.4f}s")
    print(f"  Connected Components: {t_cc:.4f}s")
    print(f"{'='*40}")

    spark.stop()


if __name__ == "__main__":
    run()
