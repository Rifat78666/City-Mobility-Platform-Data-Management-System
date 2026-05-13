"""
Advanced Data Management Project
Part 1.2 — Scalability Benchmark
==================================
Runs all 4 queries on both PostgreSQL and MongoDB across
the required data-scale combinations and saves results to CSV.

Scale parameters (as per assignment):
  users:          1k, 10k, 50k
  trips:          10k, 50k, 100k
  events/trip:    0, 2, 5, 10
"""

import csv
import itertools
import os
import sys
import time

# Add parent dir so we can import siblings
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.generate_data import generate_postgres, generate_mongo
from relational.queries   import run_all_queries as pg_queries
from document.queries     import run_all_queries as mongo_queries

USERS_SIZES  = [1_000, 10_000, 50_000]
TRIPS_SIZES  = [10_000, 50_000, 100_000]
EVENTS_SIZES = [0, 2, 5, 10]

OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "benchmark_results.csv")


def run_benchmark(target: str = "both"):
    """
    Run benchmark for all scale combinations.
    target: 'pg', 'mongo', or 'both'
    """
    combinations = list(itertools.product(USERS_SIZES, TRIPS_SIZES, EVENTS_SIZES))
    print(f"Total combinations: {len(combinations)} × 4 queries × targets")

    fieldnames = [
        "db", "users", "trips", "events_per_trip",
        "Q1_s", "Q2_s", "Q3_s", "Q4_s",
    ]

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for n_users, n_trips, n_events in combinations:
            label = f"users={n_users}, trips={n_trips}, events={n_events}"
            print(f"\n{'='*60}")
            print(f"Configuration: {label}")
            print(f"{'='*60}")

            # ── PostgreSQL ──────────────────────────────────────────
            if target in ("pg", "both"):
                print("\n[1/2] Generating PostgreSQL data...")
                generate_postgres(n_users, n_trips, n_events)
                print("[1/2] Running PostgreSQL queries...")
                pg_times = pg_queries(verbose=False)
                writer.writerow({
                    "db": "PostgreSQL",
                    "users": n_users, "trips": n_trips, "events_per_trip": n_events,
                    "Q1_s": round(pg_times["Q1"], 4),
                    "Q2_s": round(pg_times["Q2"], 4),
                    "Q3_s": round(pg_times["Q3"], 4),
                    "Q4_s": round(pg_times["Q4"], 4),
                })
                f.flush()

            # ── MongoDB ─────────────────────────────────────────────
            if target in ("mongo", "both"):
                print("\n[2/2] Generating MongoDB data...")
                generate_mongo(n_users, n_trips, n_events)
                print("[2/2] Running MongoDB queries...")
                mongo_times = mongo_queries(verbose=False)
                writer.writerow({
                    "db": "MongoDB",
                    "users": n_users, "trips": n_trips, "events_per_trip": n_events,
                    "Q1_s": round(mongo_times["Q1"], 4),
                    "Q2_s": round(mongo_times["Q2"], 4),
                    "Q3_s": round(mongo_times["Q3"], 4),
                    "Q4_s": round(mongo_times["Q4"], 4),
                })
                f.flush()

    print(f"\nBenchmark complete. Results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["pg", "mongo", "both"], default="both")
    args = parser.parse_args()
    run_benchmark(args.target)
