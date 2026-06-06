"""
Advanced Data Management Project - City Mobility Platform
=========================================================
Single entry point to run every part of the project.

Folder map:
    PostgreSQL/   relational database (schema + queries)
    MongoDB/      document database (schema + queries)
    Neo4j/        graph database (loader + queries + benchmark)
    Spark/        Spark Query 2 + graph algorithms + benchmark
    Benchmark/    PostgreSQL vs MongoDB scalability benchmark
    utils/        shared synthetic-data generator
    Reports/      final report PDF

Usage:
    python main.py            -> interactive menu
    python main.py <number>   -> run one step directly (e.g. python main.py 2)
"""

import os
import sys
import subprocess

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable  # use the exact Python running this script

# (label, script path relative to project root, extra CLI args)
STEPS = {
    "1": ("Generate synthetic data (1k users / 10k trips / 2 events, PG + MongoDB)",
          "utils/generate_data.py",
          ["--users", "1000", "--trips", "10000", "--events", "2", "--target", "both"]),
    "2": ("PostgreSQL - run Q1-Q4",
          "PostgreSQL/queries.py", []),
    "3": ("MongoDB - run Q1-Q4",
          "MongoDB/queries.py", []),
    "4": ("Spark - Query 2 (RDD + DataFrame)",
          "Spark/spark_query2.py", []),
    "5": ("Neo4j - load graph from MongoDB",
          "Neo4j/load_graph.py", []),
    "6": ("Neo4j - run graph queries Q1-Q2",
          "Neo4j/queries.py", []),
    "7": ("Spark - graph algorithms (PageRank + Connected Components)",
          "Spark/spark_graph.py", []),
    "8": ("Benchmark - PostgreSQL vs MongoDB (all 36 scale combos)",
          "Benchmark/benchmark.py", ["--target", "both"]),
    "9": ("Benchmark - Neo4j queries (all scale combos)",
          "Neo4j/neo4j_benchmark.py", []),
    "10": ("Benchmark - Spark graph (all scale combos)",
           "Spark/spark_graph_benchmark.py", []),
}

# Recommended order to set everything up from scratch
TYPICAL_FLOW = ["1", "2", "3", "4", "5", "6", "7"]


def run_step(key: str) -> int:
    label, script, args = STEPS[key]
    script_path = os.path.join(PROJECT_ROOT, script)
    print("\n" + "=" * 70)
    print(f"  >> [{key}] {label}")
    print(f"     {PY} {script} {' '.join(args)}")
    print("=" * 70 + "\n")
    result = subprocess.run([PY, script_path, *args], cwd=PROJECT_ROOT)
    if result.returncode == 0:
        print(f"\n[OK] Step {key} finished successfully.")
    else:
        print(f"\n[FAILED] Step {key} exited with code {result.returncode}.")
    return result.returncode


def print_menu():
    print("\n" + "=" * 70)
    print("  ADM Project - City Mobility Platform")
    print("=" * 70)
    print("  Databases: PostgreSQL  |  MongoDB  |  Neo4j  |  Spark")
    print("-" * 70)
    for key in sorted(STEPS, key=lambda k: int(k)):
        label = STEPS[key][0]
        print(f"   {key:>2}.  {label}")
    print("-" * 70)
    print("    a.  Run typical setup flow (steps 1 to 7 in order)")
    print("    q.  Quit")
    print("=" * 70)


def main():
    # Direct mode: python main.py 3
    if len(sys.argv) > 1:
        key = sys.argv[1].strip()
        if key in STEPS:
            sys.exit(run_step(key))
        print(f"Unknown step '{key}'. Valid steps: {', '.join(sorted(STEPS, key=lambda k: int(k)))}")
        sys.exit(1)

    # Interactive mode
    while True:
        print_menu()
        choice = input("\n  Choose an option: ").strip().lower()
        if choice in ("q", "quit", "exit"):
            print("Bye!")
            break
        elif choice == "a":
            for key in TYPICAL_FLOW:
                if run_step(key) != 0:
                    print("\nStopping flow because a step failed.")
                    break
        elif choice in STEPS:
            run_step(choice)
        else:
            print("  Invalid choice. Please pick a number from the menu.")


if __name__ == "__main__":
    main()
