# ADM Project 2025/2026 — City Mobility Platform

A multi-model data management system that stores and analyses bike/scooter
mobility data (users, stations, trips, events) across four database
technologies and compares their performance.

## Project Structure

```
adm_project/
├── main.py                       # Single entry point (interactive menu)
├── requirements.txt              # Python dependencies
│
├── PostgreSQL/                   # Relational model
│   ├── schema.sql                #   DDL: tables, constraints, indexes
│   └── queries.py                #   Q1-Q4 (SQL)
│
├── MongoDB/                      # Document model
│   ├── schema.py                 #   collections, validators, indexes
│   └── queries.py                #   Q1-Q4 (aggregation pipelines)
│
├── Neo4j/                        # Graph model
│   ├── load_graph.py             #   load data from MongoDB into Neo4j
│   ├── queries.py                #   Q1-Q2 (Cypher)
│   ├── neo4j_benchmark.py        #   scalability benchmark
│   └── neo4j_benchmark.csv       #   benchmark results
│
├── Spark/                        # Apache Spark
│   ├── spark_query2.py           #   Query 2 (RDD + DataFrame)
│   ├── spark_graph.py            #   PageRank + Connected Components
│   ├── spark_graph_benchmark.py  #   scalability benchmark
│   └── spark_graph_benchmark.csv #   benchmark results
│
├── Benchmark/                    # Cross-database benchmark
│   ├── benchmark.py              #   PostgreSQL vs MongoDB (36 combos)
│   └── benchmark_results.csv     #   benchmark results
│
├── Schema_Evolution/             # Part 1.3 - add battery_level field
│   ├── postgres_add_battery_level.sql   #   ALTER TABLE migration
│   └── mongodb_add_battery_level.py     #   validator update + backfill
│
├── utils/                        # Shared helpers
│   └── generate_data.py          #   synthetic data generator (PG + MongoDB)
│
└── Reports/
    └── ADM_Final_Report.pdf      # Final report
```

## Prerequisites

### Python packages
```bash
pip install -r requirements.txt
```

### Databases (running locally)
- **PostgreSQL** on `localhost:5432` (database `adm_mobility`, user `postgres`)
- **MongoDB** on `localhost:27017`
- **Neo4j** on `bolt://localhost:7687`
- **Java JDK 8+** (required by PySpark)

## Quick Start

The easiest way is the single entry point:

```bash
python main.py
```

This opens an interactive menu where you can run any step, or choose `a`
to run the typical setup flow (generate data, then run all queries) in order.

You can also run a single step directly:
```bash
python main.py 2      # run PostgreSQL queries
python main.py 4      # run Spark Query 2
```

## Running Each Part Manually

### 1. Create the PostgreSQL schema
```bash
psql -U postgres -c "CREATE DATABASE adm_mobility;"
psql -U postgres -d adm_mobility -f PostgreSQL/schema.sql
```

### 2. Set up MongoDB collections
```bash
python MongoDB/schema.py
```

### 3. Generate data (example: 1k users, 10k trips, 2 events/trip)
```bash
python utils/generate_data.py --users 1000 --trips 10000 --events 2 --target both
```

### 4. Run the queries
```bash
python PostgreSQL/queries.py      # relational Q1-Q4
python MongoDB/queries.py         # document Q1-Q4
python Spark/spark_query2.py      # Spark Query 2 (RDD + DataFrame)
```

### 5. Graph part (Neo4j)
```bash
python Neo4j/load_graph.py        # load MongoDB data into Neo4j
python Neo4j/queries.py           # Cypher Q1-Q2
python Spark/spark_graph.py       # PageRank + Connected Components
```

### 6. Scalability benchmarks
```bash
python Benchmark/benchmark.py --target both    # PostgreSQL vs MongoDB
python Neo4j/neo4j_benchmark.py                # Neo4j queries
python Spark/spark_graph_benchmark.py          # Spark graph
```

### 7. Schema evolution (Part 1.3 - add battery_level to BATTERY events)
```bash
psql -U postgres -d adm_mobility -f Schema_Evolution/postgres_add_battery_level.sql
python Schema_Evolution/mongodb_add_battery_level.py
```

## Scale Combinations (as per assignment)
| Users | Trips  | Events/Trip |
|-------|--------|-------------|
| 1k    | 10k    | 0           |
| 10k   | 50k    | 2           |
| 50k   | 100k   | 5           |
|       |        | 10          |

3 x 3 x 4 = **36 combinations** tested per database.
