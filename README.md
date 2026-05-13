# ADM Project 2025/2026 — City Mobility Platform

## Project Structure

```
adm_project/
├── relational/
│   ├── schema.sql          # PostgreSQL DDL (tables + indexes)
│   └── queries.py          # Q1–Q4 for PostgreSQL
├── document/
│   ├── schema.py           # MongoDB collection setup + validators
│   └── queries.py          # Q1–Q4 for MongoDB (aggregation pipelines)
├── spark/
│   └── spark_query2.py     # PySpark Q2: RDD, DataFrame, Spark SQL
├── graph/                  # (Part 2 — Neo4j)
├── utils/
│   ├── generate_data.py    # Synthetic data generator (PG + MongoDB)
│   └── benchmark.py        # Scalability benchmark runner
└── README.md
```

## Prerequisites

### Python packages
```bash
pip install pg8000 pymongo pyspark
```

### Databases
- PostgreSQL running on localhost:5432 (db: adm_mobility, user: postgres)
- MongoDB running on localhost:27017
- Java JDK 8 (for PySpark)

## Setup

### 1. Create PostgreSQL database
```bash
psql -U postgres -c "CREATE DATABASE adm_mobility;"
psql -U postgres -d adm_mobility -f relational/schema.sql
```

### 2. Setup MongoDB collections
```bash
python document/schema.py
```

## Running

### Generate data (example: 1k users, 10k trips, 2 events/trip)
```bash
python utils/generate_data.py --users 1000 --trips 10000 --events 2 --target both
```

### Run PostgreSQL queries
```bash
python relational/queries.py
```

### Run MongoDB queries
```bash
python document/queries.py
```

### Run Spark Query 2
```bash
spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  spark/spark_query2.py
```

### Run full scalability benchmark
```bash
python utils/benchmark.py --target both
# Results saved to utils/benchmark_results.csv
```

## Scale Combinations (as per assignment)
| Users | Trips  | Events/Trip |
|-------|--------|-------------|
| 1k    | 10k    | 0           |
| 10k   | 50k    | 2           |
| 50k   | 100k   | 5           |
|       |        | 10          |
