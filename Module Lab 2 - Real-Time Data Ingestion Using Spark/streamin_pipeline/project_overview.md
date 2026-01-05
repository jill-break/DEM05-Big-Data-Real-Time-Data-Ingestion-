# Real-Time Data Ingestion Pipeline

## Overview
This project simulates an e-commerce backend processing user activity in real-time. It uses a micro-services architecture containerized with Docker.

## Architecture Components
1. **Data Generator (Producer):** A Python script (`data_generator.py`) that generates synthetic "view" and "purchase" events. It uses atomic file moves to ensure data integrity.
2. **Streaming Engine (Apache Spark):** A Spark Structured Streaming job (`spark_streaming_to_postgres.py`) that monitors an input directory. It uses `foreachBatch` to write micro-batches to the database.
3. **Storage (PostgreSQL):** A relational database acting as the final sink for analytical data.

## Data Flow
[Generator] -> (CSV Files) -> [Spark Stream] -> (JDBC) -> [PostgreSQL]