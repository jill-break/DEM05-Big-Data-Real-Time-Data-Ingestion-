# User Guide

This guide provides detailed instructions on how to set up, run, and monitor the Real-Time Data Ingestion Pipeline.

## 1. Prerequisites

Ensure you have the following installed:
*   **Docker Desktop** (must be running).
*   **Git** (optional, for cloning).

## 2. Setup & Installation

Open your terminal (PowerShell or Command Prompt) and navigate to the project directory.

```bash
# Navigate to the project root directory
cd "c:\Users\CourageDei\Desktop\Moodle\DE\DE05\DEM05-Big-Data-Real-Time-Data-Ingestion-\streamin_pipeline"
```

## 3. Starting the Infrastructure

Start all necessary services (Postgres, Spark Master, Spark Worker, Archiver) in the background.

```bash
docker-compose up --build -d
```

**Verify successful startup:**
```bash
docker ps
# You should see containers: spark_master, spark_worker, file_archiver, postgres_db
```

## 4. Running the Pipeline

### Step 4.1: Start the Data Generator
This simulates user activity by generating CSV files in `data/input`.

```bash
docker exec -d spark_master python3 /app/src/data_generator.py
```
*   The `-d` flag runs the process in the background.
*   Logs can be verified via: `docker logs spark_master -f`

### Step 4.2: Submit the Spark Streaming Job
This job reads the stream of CSV files and processes them.

```bash
docker exec -d spark_master /opt/spark/bin/spark-submit --jars /opt/spark/jars/postgresql-42.6.0.jar /app/src/spark_streaming_to_postgres.py
```

## 5. Monitoring & Verification

### 5.1 Check Database Records
Connect to the PostgreSQL database to verify data ingestion.

**Check Valid Data:**
```bash
docker exec -it postgres_db psql -U myuser -d ecommerce_db -c "SELECT count(*) FROM user_activity;"
```

**Check Error Data (Dead Letter Queue):**
```bash
docker exec -it postgres_db psql -U myuser -d ecommerce_db -c "SELECT * FROM user_activity_errors;"
```

### 5.2 Monitor File Archiving
The `archiver` service automatically moves processed files to `data/archive`.

```bash
# List input directory (should be relatively empty)
ls -l data/input

# List archive directory (files accumulate here)
ls -l data/archive
```

## 6. Advanced Testing Scenarios

### 6.1 Inject Bad Data
Test the Dead Letter Queue (DLQ) by injecting invalid records.

```bash
docker exec -it spark_master python3 /app/src/inject_bad_data.py
# Check 'user_activity_errors' table afterwards to confirm rejection.
```

### 6.2 Stress Testing
To simulate high load conditions:

1.  Stop the current stack:
    ```bash
    docker-compose down
    ```
2.  Edit `docker-compose.yml` to uncomment/add stress test variables:
    ```yaml
    environment:
      - BATCH_SIZE_MIN=2000
      - BATCH_SIZE_MAX=5000
      - BATCH_INTERVAL=1
    ```
3.  Restart and run generator:
    ```bash
    docker-compose up -d
    docker exec -d spark_master python3 /app/src/data_generator.py
    ```

## 7. Shutdown & Cleanup

To stop all services and free up resources:

```bash
docker-compose down
```

**Note:** The database volume persists data. To remove it and start fresh, add the `-v` flag:
```bash
docker-compose down -v
```