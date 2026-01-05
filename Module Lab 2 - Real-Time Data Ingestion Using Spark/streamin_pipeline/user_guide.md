# User Guide

## Prerequisites
* Docker Desktop installed and running.

## Step-by-Step Execution

1. **Start Infrastructure**
   ```bash
   docker-compose up -d

2. **Verify Database Creation**
    ```bash
    docker exec -it postgres_db psql -U myuser -d ecommerce_db -c "\dt"

3. **Start the Data Generator (Terminal 1)**
    ```bash
    docker exec -it spark_master python3 /app/src/data_generator.py

4. **Submit the Spark Job (Terminal 2)**
    ```bash
    docker exec -it spark_master /opt/spark/bin/spark-submit \
    --jars /opt/spark/jars/postgresql-42.6.0.jar \
    /app/src/spark_streaming_to_postgres.py
    

5. **Verify Data Ingestion Check the row count in the database:**
    ```bash
    docker exec -it postgres_db psql -U myuser -d ecommerce_db -c "SELECT count(*) FROM user_activity;"