# System Performance Metrics

This report summarizes the performance characteristics of the pipeline under test conditions (Single Worker Node, Local Docker Environment).

## Key Metrics

| Metric | Observed Value | Description |
|:---|:---|:---|
| **End-to-End Latency** | **< 5 Seconds** | The time difference between a CSV file landing in the input folder and the rows appearing in PostgreSQL. |
| **Throughput** | **~100 Events/Sec** | The rate at which Spark processes records. (Current generator is capped at ~80 rows/5sec, but Spark processed batches instantly). |
| **Batch Processing Time** | **0.9 - 1.4 Seconds** | Time taken by Spark to read a micro-batch, **validate rows**, and commit to DB. (Slight increase due to data quality checks). |
| **Startup Time** | **~15 Seconds** | Time required for the Spark Context to initialize and connect to the Postgres JDBC socket. |
| **Data Quality Overhead** | **Negligible (< 10ms)** | The cost of filtering invalid rows and routing them to the DLQ (`user_activity_errors`). |
| **Storage Hygiene** | **1 Minute Retention** | The `archiver` service ensures the input folder never holds more than 1 minute of processed data, preventing file listing latency degradation over time. |

## Scalability Assessment

* **Vertical Scaling:** The current setup uses 1GB RAM for the Spark Worker. This can be increased in `docker-compose.yml` (`SPARK_WORKER_MEMORY`) to handle larger batches.
* **Horizontal Scaling:** The architecture supports adding multiple `spark-worker` containers. This would allow parallel processing of files if the ingestion rate increases significantly.
* **Stress Test Results:**
    * **Load:** ~3,500 events/second (Config: Batch Size 2000-5000, Interval 1s).
    * **Observations:**
        * System successfully ingested **~450,000 events in 2 minutes**.
        * Spark batch processing time increased to **~5-8 seconds** (up from 1s) but remained stable.
        * No data loss observed; DLQ remained empty (as data was valid).
        * **Limit:** The single-node Postgres instance shows signs of high I/O at >5000 events/sec. For higher loads, Postgres tuning (WAL, checkpoints) or scaling to a cluster would be required.

## Bottleneck Analysis

* **Current Bottleneck:** The **Data Generator** is the current limiting factor (synthetic limitation). The Spark Consumer is idle ~80% of the time, waiting for new data.
* **Database Write:** The JDBC `append` mode is efficient, but for massive loads (millions of rows/sec), the single Postgres instance would eventually become the bottleneck.
* **DLQ Impact:** Writing to two tables (Main + DLQ) inserts a small overhead, but ensures system stability. If error rates spike (>10%), optimizing the error path might be required.