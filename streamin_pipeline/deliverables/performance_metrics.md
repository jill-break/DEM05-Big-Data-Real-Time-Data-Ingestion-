# System Performance Metrics

This report summarizes the performance characteristics of the pipeline under test conditions (Single Worker Node, Local Docker Environment).

## Key Metrics

| Metric | Observed Value | Description |
|:---|:---|:---|
| **End-to-End Latency** | **< 5 Seconds** | The time difference between a CSV file landing in the input folder and the rows appearing in PostgreSQL. |
| **Throughput** | **~100 Events/Sec** | The rate at which Spark processes records. (Current generator is capped at ~80 rows/5sec, but Spark processed batches instantly). |
| **Batch Processing Time** | **0.8 - 1.2 Seconds** | Time taken by Spark to read a micro-batch, transform it, and commit to DB via JDBC (Java Database Connectivity). |
| **Startup Time** | **~15 Seconds** | Time required for the Spark Context to initialize and connect to the Postgres JDBC (Java Database Connectivity) socket. |

## Scalability Assessment

* **Vertical Scaling:** The current setup uses 1GB RAM for the Spark Worker. This can be increased in `docker-compose.yml` (`SPARK_WORKER_MEMORY`) to handle larger batches.
* **Horizontal Scaling:** The architecture supports adding multiple `spark-worker` containers. This would allow parallel processing of files if the ingestion rate increases significantly.

## Bottleneck Analysis
* **Current Bottleneck:** The **Data Generator** is the current limiting factor (synthetic limitation). The Spark Consumer is idle ~80% of the time, waiting for new data.
* **Database Write:** The JDBC `append` mode is efficient, but for massive loads (millions of rows/sec), the single Postgres instance would eventually become the bottleneck.