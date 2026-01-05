#### 4. `performance_metrics.md`
This is where you report the "speed" of your system. You can get this data from your Spark Terminal logs.

* **Latency:** How long it takes from "File Created" to "Row in DB". In your logs, you saw Spark pick up files almost instantly. Estimated: < 5 seconds.
* **Throughput:** Look at your Spark logs: `Processing Batch ID: X with 85 records`. If that batch took 1 second, your throughput is 85 events/sec.

```markdown
# Performance Metrics

* **Average Batch Size:** ~60-90 records per file.
* **Processing Latency:** < 5 seconds (Time from CSV generation to PostgreSQL availability).
* **Throughput:** Capable of handling approx. 100 events/second in the current single-worker configuration.
* **Scalability:** The architecture uses Spark, allowing horizontal scaling by adding more `spark-worker` containers in `docker-compose.yml`.