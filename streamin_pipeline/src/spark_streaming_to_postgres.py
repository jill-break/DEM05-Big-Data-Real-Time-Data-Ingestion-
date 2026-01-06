from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json

# --- Configuration ---
APP_NAME = "EcommerceStreamingPipeline"
INPUT_DIR = "/app/data/input"
CHECKPOINT_DIR = "/app/data/checkpoints/postgres_sink"

# Database Connection Properties
# Note: Host is 'postgres' (service name in docker-compose), Port is 5432 (internal docker port)
DB_URL = "jdbc:postgresql://postgres:5432/ecommerce_db"
DB_PROPERTIES = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

def get_spark_session():
    """Creates the Spark Session with the Postgres JDBC driver loaded."""
    return SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

def write_to_postgres(batch_df, batch_id):
    """
    Function called for every micro-batch.
    Writes the current batch of data to PostgreSQL.
    """
    print(f"Processing Batch ID: {batch_id} with {batch_df.count()} records.")
    
    # use 'append' mode to add new rows without deleting old ones
    batch_df.write \
        .jdbc(url=DB_URL, table="user_activity", mode="append", properties=DB_PROPERTIES)

def main():
    """
    Main function to set up the Spark Structured Streaming pipeline.
    1. Reads CSV files from INPUT_DIR as they arrive.
    2. Writes the data into PostgreSQL using foreachBatch.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN") # Reduce log noise

    # 1. Define Schema explicitly (Critical for Streaming)
    # If this is not defined, Spark has to peek at files which is slow/risky in streaming
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("session_id", StringType(), True)
    ])

    print(">>> Monitoring directory for new files...")

    # 2. Read Stream
    streaming_df = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(INPUT_DIR)

    # 3. Write Stream to PostgreSQL
    query = streaming_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()