import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json

# --- Configuration ---
APP_NAME = "EcommerceStreamingPipeline"
INPUT_DIR = "/app/data/input"
CHECKPOINT_DIR = "/app/data/checkpoints/postgres_sink"

# Database Connection Properties
DB_URL = "jdbc:postgresql://postgres:5432/ecommerce_db"
DB_PROPERTIES = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

# --- Logging Setup ---
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "spark_streaming.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True # Override any default Spark logging configs
)
logger = logging.getLogger(__name__)

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
    try:
        count = batch_df.count()
        if count > 0:
            logger.info(f"Processing Batch ID: {batch_id} with {count} records.")
            
            # Write to DB
            batch_df.write \
                .jdbc(url=DB_URL, table="user_activity", mode="append", properties=DB_PROPERTIES)
            
            logger.info(f"Batch ID: {batch_id} written successfully.")
        else:
            logger.info(f"Batch ID: {batch_id} is empty. Skipping write.")
            
    except Exception as e:
        logger.error(f"FAILED to write Batch ID: {batch_id}. Error: {e}", exc_info=True)

def main():
    """
    Main function to set up the Spark Structured Streaming pipeline.
    """
    try:
        spark = get_spark_session()
        spark.sparkContext.setLogLevel("WARN") # Reduce Spark's internal noise
        
        logger.info(">>> Spark Session Initialized. Starting Stream...")

        # 1. Define Schema explicitly
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("session_id", StringType(), True)
        ])

        logger.info(f"Monitoring input directory: {INPUT_DIR}")

        # 2. Read Stream
        streaming_df = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(INPUT_DIR)

        # 3. Write Stream using foreachBatch
        query = streaming_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .outputMode("append") \
            .start()

        query.awaitTermination()
        
    except Exception as e:
        logger.critical(f"Spark Streaming Job Crashed: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure logs appear immediately in Docker (Unbuffered)
    sys.stdout.reconfigure(line_buffering=True) # type: ignore
    main()