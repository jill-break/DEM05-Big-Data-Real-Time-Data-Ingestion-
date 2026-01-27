import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json

# Import configuration and logging
import config
from logger_utils import setup_logger
import signal

#  Logging Setup 
LOG_FILE = os.path.join(config.LOG_DIR, "spark_streaming.log")
logger = setup_logger("SparkStreaming", LOG_FILE)

# Database Connection Properties
DB_URL = config.DB_URL
DB_PROPERTIES = {
    "user": config.DB_USER,
    "password": config.DB_PASSWORD,
    "driver": config.DB_DRIVER
}

# Global flag for graceful shutdown
STOP_FLAG = False

def signal_handler(signum, frame):
    """Handles termination signals for graceful shutdown."""
    global STOP_FLAG
    logger.info(f"Received termination signal: {signum}. Stopping stream...")
    STOP_FLAG = True

def get_spark_session():
    """Creates the Spark Session with the Postgres JDBC driver loaded."""
    return SparkSession.builder \
        .appName(config.APP_NAME) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

def write_to_postgres(batch_df, batch_id):
    """
    Function called for every micro-batch.
    Validates data, segregates bad rows to DLQ, and writes good rows to production table.
    """
    try:
        if batch_df.isEmpty():
            logger.info(f"Batch ID: {batch_id} is empty. Skipping write.")
            return

        logger.info(f"Processing Batch ID: {batch_id} with {batch_df.count()} records.")

        #  Validation Logic 
        # 1. Price must be > 0
        # 2. Event Type must be valid
        # 3. Product ID and User ID must not be null
        
        # Casting checking implicitly handled by schema, but logical checks needed
        valid_condition = (
            (col("price") > 0) & 
            (col("event_type").isin("view", "purchase")) &
            (col("product_id").isNotNull()) &
            (col("user_id").isNotNull())
        )

        valid_df = batch_df.filter(valid_condition)
        invalid_df = batch_df.filter(~valid_condition)

        #  Write Valid Rows 
        valid_count = valid_df.count()
        if valid_count > 0:
            valid_df.write \
                .jdbc(url=DB_URL, table="user_activity", mode="append", properties=DB_PROPERTIES)
            logger.info(f"Batch ID: {batch_id} - Success: Wrote {valid_count} valid records.")

        #  Write Invalid Rows (DLQ) 
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            logger.warning(f"Batch ID: {batch_id} - Found {invalid_count} invalid records! Sending to DLQ.")
            
            # Add error metadata (simple approach: cast all to string + error msg)
            # In a real app, we'd map this more dynamically
            dlq_df = invalid_df.select(
                col("event_id").cast(StringType()),
                col("event_time").cast(StringType()),
                col("event_type").cast(StringType()),
                col("product_id").cast(StringType()),
                col("price").cast(StringType()),
                col("user_id").cast(StringType()),
                col("session_id").cast(StringType())
            ).withColumn("error_message", from_json(col("event_id"), StringType())) # type: ignore # Placeholder, normally we'd compute the reason
            
            # Just set a generic error message for now since Spark column expressions for specific errors are complex
            from pyspark.sql.functions import lit
            dlq_df = dlq_df.withColumn("error_message", lit("Validation Failed: Negative Price or Invalid Event Type or Null ID"))

            dlq_df.write \
                .jdbc(url=DB_URL, table="user_activity_errors", mode="append", properties=DB_PROPERTIES)

    except Exception as e:
        logger.error(f"FAILED to write Batch ID: {batch_id}. Error: {e}", exc_info=True)

def main():
    """
    Main function to set up the Spark Structured Streaming pipeline.
    """
    # Register Signal Handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

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

        logger.info(f"Monitoring input directory: {config.INPUT_DIR}")

        # 2. Read Stream
        streaming_df = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .option("maxFilesPerTrigger", 1) \
            .load(config.INPUT_DIR)

        # 3. Write Stream using foreachBatch
        # We assign to 'query' variable to control it
        query = streaming_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .option("checkpointLocation", config.CHECKPOINT_DIR) \
            .outputMode("append") \
            .start()

        # Graceful Shutdown Loop
        import time
        while query.isActive:
            if STOP_FLAG:
                logger.info("Termination signal received. Stopping query...")
                query.stop()
            time.sleep(1)
            
        query.awaitTermination()
        
    except Exception as e:
        logger.critical(f"Spark Streaming Job Crashed: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure logs appear immediately in Docker (Unbuffered)
    sys.stdout.reconfigure(line_buffering=True) # type: ignore
    main()