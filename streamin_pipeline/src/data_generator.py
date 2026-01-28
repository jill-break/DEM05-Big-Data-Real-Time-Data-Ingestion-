import csv
import time
import uuid
import random
import os
import shutil
import sys
from datetime import datetime
from faker import Faker

# Import configuration and logging
import config
from logger_utils import setup_logger

#  Logging Setup 
LOG_FILE = os.path.join(config.LOG_DIR, "data_generator.log")
logger = setup_logger("DataGenerator", LOG_FILE)

# Ensure data directories exist
os.makedirs(config.INPUT_DIR, exist_ok=True)
os.makedirs(config.TEMP_DIR, exist_ok=True)

fake = Faker()

def generate_event():
    """Creates a single dictionary representing a user event."""
    # 70% chance of 'view', 30% chance of 'purchase'
    event_type = random.choices(['view', 'purchase'], weights=[0.7, 0.3])[0]
    
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": event_type,
        "product_id": random.randint(100, 200),
        "price": round(random.uniform(10.0, 500.0), 2),
        "user_id": random.randint(1000, 5000),
        "session_id": str(uuid.uuid4())
    }

def generate_batch(batch_id):
    """Generates a CSV file with 50-100 random events."""
    file_name = f"events_{batch_id}.csv"
    temp_path = os.path.join(config.TEMP_DIR, file_name)
    final_path = os.path.join(config.INPUT_DIR, file_name)
    
    try:
        # 1. Write to specific Temp path first  (to ensure atomicity)
        with open(temp_path, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "event_id", "event_time", "event_type", "product_id", "price", "user_id", "session_id"
            ])
            writer.writeheader()
            
            # Generate random number of rows
            rows = random.randint(config.BATCH_SIZE_MIN, config.BATCH_SIZE_MAX)
            for _ in range(rows):
                writer.writerow(generate_event())
                
        # 2. Atomic Move: move file to input folder 
        shutil.move(temp_path, final_path)
        
        # Log success using the logger
        logger.info(f"Generated Batch #{batch_id}: {file_name} with {rows} rows.")
        
    except Exception as e:
        logger.error(f"Failed to generate batch #{batch_id}: {e}", exc_info=True)

if __name__ == "__main__":
    # Force unbuffered output for the console so logs appear instantly in Docker
    sys.stdout.reconfigure(line_buffering=True) # type: ignore

    logger.info("Starting Data Generator...")
    logger.info(f"Writing data to: {config.INPUT_DIR}")
    logger.info(f"Configuration: Batch Size [{config.BATCH_SIZE_MIN}-{config.BATCH_SIZE_MAX}], Interval {config.BATCH_INTERVAL}s")
    
    batch_counter = 1
    try:
        while True:
            generate_batch(batch_counter)
            batch_counter += 1
            # Wait configurable time before next batch
            time.sleep(config.BATCH_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Generator stopped by user.")
    except Exception as e:
        logger.critical(f"Generator crashed unexpectedly: {e}", exc_info=True)