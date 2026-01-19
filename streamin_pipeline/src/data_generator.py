import csv
import time
import uuid
import random
import os
import shutil
import logging
from datetime import datetime
from faker import Faker

# --- Configuration ---
DATA_DIR = "/app/data"
INPUT_DIR = os.path.join(DATA_DIR, "input")   # Where Spark looks for files
TEMP_DIR = os.path.join(DATA_DIR, "temp")     # Where we write files initially

# --- Logging Setup ---
# Define log directory relative to the project root
# If running in Docker, this maps to the volume mounted at /app/logs
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "data_generator.log")

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),      # Write to file for history
        logging.StreamHandler()             # Write to console so you see output live
    ]
)
logger = logging.getLogger(__name__)

# Ensure data directories exist
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

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
    temp_path = os.path.join(TEMP_DIR, file_name)
    final_path = os.path.join(INPUT_DIR, file_name)
    
    try:
        # 1. Write to specific Temp path first (Safe from Spark)
        with open(temp_path, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "event_id", "event_time", "event_type", "product_id", "price", "user_id", "session_id"
            ])
            writer.writeheader()
            
            # Generate random number of rows
            rows = random.randint(50, 100)
            for _ in range(rows):
                writer.writerow(generate_event())
                
        # 2. Atomic Move: Rename file to input folder (Spark sees it instantly)
        shutil.move(temp_path, final_path)
        
        # Log success using the logger
        logger.info(f"Generated Batch #{batch_id}: {file_name} with {rows} rows.")
        
    except Exception as e:
        logger.error(f"Failed to generate batch #{batch_id}: {e}", exc_info=True)

if __name__ == "__main__":
    logger.info("Starting Data Generator...")
    logger.info(f"Writing data to: {INPUT_DIR}")
    
    batch_counter = 1
    try:
        while True:
            generate_batch(batch_counter)
            batch_counter += 1
            # Wait 5 seconds before next batch
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Generator stopped by user.")
    except Exception as e:
        logger.critical(f"Generator crashed unexpectedly: {e}", exc_info=True)