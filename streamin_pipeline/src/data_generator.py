import csv
import time
import uuid
import random
import os
import shutil
from datetime import datetime
from faker import Faker

# --- Configuration ---
DATA_DIR = "/app/data"
INPUT_DIR = os.path.join(DATA_DIR, "input")  # Where Spark looks for files
TEMP_DIR = os.path.join(DATA_DIR, "temp")    # Where we write files initially

# Ensure directories exist
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
    print(f"Generated {file_name} with {rows} rows.")
    shutil.move(temp_path, final_path)

if __name__ == "__main__":
    print("Starting Data Generator...")
    print(f"Writing data to: {INPUT_DIR}")
    
    batch_counter = 1
    try:
        while True:
            generate_batch(batch_counter)
            batch_counter += 1
            # Wait 5 seconds before next batch
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nGenerator stopped.")