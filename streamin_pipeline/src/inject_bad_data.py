import csv
import os
import shutil
import uuid
from datetime import datetime
import config

# Manually override for standalone run
config.INPUT_DIR = "/app/data/input"
config.TEMP_DIR = "/app/data/temp"

def generate_bad_data():
    file_name = f"bad_data_{uuid.uuid4()}.csv"
    temp_path = os.path.join(config.TEMP_DIR, file_name)
    final_path = os.path.join(config.INPUT_DIR, file_name)
    
    os.makedirs(config.TEMP_DIR, exist_ok=True)
    os.makedirs(config.INPUT_DIR, exist_ok=True)

    print(f"Generating bad data: {file_name}")
    
    with open(temp_path, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["event_id", "event_time", "event_type", "product_id", "price", "user_id", "session_id"])
        
        # Row 1: Negative Price
        writer.writerow([str(uuid.uuid4()), datetime.now(), "view", 101, -50.00, 1001, "sess1"])
        
        # Row 2: Invalid Event Type
        writer.writerow([str(uuid.uuid4()), datetime.now(), "unknown_event", 102, 10.00, 1002, "sess2"])
        
        # Row 3: Valid Row (Control)
        writer.writerow([str(uuid.uuid4()), datetime.now(), "purchase", 103, 100.00, 1003, "sess3"])

    shutil.move(temp_path, final_path)
    print("Bad data injected.")

if __name__ == "__main__":
    generate_bad_data()
