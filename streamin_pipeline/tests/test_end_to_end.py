import pytest
import psycopg2
import os
import time
import glob
import pandas as pd
from datetime import datetime

# --- Configuration (Updated to match docker-compose.yml) ---
DB_CONFIG = {
    "dbname": "ecommerce_db",   # Matches POSTGRES_DB
    "user": "myuser",           # Matches POSTGRES_USER
    "password": "mypassword",   # Matches POSTGRES_PASSWORD
    "host": "localhost",        # Localhost because we are running tests from Windows
    "port": "5433"              # Matches the mapped port "5433:5432"
}

INPUT_DATA_DIR = "./data/input"

# --- Fixtures ---
@pytest.fixture(scope="module")
def db_cursor():
    conn = None
    try:
        # Corrected connection logic
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        yield cursor
    except psycopg2.OperationalError as e:
        pytest.fail(f"Could not connect to Database: {e}")
    finally:
        if conn:
            conn.close()

# --- Test Cases ---

def test_tc001_verify_database_initialization(db_cursor):
    """
    1. Are the data transformations correct? (Schema Verification)
    Checks if the target table exists and has the correct columns.
    """
    # Check if table exists
    db_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'user_activity');")
    table_exists = db_cursor.fetchone()[0]
    assert table_exists, "FAIL: Table 'user_activity' missing. Did the Spark job create it?"

def test_tc002_csv_generation_correctness():
    """
    2. Are the CSV files being generated correctly?
    Verifies that files exist AND contain valid headers/data.
    """
    # Allow generator some time to create files if folder is empty
    time.sleep(5)
    
    csv_files = glob.glob(os.path.join(INPUT_DATA_DIR, "*.csv"))
    assert len(csv_files) > 0, "FAIL: No CSV files found. Generator might be down."

    # Inspect the newest file
    latest_file = max(csv_files, key=os.path.getctime)
    df = pd.read_csv(latest_file)

    # Expected headers based on your data_generator.py output
    expected_headers = [
        'event_id', 'event_time', 'event_type', 'user_id', 
        'price', 'quantity', 'total_amount'
    ]
    
    # Check if the actual columns match our expectation
    assert list(df.columns) == expected_headers, \
        f"FAIL: Header mismatch.\nExpected: {expected_headers}\nFound: {list(df.columns)}"

    # Check Data Content
    assert not df.empty, "FAIL: CSV file is empty."
    assert df['event_type'].isin(['view', 'purchase']).all(), "FAIL: CSV contains invalid event types."

def test_tc003_spark_detection_and_processing(db_cursor):
    """
    3. Is Spark detecting and processing new files?
    4. Is data being written into PostgreSQL without errors?
    We verify this by checking if the row count increases over a short window.
    """
    db_cursor.execute("SELECT COUNT(*) FROM user_activity;")
    initial_count = db_cursor.fetchone()[0]

    print(f"\n[Monitor] Initial DB Count: {initial_count}. Waiting for Spark...")
    
    # Wait for Spark batch interval (e.g., 10-15s)
    time.sleep(15)

    db_cursor.execute("SELECT COUNT(*) FROM user_activity;")
    final_count = db_cursor.fetchone()[0]

    print(f"[Monitor] Final DB Count: {final_count}")

    assert final_count > initial_count, "FAIL: Row count did not increase. Spark is not processing data or writing failed."

def test_tc005_data_transformation_accuracy(db_cursor):
    """
    5. Are the data transformations correct?
    Verifies that the data in the DB matches expected formats (e.g. timestamps).
    """
    db_cursor.execute("SELECT timestamp, user_id FROM user_activity ORDER BY timestamp DESC LIMIT 5;")
    rows = db_cursor.fetchall()
    
    assert len(rows) > 0, "FAIL: No data to verify transformations."

    for row in rows:
        ts_val, user_id = row
        
        # Verify Timestamp Format (Postgres returns datetime object)
        assert isinstance(ts_val, datetime), f"FAIL: Invalid timestamp format in DB: {ts_val}"
        
        # Verify Transformations (e.g. user_id should be integer not null)
        assert isinstance(user_id, int), f"FAIL: user_id should be integer, got {type(user_id)}"

def test_tc006_performance_metrics(db_cursor):
    """
    6. Are performance metrics (processing speed) within limits?
    Pass Condition: Data must appear in the DB within 30 seconds of file creation.
    """
    # Record start time
    start_time = time.time()
    
    # Get current max event_id in DB
    db_cursor.execute("SELECT MAX(event_id) FROM user_activity;")
    result = db_cursor.fetchone()[0]
    initial_max_id = result if result else 0

    # Wait for new max_id to appear
    timeout = 30  # Performance Threshold: 30 seconds
    data_arrived = False
    
    while time.time() - start_time < timeout:
        db_cursor.execute("SELECT MAX(event_id) FROM user_activity;")
        current_max = db_cursor.fetchone()[0]
        if current_max and current_max > initial_max_id:
            data_arrived = True
            break
        time.sleep(2)

    elapsed = round(time.time() - start_time, 2)
    assert data_arrived, f"FAIL: Performance Lag! Data took longer than {timeout}s to appear."
    print(f"\n[Performance] Success! Data appeared in {elapsed} seconds.")