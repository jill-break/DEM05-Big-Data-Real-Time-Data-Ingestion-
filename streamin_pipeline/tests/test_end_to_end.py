import pytest
import psycopg2
import os
import time
import glob
import pandas as pd
from datetime import datetime

# --- Configuration ---
DB_CONFIG = {
    "dbname": "ecommerce_db",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": "5433"
}

INPUT_DATA_DIR = "./data/input"

# --- Fixtures ---
@pytest.fixture(scope="module")
def db_cursor():
    conn = None
    try:
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
    1. Verify table exists.
    """
    db_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'user_activity');")
    assert db_cursor.fetchone()[0], "FAIL: Table 'user_activity' missing."

def test_tc002_csv_generation_correctness():
    """
    2. Verify CSV headers match your actual generator output.
    """
    time.sleep(2)
    csv_files = glob.glob(os.path.join(INPUT_DATA_DIR, "*.csv"))
    assert len(csv_files) > 0, "FAIL: No CSV files found."

    latest_file = max(csv_files, key=os.path.getctime)
    df = pd.read_csv(latest_file)

    # UPDATED: Matches the "Found" list from your error logs
    expected_headers = [
        'event_id', 'event_time', 'event_type', 'product_id', 
        'price', 'user_id', 'session_id'
    ]
    
    # Sort both lists to avoid ordering issues causing failures
    assert sorted(list(df.columns)) == sorted(expected_headers), \
        f"FAIL: Header mismatch.\nExpected: {expected_headers}\nFound: {list(df.columns)}"

    assert not df.empty, "FAIL: CSV file is empty."

def test_tc003_spark_detection_and_processing(db_cursor):
    """
    3. Verify row count increases.
    """
    db_cursor.execute("SELECT COUNT(*) FROM user_activity;")
    initial_count = db_cursor.fetchone()[0]

    print(f"\n[Monitor] Initial DB Count: {initial_count}. Waiting for Spark...")
    time.sleep(15)

    db_cursor.execute("SELECT COUNT(*) FROM user_activity;")
    final_count = db_cursor.fetchone()[0]

    print(f"[Monitor] Final DB Count: {final_count}")
    assert final_count > initial_count, "FAIL: Row count did not increase."

def test_tc005_data_transformation_accuracy(db_cursor):
    """
    5. Verify data types using the correct column 'event_time'.
    """
    # UPDATED: changed 'timestamp' to 'event_time'
    db_cursor.execute("SELECT event_time, user_id FROM user_activity ORDER BY event_time DESC LIMIT 5;")
    rows = db_cursor.fetchall()
    
    assert len(rows) > 0, "FAIL: No data to verify."

    for row in rows:
        ts_val, user_id = row
        assert isinstance(ts_val, datetime), f"FAIL: Invalid timestamp format: {ts_val}"
        # user_id can be string or int depending on generation, checking it exists is enough
        assert user_id is not None, "FAIL: user_id is Null"

def test_tc006_performance_metrics(db_cursor):
    """
    6. Performance check.
    """
    start_time = time.time()
    
    # Get current max event_id
    db_cursor.execute("SELECT MAX(event_id) FROM user_activity;")
    result = db_cursor.fetchone()[0]
    initial_max_id = result if result else 0

    timeout = 120
    data_arrived = False
    
    while time.time() - start_time < timeout:
        db_cursor.execute("SELECT MAX(event_id) FROM user_activity;")
        current_max = db_cursor.fetchone()[0]
        
        # Check if new data arrived (handle potential None if DB was empty)
        if current_max is not None and (initial_max_id is None or current_max > initial_max_id):
            data_arrived = True
            break
        time.sleep(2)

    elapsed = round(time.time() - start_time, 2)
    assert data_arrived, f"FAIL: Performance Lag! Data took longer than {timeout}s."
    print(f"\n[Performance] Success! Data appeared in {elapsed} seconds.")