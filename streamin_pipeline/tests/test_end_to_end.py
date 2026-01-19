import pytest
import psycopg2
import os
import time
import glob
import pandas as pd
import logging
import sys
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
LOG_DIR = "./logs"
LOG_FILE = os.path.join(LOG_DIR, "test_execution.log")

# --- Logging Setup ---
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
logger = logging.getLogger(__name__)

# --- Fixtures ---
@pytest.fixture(scope="module")
def db_cursor():
    logger.info("Setting up Database Connection...")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG) # type: ignore
        conn.autocommit = True
        cursor = conn.cursor()
        yield cursor
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to Database: {e}")
        pytest.fail(f"Could not connect to Database: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database Connection Closed.")

# --- Test Cases ---

def test_tc001_verify_database_initialization(db_cursor):
    """
    1. Verify table exists.
    """
    logger.info("Running TC-001: Verify Database Initialization...")
    db_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'user_activity');")
    exists = db_cursor.fetchone()[0]
    
    if exists:
        logger.info("TC-001 Passed: Table 'user_activity' exists.")
    else:
        logger.error("TC-001 Failed: Table 'user_activity' missing.")
        
    assert exists, "FAIL: Table 'user_activity' missing."

def test_tc002_csv_generation_correctness():
    """
    2. Verify CSV headers match your actual generator output.
    """
    logger.info("Running TC-002: Verify CSV Generation...")
    time.sleep(2)
    csv_files = glob.glob(os.path.join(INPUT_DATA_DIR, "*.csv"))
    
    if not csv_files:
        logger.error("TC-002 Failed: No CSV files found.")
        pytest.fail("FAIL: No CSV files found.")

    latest_file = max(csv_files, key=os.path.getctime)
    logger.info(f"Inspecting file: {latest_file}")
    df = pd.read_csv(latest_file)

    expected_headers = [
        'event_id', 'event_time', 'event_type', 'product_id', 
        'price', 'user_id', 'session_id'
    ]
    
    # Sort both lists to avoid ordering issues causing failures
    headers_match = sorted(list(df.columns)) == sorted(expected_headers)
    
    if headers_match:
         logger.info("TC-002 Passed: Headers match expectation.")
    else:
         logger.error(f"TC-002 Failed: Header mismatch. Found {list(df.columns)}")

    assert headers_match, f"FAIL: Header mismatch.\nExpected: {expected_headers}\nFound: {list(df.columns)}"
    assert not df.empty, "FAIL: CSV file is empty."

def test_tc003_spark_detection_and_processing(db_cursor):
    """
    3. Verify row count increases.
    """
    logger.info("Running TC-003: Verify Spark Data Processing...")
    
    db_cursor.execute("SELECT COUNT(*) FROM user_activity;")
    initial_count = db_cursor.fetchone()[0]
    logger.info(f"Initial DB Count: {initial_count}")

    logger.info("Waiting 15 seconds for Spark processing...")
    time.sleep(15)

    db_cursor.execute("SELECT COUNT(*) FROM user_activity;")
    final_count = db_cursor.fetchone()[0]
    logger.info(f"Final DB Count: {final_count}")

    if final_count > initial_count:
        logger.info(f"TC-003 Passed: Row count increased by {final_count - initial_count}.")
    else:
        logger.error("TC-003 Failed: Row count did not increase.")

    assert final_count > initial_count, "FAIL: Row count did not increase."

def test_tc005_data_transformation_accuracy(db_cursor):
    """
    5. Verify data types using the correct column 'event_time'.
    """
    logger.info("Running TC-005: Verify Data Transformation...")
    
    db_cursor.execute("SELECT event_time, user_id FROM user_activity ORDER BY event_time DESC LIMIT 5;")
    rows = db_cursor.fetchall()
    
    if not rows:
        logger.error("TC-005 Failed: No data found in DB.")
        pytest.fail("FAIL: No data to verify.")

    for row in rows:
        ts_val, user_id = row
        assert isinstance(ts_val, datetime), f"FAIL: Invalid timestamp format: {ts_val}"
        assert user_id is not None, "FAIL: user_id is Null"
    
    logger.info("TC-005 Passed: Data types verified successfully.")

def test_tc006_performance_metrics(db_cursor):
    """
    6. Performance check.
    """
    logger.info("Running TC-006: Performance Metrics Check...")
    start_time = time.time()
    
    # Get current max event_id
    db_cursor.execute("SELECT MAX(event_id) FROM user_activity;")
    result = db_cursor.fetchone()[0]
    initial_max_id = result if result else 0
    logger.info(f"Starting Max ID: {initial_max_id}")

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
    
    if data_arrived:
        logger.info(f"TC-006 Passed: New data appeared in {elapsed} seconds.")
    else:
        logger.error(f"TC-006 Failed: Timeout after {timeout} seconds.")

    assert data_arrived, f"FAIL: Performance Lag! Data took longer than {timeout}s."