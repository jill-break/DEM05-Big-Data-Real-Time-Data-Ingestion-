# Manual Test Plan

| Test Case ID | Description | Steps to Reproduce | Expected Outcome | Actual Outcome | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-001** | Verify Database Initialization | 1. Run `docker-compose up`.<br>2. Check Postgres tables. | Table `user_activity` should exist with correct schema. | Table exists with all columns. | PASS |
| **TC-002** | Data Generation | 1. Run `data_generator.py`.<br>2. Check `data/input` folder. | CSV files should appear every 5 seconds. | Files `events_1.csv`, `events_2.csv` created. | PASS |
| **TC-003** | Spark Job Execution | 1. Submit Spark job.<br>2. Check terminal logs. | Logs show "Processing Batch ID: X". | Logs confirmed batch processing. | PASS |
| **TC-004** | Data Persistence | 1. Query Postgres count.<br>2. Wait 10 seconds.<br>3. Query count again. | Row count should increase. | Count increased from 0 to 2012. | PASS |
| **TC-005** | Data Integrity | 1. Select one row from DB.<br>2. Compare `event_type`. | `event_type` should be 'view' or 'purchase'. | Valid data found. | PASS |