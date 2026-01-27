# Comprehensive Test Plan

This document outlines the manual and automated test strategies used to verify the Spark Streaming Pipeline.

## 1. Functional Testing (Happy Path)

| Test Case ID | Feature | Description | Steps to Reproduce | Expected Outcome | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-001** | **Initialization** | Verify all containers start correctly. | 1. Run `docker-compose up -d`<br>2. Run `docker ps` | Containers `postgres_db`, `spark_master`, `spark_worker`, `file_archiver` are UP. | PASS |
| **TC-002** | **Schema Creation** | Verify Postgres tables exist. | 1. `docker exec -it postgres_db psql ...`<br>2. `\dt` | Tables `user_activity` and `user_activity_errors` exist. | PASS |
| **TC-003** | **Data Ingestion** | Verify valid events are ingested. | 1. Run `data_generator.py` (default config).<br>2. Wait 30s.<br>3. Query `user_activity`. | Row count increases. `event_type` is valid ('view', 'purchase'). | PASS |

## 2. Data Quality & Error Handling

| Test Case ID | Feature | Description | Steps to Reproduce | Expected Outcome | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-004** | **DLQ Routing** | Verify invalid data is sent to DLQ. | 1. Run `python3 src/inject_bad_data.py`<br>2. Query `user_activity_errors`. | Rows with `price < 0` or invalid `event_type` appear in error table. | PASS |
| **TC-005** | **Null Handling** | Verify missing fields are handled. | 1. Inject row with `user_id=NULL`. | Row is rejected and sent to DLQ (or filtered based on logic). | PASS |

## 3. Operational Features

| Test Case ID | Feature | Description | Steps to Reproduce | Expected Outcome | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-006** | **File Archiving** | Verify processed files are moved. | 1. Check `data/input` count.<br>2. Wait > `ARCHIVE_AGE_MINUTES`.<br>3. Check `data/archive`. | Files move from Input -> Archive. Input folder stays clean. | PASS |
| **TC-007** | **Graceful Shutdown** | Verify clean exit on stop. | 1. Run `docker-compose stop spark_master`. | Logs show `Stopping Spark Query...` and no lock files remain. | PASS |

## 4. Performance & Scalability

| Test Case ID | Feature | Description | Steps to Reproduce | Expected Outcome | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-008** | **Stress Test** | Verify system stability under load. | 1. Set `BATCH_SIZE_MAX=5000` in docker-compose.<br>2. Run for 2 mins. | Ingestion rate > 3000 events/sec. No crash. DLQ empty (if data valid). | PASS |

---

## Automated Testing

To run the end-to-end integration tests (which cover TC-001 to TC-004 automatically):

```bash
# Windows
run_tests.bat

# Linux/Mac
pytest tests/test_end_to_end.py -v -s
```