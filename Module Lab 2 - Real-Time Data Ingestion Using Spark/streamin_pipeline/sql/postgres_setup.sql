-- sql/postgres_setup.sql

-- 1. Create the table with strict data types
CREATE TABLE IF NOT EXISTS user_activity (
    event_id VARCHAR(50) PRIMARY KEY, -- Unique ID for the event
    event_time TIMESTAMP NOT NULL,    -- Accurate timestamp for time-series analysis
    event_type VARCHAR(20),           -- 'view' or 'purchase'
    product_id INTEGER,
    price DECIMAL(10, 2),             -- DECIMAL is better for currency than FLOAT
    user_id INTEGER,
    session_id VARCHAR(50)
);

-- 2. Create an index on event_time for faster queries later
-- (Optional but best practice for time-series data)
CREATE INDEX idx_event_time ON user_activity(event_time);

-- 3. Verification message (will show in logs)
DO $$
BEGIN
    RAISE NOTICE 'Table user_activity created successfully.';
END $$;