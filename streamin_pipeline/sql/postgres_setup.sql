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
-- (best practice for time-series data)
CREATE INDEX idx_event_time ON user_activity(event_time);

-- 3. Verification message (will show in logs)
    RAISE NOTICE 'Table user_activity created successfully.';
END $$;

-- 4. Create the Dead Letter Queue (DLQ) table for invalid rows
CREATE TABLE IF NOT EXISTS user_activity_errors (
    error_id SERIAL PRIMARY KEY,
    event_id VARCHAR(50),
    event_time VARCHAR(50), -- Store as string since it might be malformed
    event_type VARCHAR(20),
    product_id VARCHAR(20), -- Store as string
    price VARCHAR(20),      -- Store as string
    user_id VARCHAR(20),    -- Store as string
    session_id VARCHAR(50),
    error_message TEXT,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DO $$
BEGIN
    RAISE NOTICE 'Table user_activity_errors created successfully.';
END $$;