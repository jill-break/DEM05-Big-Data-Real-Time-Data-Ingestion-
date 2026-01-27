import os

# Database Configuration
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "ecommerce_db")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not DB_USER or not DB_PASSWORD:
    raise ValueError("DB_USER and DB_PASSWORD environment variables must be set.")

DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_DRIVER = "org.postgresql.Driver"

# Spark Configuration
APP_NAME = os.getenv("SPARK_APP_NAME", "EcommerceStreamingPipeline")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/app/data/checkpoints/postgres_sink")
INPUT_DIR = os.getenv("INPUT_DIR", "/app/data/input")
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")

# Data Generator & Archiver Configuration
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
TEMP_DIR = os.path.join(DATA_DIR, "temp")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")
ARCHIVE_AGE_MINUTES = int(os.getenv("ARCHIVE_AGE_MINUTES", "1")) # Archive quickly for demo

# Data Generation Configuration
BATCH_SIZE_MIN = int(os.getenv("BATCH_SIZE_MIN", "50"))
BATCH_SIZE_MAX = int(os.getenv("BATCH_SIZE_MAX", "100"))
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", "5"))
