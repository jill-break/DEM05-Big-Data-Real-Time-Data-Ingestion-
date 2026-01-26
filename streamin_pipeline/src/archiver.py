import time
import os
import shutil
import sys
from datetime import datetime, timedelta

import config
from logger_utils import setup_logger

# --- Logging Setup ---
LOG_FILE = os.path.join(config.LOG_DIR, "archiver.log")
logger = setup_logger("Archiver", LOG_FILE)

def archive_files():
    """
    Moves files from INPUT_DIR to ARCHIVE_DIR if they are older than threshold.
    """
    # Ensure archive directory exists
    os.makedirs(config.ARCHIVE_DIR, exist_ok=True)
    
    threshold_minutes = config.ARCHIVE_AGE_MINUTES
    cutoff_time = datetime.now() - timedelta(minutes=threshold_minutes)
    
    logger.info(f"Scanning for files older than {threshold_minutes} minutes (before {cutoff_time})...")
    
    # List files in input directory
    files = [f for f in os.listdir(config.INPUT_DIR) if f.endswith('.csv')]
    
    moved_count = 0
    
    for file_name in files:
        source_path = os.path.join(config.INPUT_DIR, file_name)
        archive_path = os.path.join(config.ARCHIVE_DIR, file_name)
        
        try:
            # Check file modification time
            file_mtime = datetime.fromtimestamp(os.path.getmtime(source_path))
            
            if file_mtime < cutoff_time:
                shutil.move(source_path, archive_path)
                logger.info(f"Archived: {file_name}")
                moved_count += 1
                
        except Exception as e:
            logger.error(f"Failed to archive {file_name}: {e}")
            
    if moved_count > 0:
        logger.info(f"Run Complete: Archived {moved_count} files.")
    else:
        logger.info("No files to archive.")

if __name__ == "__main__":
    # Force unbuffered output 
    sys.stdout.reconfigure(line_buffering=True) # type: ignore
    
    logger.info("Starting Archiver Service...")
    logger.info(f"Monitoring: {config.INPUT_DIR}")
    logger.info(f"Archiving to: {config.ARCHIVE_DIR}")
    
    try:
        while True:
            archive_files()
            # Run every minute
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Archiver stopped by user.")
    except Exception as e:
        logger.critical(f"Archiver crashed: {e}", exc_info=True)
