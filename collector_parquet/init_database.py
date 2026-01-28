# ============================================
# FILE: init_database.py (UPDATED)
# ============================================

"""
Script to initialize/verify the Parquet storage
"""
import sys
import os
from database import Database
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database():
    """Initialize the Parquet storage"""
    try:
        Config.validate()
        
        logger.info("Initializing Parquet storage...")
        db = Database(Config.DATA_DIR)
        
        logger.info("✓ Parquet storage initialized successfully!")
        logger.info(f"✓ Data directory: {Config.DATA_DIR}/")
        logger.info(f"✓ Snapshot file: {db.snapshot_file}")
        
        # Check if file exists
        if os.path.exists(db.snapshot_file):
            count = db.get_snapshot_count()
            logger.info(f"✓ Existing snapshots: {count:,}")
        else:
            logger.info("✓ Ready to collect data (no existing file)")
        
        return True
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return False


if __name__ == "__main__":
    success = init_database()
    sys.exit(0 if success else 1)


## Key Changes:
"""

| Before (CSV) | After (Parquet) |
|--------------|-----------------|
| Multiple CSV files | Single `snapshots.parquet` file |
| Write every row | Buffer 100 rows, then write |
| Slow appends | Fast compressed appends |
| Large files | ~5x smaller with Snappy compression |
| Slow reads | Fast columnar reads |

## Schema:
```
snapshots.parquet:
├── timestamp (int64)      - Unix ms
├── datetime (timestamp)   - For easy filtering
├── asset_id (string)
├── market_id (string)
├── mid_price (float64)
├── best_bid (float64)
├── best_ask (float64)
├── spread (float64)
├── raw_json (string)      - Full orderbook for replay
"""