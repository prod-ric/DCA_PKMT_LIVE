# ============================================
# FILE: database.py (FIXED - PERIODIC SAVES)
# ============================================

"""
Data storage for Polymarket orderbook data using Parquet files
- Saves periodically (not just on buffer full)
- Uses incremental file chunks to avoid rewriting large files
- Merges on shutdown
"""
import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq
from threading import Lock, Thread

logger = logging.getLogger(__name__)


class Database:
    """Handles Parquet file storage for orderbook data"""
    
    # Schema for snapshots
    SNAPSHOT_SCHEMA = pa.schema([
        ('timestamp', pa.int64()),
        ('datetime', pa.timestamp('ms')),
        ('asset_id', pa.string()),
        ('market_id', pa.string()),
        ('mid_price', pa.float64()),
        ('best_bid', pa.float64()),
        ('best_ask', pa.float64()),
        ('spread', pa.float64()),
        ('raw_json', pa.string()),
    ])
    
    def __init__(self, data_dir: str = "data", flush_interval: int = 60):
        """
        Initialize database
        
        Args:
            data_dir: Directory for data files
            flush_interval: Seconds between automatic flushes (default 60s)
        """
        self.data_dir = data_dir
        self.chunks_dir = os.path.join(data_dir, "chunks")
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(self.chunks_dir, exist_ok=True)
        
        # Buffer for batching writes
        self.buffer: List[Dict] = []
        self.buffer_lock = Lock()
        
        # File paths
        self.snapshot_file = os.path.join(data_dir, "snapshots.parquet")
        
        # Chunk tracking
        self.chunk_counter = self._get_next_chunk_number()
        self.flush_interval = flush_interval
        self.last_flush_time = time.time()
        
        # Stats
        self.total_records_written = 0
        
        # Start background flush thread
        self.running = True
        self.flush_thread = Thread(target=self._periodic_flush_loop, daemon=True)
        self.flush_thread.start()
        
        logger.info(f"Database initialized: {data_dir}")
        logger.info(f"Auto-flush every {flush_interval}s")
    
    def _get_next_chunk_number(self) -> int:
        """Find the next chunk number based on existing files"""
        existing = [f for f in os.listdir(self.chunks_dir) if f.startswith('chunk_') and f.endswith('.parquet')]
        if not existing:
            return 0
        
        numbers = []
        for f in existing:
            try:
                num = int(f.replace('chunk_', '').replace('.parquet', ''))
                numbers.append(num)
            except:
                pass
        
        return max(numbers) + 1 if numbers else 0
    
    def _periodic_flush_loop(self):
        """Background thread that flushes periodically"""
        while self.running:
            time.sleep(10)  # Check every 10 seconds
            
            # Flush if interval passed and buffer has data
            with self.buffer_lock:
                time_since_flush = time.time() - self.last_flush_time
                has_data = len(self.buffer) > 0
            
            if has_data and time_since_flush >= self.flush_interval:
                self.flush()
    
    def _parse_price(self, price_str: str) -> float:
        """Parse price string (handles '.48' format)"""
        if not price_str:
            return 0.0
        if str(price_str).startswith('.'):
            return float('0' + str(price_str))
        return float(price_str)
    
    def save_book_snapshot(self, data: Dict) -> None:
        """Save a full orderbook snapshot"""
        try:
            timestamp_ms = int(data.get('timestamp', datetime.now().timestamp() * 1000))
            asset_id = data.get('asset_id', '')
            market_id = data.get('market', '')
            
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            best_bid = None
            best_ask = None
            
            if bids:
                bid_prices = [self._parse_price(b['price']) for b in bids]
                best_bid = max(bid_prices) if bid_prices else None
            
            if asks:
                ask_prices = [self._parse_price(a['price']) for a in asks]
                best_ask = min(ask_prices) if ask_prices else None
            
            mid_price = None
            spread = None
            if best_bid is not None and best_ask is not None:
                mid_price = (best_bid + best_ask) / 2
                spread = best_ask - best_bid
            
            record = {
                'timestamp': timestamp_ms,
                'datetime': datetime.fromtimestamp(timestamp_ms / 1000),
                'asset_id': asset_id,
                'market_id': market_id,
                'mid_price': mid_price,
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'raw_json': json.dumps(data),
            }
            
            with self.buffer_lock:
                self.buffer.append(record)
            
        except Exception as e:
            logger.error(f"Error saving book snapshot: {e}")
    
    def save_price_change(self, data: Dict) -> None:
        """Save price change events"""
        try:
            timestamp_ms = int(data.get('timestamp', datetime.now().timestamp() * 1000))
            market_id = data.get('market', '')
            
            for change in data.get('price_changes', []):
                asset_id = change.get('asset_id', '')
                
                best_bid = self._parse_price(change.get('best_bid')) if change.get('best_bid') else None
                best_ask = self._parse_price(change.get('best_ask')) if change.get('best_ask') else None
                
                mid_price = None
                spread = None
                if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
                    mid_price = (best_bid + best_ask) / 2
                    spread = best_ask - best_bid
                
                record = {
                    'timestamp': timestamp_ms,
                    'datetime': datetime.fromtimestamp(timestamp_ms / 1000),
                    'asset_id': asset_id,
                    'market_id': market_id,
                    'mid_price': mid_price,
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'spread': spread,
                    'raw_json': json.dumps(change),
                }
                
                with self.buffer_lock:
                    self.buffer.append(record)
            
        except Exception as e:
            logger.error(f"Error saving price change: {e}")
    
    def flush(self) -> int:
        """
        Flush buffer to a new chunk file
        Returns number of records written
        """
        with self.buffer_lock:
            if not self.buffer:
                return 0
            
            # Take buffer and reset
            records = self.buffer.copy()
            self.buffer.clear()
            self.last_flush_time = time.time()
        
        try:
            # Write to chunk file
            chunk_file = os.path.join(self.chunks_dir, f"chunk_{self.chunk_counter:06d}.parquet")
            
            table = pa.Table.from_pylist(records, schema=self.SNAPSHOT_SCHEMA)
            pq.write_table(table, chunk_file, compression='snappy')
            
            self.chunk_counter += 1
            self.total_records_written += len(records)
            
            logger.info(f"Flushed {len(records):,} records to {chunk_file}")
            return len(records)
            
        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")
            # Put records back in buffer
            with self.buffer_lock:
                self.buffer = records + self.buffer
            return 0
    
    def merge_chunks(self) -> str:
        """
        Merge all chunk files into final snapshots.parquet
        Call this on shutdown or periodically
        
        Returns:
            Path to merged file
        """
        # First flush any remaining buffer
        self.flush()
        
        chunk_files = sorted([
            os.path.join(self.chunks_dir, f) 
            for f in os.listdir(self.chunks_dir) 
            if f.startswith('chunk_') and f.endswith('.parquet')
        ])
        
        if not chunk_files:
            logger.info("No chunks to merge")
            return self.snapshot_file
        
        logger.info(f"Merging {len(chunk_files)} chunk files...")
        
        try:
            # Read all chunks
            tables = []
            
            # Include existing main file if exists
            if os.path.exists(self.snapshot_file):
                tables.append(pq.read_table(self.snapshot_file))
            
            # Read all chunks
            for chunk_file in chunk_files:
                tables.append(pq.read_table(chunk_file))
            
            # Concatenate
            if tables:
                combined = pa.concat_tables(tables)
                
                # Sort by timestamp
                combined = combined.sort_by('timestamp')
                
                # Write merged file
                pq.write_table(combined, self.snapshot_file, compression='snappy')
                
                logger.info(f"Merged {len(combined):,} total records to {self.snapshot_file}")
                
                # Delete chunk files
                for chunk_file in chunk_files:
                    os.remove(chunk_file)
                
                logger.info(f"Deleted {len(chunk_files)} chunk files")
            
            return self.snapshot_file
            
        except Exception as e:
            logger.error(f"Error merging chunks: {e}")
            return self.snapshot_file
    
    def get_snapshot_count(self) -> int:
        """Get total number of snapshots (including buffer and chunks)"""
        count = 0
        
        # Count in main file
        if os.path.exists(self.snapshot_file):
            try:
                count += pq.read_metadata(self.snapshot_file).num_rows
            except:
                pass
        
        # Count in chunks
        chunk_files = [
            os.path.join(self.chunks_dir, f) 
            for f in os.listdir(self.chunks_dir) 
            if f.startswith('chunk_') and f.endswith('.parquet')
        ]
        for chunk_file in chunk_files:
            try:
                count += pq.read_metadata(chunk_file).num_rows
            except:
                pass
        
        # Count in buffer
        with self.buffer_lock:
            count += len(self.buffer)
        
        return count
    
    def get_buffer_size(self) -> int:
        """Get current buffer size"""
        with self.buffer_lock:
            return len(self.buffer)
    
    def get_chunk_count(self) -> int:
        """Get number of chunk files"""
        return len([f for f in os.listdir(self.chunks_dir) if f.startswith('chunk_') and f.endswith('.parquet')])
    
# In database.py, update the shutdown method:

    def shutdown(self):
        """Shutdown database - stop thread, flush and merge"""
        logger.info("Database shutting down...")
        
        # Stop background thread
        self.running = False
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)
        
        # Final flush
        self.flush()
        
        # Merge all chunks
        self.merge_chunks()
        
        logger.info("Database shutdown complete")
