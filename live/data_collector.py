"""
Data collector for live orderbook data.

Saves orderbook snapshots to parquet chunks — same schema/format
as collector_parquet/database.py — so they can be fed straight into
the reconstruct_orderbooks pipeline for later backtesting.
"""

import os
import json
import logging
import time
import threading
from datetime import datetime
from typing import Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class DataCollector:
    """
    Collects orderbook data and writes to parquet chunks.

    Uses a background thread for writing to avoid blocking the main loop.
    Data is buffered in memory and flushed periodically.

    The schema includes **orderbook_bids / orderbook_asks** columns so
    the chunks can be consumed by ``reconstruct_orderbooks.py`` or loaded
    directly for analysis.
    """

    SNAPSHOT_SCHEMA = pa.schema(
        [
            ("timestamp", pa.int64()),
            ("datetime", pa.timestamp("ms")),
            ("asset_id", pa.string()),
            ("market_id", pa.string()),
            ("mid_price", pa.float64()),
            ("best_bid", pa.float64()),
            ("best_ask", pa.float64()),
            ("spread", pa.float64()),
            ("orderbook_bids", pa.string()),
            ("orderbook_asks", pa.string()),
        ]
    )

    def __init__(
        self,
        data_dir: str = "data",
        flush_interval: int = 60,
        buffer_size: int = 1000,
    ):
        self.data_dir = data_dir
        self.chunks_dir = os.path.join(data_dir, "chunks")
        os.makedirs(self.chunks_dir, exist_ok=True)

        self.flush_interval = flush_interval
        self.buffer_size = buffer_size

        self.buffer: List[Dict] = []
        self.buffer_lock = threading.Lock()

        self.chunk_counter = self._get_next_chunk_number()
        self.last_flush_time = time.time()

        self.total_records = 0
        self.records_buffered = 0

        self.running = True
        self.flush_thread = threading.Thread(
            target=self._periodic_flush_loop, daemon=True
        )
        self.flush_thread.start()

        self.snapshot_file = os.path.join(data_dir, "snapshots.parquet")

        logger.info(f"DataCollector initialised: {data_dir}")
        logger.info(f"  Flush interval: {flush_interval}s, buffer size: {buffer_size}")

    # ------------------------------------------------------------------ helpers

    def _get_next_chunk_number(self) -> int:
        if not os.path.exists(self.chunks_dir):
            return 0
        existing = [
            f
            for f in os.listdir(self.chunks_dir)
            if f.startswith("chunk_") and f.endswith(".parquet")
        ]
        if not existing:
            return 0
        numbers = []
        for f in existing:
            try:
                numbers.append(int(f.replace("chunk_", "").replace(".parquet", "")))
            except ValueError:
                pass
        return max(numbers) + 1 if numbers else 0

    def _periodic_flush_loop(self):
        while self.running:
            time.sleep(10)
            with self.buffer_lock:
                time_since = time.time() - self.last_flush_time
                has_data = len(self.buffer) > 0
            if has_data and time_since >= self.flush_interval:
                self.flush()

    # ------------------------------------------------------------------ record

    def record_tick(self, tick) -> None:
        """Record an OrderbookTick from orderbook_feed."""
        try:
            timestamp_ms = int(tick.timestamp.timestamp() * 1000)
            record = {
                "timestamp": timestamp_ms,
                "datetime": tick.timestamp,
                "asset_id": tick.asset_id,
                "market_id": tick.market_id,
                "mid_price": tick.mid_price,
                "best_bid": tick.best_bid,
                "best_ask": tick.best_ask,
                "spread": tick.spread,
                "orderbook_bids": json.dumps(tick.bids) if tick.bids else "[]",
                "orderbook_asks": json.dumps(tick.asks) if tick.asks else "[]",
            }
            with self.buffer_lock:
                self.buffer.append(record)
                self.records_buffered += 1
                if len(self.buffer) >= self.buffer_size:
                    self._flush_unlocked()
        except Exception as e:
            logger.error(f"Error recording tick: {e}")

    def record_raw(self, data: Dict) -> None:
        """Record raw orderbook data dict."""
        try:
            ts = data.get("timestamp")
            if isinstance(ts, (int, float)):
                timestamp_ms = int(ts) if ts > 1e12 else int(ts * 1000)
                dt = datetime.fromtimestamp(timestamp_ms / 1000)
            else:
                dt = datetime.utcnow()
                timestamp_ms = int(dt.timestamp() * 1000)

            bids = data.get("bids", [])
            asks = data.get("asks", [])

            best_bid = (
                max((float(b.get("price", 0)) for b in bids), default=0.0)
                if bids
                else 0.0
            )
            best_ask = (
                min((float(a.get("price", 0)) for a in asks), default=0.0)
                if asks
                else 0.0
            )
            mid_price = (
                (best_bid + best_ask) / 2
                if best_bid > 0 and best_ask > 0
                else 0.0
            )
            spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0

            record = {
                "timestamp": timestamp_ms,
                "datetime": dt,
                "asset_id": data.get("asset_id", ""),
                "market_id": data.get("market", ""),
                "mid_price": mid_price,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "orderbook_bids": json.dumps(bids),
                "orderbook_asks": json.dumps(asks),
            }
            with self.buffer_lock:
                self.buffer.append(record)
                self.records_buffered += 1
                if len(self.buffer) >= self.buffer_size:
                    self._flush_unlocked()
        except Exception as e:
            logger.error(f"Error recording raw data: {e}")

    # ------------------------------------------------------------------ flush

    def _flush_unlocked(self) -> int:
        """Must be called with buffer_lock held."""
        if not self.buffer:
            return 0
        records = self.buffer.copy()
        self.buffer.clear()
        self.last_flush_time = time.time()
        return self._write_chunk(records)

    def _write_chunk(self, records: List[Dict]) -> int:
        if not records:
            return 0
        try:
            chunk_file = os.path.join(
                self.chunks_dir, f"chunk_{self.chunk_counter:06d}.parquet"
            )
            table = pa.Table.from_pylist(records, schema=self.SNAPSHOT_SCHEMA)
            pq.write_table(table, chunk_file, compression="snappy")
            self.chunk_counter += 1
            self.total_records += len(records)
            logger.info(f"Flushed {len(records):,} records to {chunk_file}")
            return len(records)
        except Exception as e:
            logger.error(f"Error writing chunk: {e}")
            with self.buffer_lock:
                self.buffer = records + self.buffer
            return 0

    def flush(self) -> int:
        """Thread-safe flush."""
        with self.buffer_lock:
            return self._flush_unlocked()

    def merge_chunks(self) -> str:
        """Merge all chunk files into final snapshots.parquet."""
        self.flush()
        chunk_files = sorted(
            [
                os.path.join(self.chunks_dir, f)
                for f in os.listdir(self.chunks_dir)
                if f.startswith("chunk_") and f.endswith(".parquet")
            ]
        )
        if not chunk_files:
            logger.info("No chunks to merge")
            return self.snapshot_file

        logger.info(f"Merging {len(chunk_files)} chunk files...")
        try:
            tables = []
            if os.path.exists(self.snapshot_file):
                tables.append(pq.read_table(self.snapshot_file))
            for cf in chunk_files:
                tables.append(pq.read_table(cf))
            if tables:
                combined = pa.concat_tables(tables)
                combined = combined.sort_by("timestamp")
                pq.write_table(combined, self.snapshot_file, compression="snappy")
                logger.info(
                    f"Merged {len(combined):,} total records to {self.snapshot_file}"
                )
                for cf in chunk_files:
                    os.remove(cf)
                logger.info(f"Deleted {len(chunk_files)} chunk files")
            return self.snapshot_file
        except Exception as e:
            logger.error(f"Error merging chunks: {e}")
            return self.snapshot_file

    def get_stats(self) -> Dict:
        with self.buffer_lock:
            buf_size = len(self.buffer)
        chunk_count = len(
            [
                f
                for f in os.listdir(self.chunks_dir)
                if f.startswith("chunk_") and f.endswith(".parquet")
            ]
        )
        return {
            "total_records": self.total_records,
            "records_buffered": buf_size,
            "chunk_count": chunk_count,
            "data_dir": self.data_dir,
        }

    def stop(self):
        """Stop the collector, flush and merge."""
        logger.info("DataCollector stopping...")
        self.running = False
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)
        self.flush()
        self.merge_chunks()
        logger.info(f"DataCollector stopped. Total records: {self.total_records:,}")
