"""
Data storage for Polymarket orderbook data using Parquet files.

Reconstructs orderbooks on-the-fly from ``book`` snapshots and
``price_change`` deltas — no separate reconstruction step required.

The output schema exactly matches ``live/data_collector.py`` so files
produced by either system are interchangeable.
"""

import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, List
import pyarrow as pa
import pyarrow.parquet as pq
from threading import Lock, Thread

logger = logging.getLogger(__name__)

MAX_BOOK_LEVELS = 50


class _BookState:
    """Per-asset reconstructed orderbook state.

    Mirrors the ``_BookState`` class in ``live/orderbook_feed.py``.
    """

    __slots__ = ("bids", "asks")

    def __init__(self):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

    def reset(self, raw_bids: list, raw_asks: list):
        self.bids = {
            float(b["price"]): float(b["size"])
            for b in raw_bids
            if float(b.get("size", 0)) > 0
        }
        self.asks = {
            float(a["price"]): float(a["size"])
            for a in raw_asks
            if float(a.get("size", 0)) > 0
        }

    def apply_delta(self, side: str, price: float, size: float):
        book = self.bids if side == "BUY" else self.asks
        if size > 0:
            book[price] = size
        else:
            book.pop(price, None)

    def sorted_bids_json(self, max_levels: int = MAX_BOOK_LEVELS) -> str:
        items = sorted(self.bids.items(), key=lambda x: -x[0])[:max_levels]
        return json.dumps([{"price": p, "size": s} for p, s in items])

    def sorted_asks_json(self, max_levels: int = MAX_BOOK_LEVELS) -> str:
        items = sorted(self.asks.items(), key=lambda x: x[0])[:max_levels]
        return json.dumps([{"price": p, "size": s} for p, s in items])

    @property
    def best_bid(self) -> float:
        return max(self.bids, default=0.0) if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return min(self.asks, default=0.0) if self.asks else 0.0


class Database:
    """Parquet storage with on-the-fly orderbook reconstruction.

    Every flushed row contains the **full reconstructed orderbook** so
    downstream consumers (notebook, live backtest) can use the data
    directly without ``reconstruct_orderbooks.py``.
    """

    SNAPSHOT_SCHEMA = pa.schema([
        ('timestamp', pa.int64()),
        ('datetime', pa.timestamp('ms')),
        ('asset_id', pa.string()),
        ('market_id', pa.string()),
        ('mid_price', pa.float64()),
        ('best_bid', pa.float64()),
        ('best_ask', pa.float64()),
        ('spread', pa.float64()),
        ('orderbook_bids', pa.string()),
        ('orderbook_asks', pa.string()),
        ('event_type', pa.string()),
        ('raw_json', pa.string()),
    ])

    def __init__(self, data_dir: str = "data", flush_interval: int = 60):
        self.data_dir = data_dir
        self.chunks_dir = os.path.join(data_dir, "chunks")
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(self.chunks_dir, exist_ok=True)

        self.buffer: List[Dict] = []
        self.buffer_lock = Lock()

        self.snapshot_file = os.path.join(data_dir, "snapshots.parquet")

        self.chunk_counter = self._get_next_chunk_number()
        self.flush_interval = flush_interval
        self.last_flush_time = time.time()

        self.total_records_written = 0

        # Per-asset reconstructed books
        self._books: Dict[str, _BookState] = {}

        self.running = True
        self.flush_thread = Thread(target=self._periodic_flush_loop, daemon=True)
        self.flush_thread.start()

        logger.info(f"Database initialized: {data_dir}")
        logger.info(f"Auto-flush every {flush_interval}s | on-the-fly reconstruction enabled")

    # ---------------------------------------------------------------- helpers

    def _get_book(self, asset_id: str) -> _BookState:
        if asset_id not in self._books:
            self._books[asset_id] = _BookState()
        return self._books[asset_id]

    def _get_next_chunk_number(self) -> int:
        existing = [
            f for f in os.listdir(self.chunks_dir)
            if f.startswith('chunk_') and f.endswith('.parquet')
        ]
        if not existing:
            return 0
        numbers = []
        for f in existing:
            try:
                numbers.append(int(f.replace('chunk_', '').replace('.parquet', '')))
            except ValueError:
                pass
        return max(numbers) + 1 if numbers else 0

    def _periodic_flush_loop(self):
        while self.running:
            time.sleep(10)
            with self.buffer_lock:
                time_since_flush = time.time() - self.last_flush_time
                has_data = len(self.buffer) > 0
            if has_data and time_since_flush >= self.flush_interval:
                self.flush()

    @staticmethod
    def _parse_price(price_str) -> float:
        if not price_str:
            return 0.0
        s = str(price_str)
        if s.startswith('.'):
            return float('0' + s)
        return float(s)

    # ------------------------------------------------------------- record

    def save_book_snapshot(self, data: Dict, raw_message: str = "") -> None:
        """Process a ``book`` event — full orderbook reset + record."""
        try:
            timestamp_ms = int(data.get('timestamp', datetime.now().timestamp() * 1000))
            asset_id = data.get('asset_id', '')
            market_id = data.get('market', '')

            book = self._get_book(asset_id)
            book.reset(data.get('bids', []), data.get('asks', []))

            best_bid = book.best_bid
            best_ask = book.best_ask
            mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else None
            spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else None

            record = {
                'timestamp': timestamp_ms,
                'datetime': datetime.fromtimestamp(timestamp_ms / 1000),
                'asset_id': asset_id,
                'market_id': market_id,
                'mid_price': mid_price,
                'best_bid': best_bid if best_bid > 0 else None,
                'best_ask': best_ask if best_ask > 0 else None,
                'spread': spread,
                'orderbook_bids': book.sorted_bids_json(),
                'orderbook_asks': book.sorted_asks_json(),
                'event_type': 'book',
                'raw_json': raw_message or json.dumps(data),
            }

            with self.buffer_lock:
                self.buffer.append(record)

        except Exception as e:
            logger.error(f"Error saving book snapshot: {e}")

    def save_price_change(self, data: Dict, raw_message: str = "") -> None:
        """Process a ``price_change`` event — apply deltas + record."""
        try:
            timestamp_ms = int(data.get('timestamp', datetime.now().timestamp() * 1000))
            market_id = data.get('market', '')

            # Group changes by asset
            changes_by_asset: Dict[str, list] = {}
            for change in data.get('price_changes', []):
                aid = change.get('asset_id', '')
                if aid:
                    changes_by_asset.setdefault(aid, []).append(change)

            for asset_id, changes in changes_by_asset.items():
                book = self._get_book(asset_id)
                for ch in changes:
                    side = str(ch.get('side', '')).upper()
                    price = self._parse_price(ch.get('price', 0))
                    size = float(ch.get('size', 0))
                    if side in ('BUY', 'SELL') and price > 0:
                        book.apply_delta(side, price, size)

                best_bid = book.best_bid
                best_ask = book.best_ask
                mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else None
                spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else None

                record = {
                    'timestamp': timestamp_ms,
                    'datetime': datetime.fromtimestamp(timestamp_ms / 1000),
                    'asset_id': asset_id,
                    'market_id': market_id,
                    'mid_price': mid_price,
                    'best_bid': best_bid if best_bid > 0 else None,
                    'best_ask': best_ask if best_ask > 0 else None,
                    'spread': spread,
                    'orderbook_bids': book.sorted_bids_json(),
                    'orderbook_asks': book.sorted_asks_json(),
                    'event_type': 'price_change',
                    'raw_json': raw_message or json.dumps(data),
                }

                with self.buffer_lock:
                    self.buffer.append(record)

        except Exception as e:
            logger.error(f"Error saving price change: {e}")

    # ------------------------------------------------------------- flush

    def flush(self) -> int:
        with self.buffer_lock:
            if not self.buffer:
                return 0
            records = self.buffer.copy()
            self.buffer.clear()
            self.last_flush_time = time.time()

        try:
            chunk_file = os.path.join(
                self.chunks_dir, f"chunk_{self.chunk_counter:06d}.parquet"
            )
            table = pa.Table.from_pylist(records, schema=self.SNAPSHOT_SCHEMA)
            pq.write_table(table, chunk_file, compression='snappy')

            self.chunk_counter += 1
            self.total_records_written += len(records)

            logger.info(f"Flushed {len(records):,} records to {chunk_file}")
            return len(records)

        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")
            with self.buffer_lock:
                self.buffer = records + self.buffer
            return 0

    def merge_chunks(self) -> str:
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
            tables = []
            if os.path.exists(self.snapshot_file):
                tables.append(pq.read_table(self.snapshot_file))
            for chunk_file in chunk_files:
                tables.append(pq.read_table(chunk_file))

            if tables:
                combined = pa.concat_tables(tables, promote_options="default")
                combined = combined.sort_by('timestamp')
                pq.write_table(combined, self.snapshot_file, compression='snappy')
                logger.info(f"Merged {len(combined):,} total records to {self.snapshot_file}")

                for chunk_file in chunk_files:
                    os.remove(chunk_file)
                logger.info(f"Deleted {len(chunk_files)} chunk files")

            return self.snapshot_file

        except Exception as e:
            logger.error(f"Error merging chunks: {e}")
            return self.snapshot_file

    # ------------------------------------------------------------- stats

    def get_snapshot_count(self) -> int:
        count = 0
        if os.path.exists(self.snapshot_file):
            try:
                count += pq.read_metadata(self.snapshot_file).num_rows
            except Exception:
                pass
        chunk_files = [
            os.path.join(self.chunks_dir, f)
            for f in os.listdir(self.chunks_dir)
            if f.startswith('chunk_') and f.endswith('.parquet')
        ]
        for cf in chunk_files:
            try:
                count += pq.read_metadata(cf).num_rows
            except Exception:
                pass
        with self.buffer_lock:
            count += len(self.buffer)
        return count

    def get_buffer_size(self) -> int:
        with self.buffer_lock:
            return len(self.buffer)

    def get_chunk_count(self) -> int:
        return len([
            f for f in os.listdir(self.chunks_dir)
            if f.startswith('chunk_') and f.endswith('.parquet')
        ])

    def shutdown(self):
        logger.info("Database shutting down...")
        self.running = False
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)
        self.flush()
        self.merge_chunks()
        logger.info("Database shutdown complete")
