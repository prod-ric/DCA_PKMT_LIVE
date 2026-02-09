"""
Standalone data collector for Polymarket orderbook data.

This runs as a SEPARATE PROCESS from the trading strategy.
It connects to the WebSocket, collects orderbook data, and saves to parquet.

Usage:
    # Collect data only (no trading):
    python -m live.collector

    # Run alongside trading in separate terminals:
    # Terminal 1: python -m live.main         (trading only)
    # Terminal 2: python -m live.collector    (data collection only)

Configure via environment variables or .env file.
"""

import asyncio
import logging
import signal
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from live.config import (
    POLYMARKET_WSS_URL,
    LOG_LEVEL,
    load_markets,
)
from live.orderbook_feed import OrderbookFeed, OrderbookTick
from live.data_collector import DataCollector

# ============================================================
# CONFIG
# ============================================================
DATA_DIR = os.getenv("COLLECTOR_DATA_DIR", "live/data")
FLUSH_INTERVAL = int(os.getenv("COLLECTOR_FLUSH_INTERVAL", "60"))
BUFFER_SIZE = int(os.getenv("COLLECTOR_BUFFER_SIZE", "1000"))
HEALTHCHECK_INTERVAL = int(os.getenv("COLLECTOR_HEALTHCHECK_INTERVAL", "300"))

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("live/collector.log"),
    ],
)
logger = logging.getLogger("live.collector")

# Globals
shutdown_event: asyncio.Event = None
collector: DataCollector = None
feed: OrderbookFeed = None
start_time: datetime = None


def format_bytes(n: int) -> str:
    """Format bytes to human readable."""
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    elif n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    else:
        return f"{n / (1024 * 1024 * 1024):.2f} GB"


def get_data_size() -> int:
    """Get total size of data files."""
    total = 0
    if collector and os.path.exists(collector.data_dir):
        for root, dirs, files in os.walk(collector.data_dir):
            for f in files:
                if f.endswith('.parquet'):
                    total += os.path.getsize(os.path.join(root, f))
    return total


async def healthcheck(interval: int):
    """Periodic health check and stats."""
    global collector, feed, start_time
    
    last_records = 0
    last_check = datetime.utcnow()
    
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
        
        if shutdown_event.is_set():
            break
        
        now = datetime.utcnow()
        uptime = (now - start_time).total_seconds() if start_time else 0
        
        coll_stats = collector.get_stats() if collector else {}
        feed_stats = feed.get_stats() if feed else {}
        
        current_records = coll_stats.get('total_records', 0)
        time_diff = (now - last_check).total_seconds()
        rate = (current_records - last_records) / time_diff * 60 if time_diff > 0 else 0
        
        data_size = get_data_size()
        
        msg = f"""
================================================================================
📊 COLLECTOR HEALTHCHECK - {now.strftime('%Y-%m-%d %H:%M:%S')}
================================================================================
Uptime:              {uptime / 60:.1f} minutes
Records Collected:   {current_records:,} total ({rate:.0f}/min)
Buffer Pending:      {coll_stats.get('records_buffered', 0):,}
Chunk Files:         {coll_stats.get('chunk_count', 0)}
Data Size:           {format_bytes(data_size)}
Data Directory:      {coll_stats.get('data_dir', DATA_DIR)}

Feed Stats:
  Book Messages:     {feed_stats.get('book_messages', 0):,}
  Price Changes:     {feed_stats.get('price_change_messages', 0):,}
  Last Message:      {feed_stats.get('last_message_time', 'never')}
  Known Assets:      {feed_stats.get('known_assets', 0)}
================================================================================
"""
        logger.info(msg)
        
        last_records = current_records
        last_check = now


async def on_tick(tick: OrderbookTick):
    """Handle orderbook tick - record to collector."""
    if collector:
        collector.record_tick(tick)


async def main():
    global shutdown_event, collector, feed, start_time
    
    shutdown_event = asyncio.Event()
    start_time = datetime.utcnow()
    
    # Signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: shutdown_event.set())
    
    try:
        # Load markets
        markets_config = load_markets()
        markets = markets_config.get("markets", [])
        if not markets:
            logger.error(
                "No markets configured. Create live/markets.json with market definitions."
            )
            return
        
        # Collect all asset IDs
        all_asset_ids = []
        for m in markets:
            all_asset_ids.extend(m.get("asset_ids", []))
        
        logger.info(f"{'='*60}")
        logger.info(f"  POLYMARKET DATA COLLECTOR")
        logger.info(f"{'='*60}")
        logger.info(f"  Markets:        {len(markets)}")
        logger.info(f"  Assets:         {len(all_asset_ids)}")
        logger.info(f"  Data Directory: {DATA_DIR}")
        logger.info(f"  Flush Interval: {FLUSH_INTERVAL}s")
        logger.info(f"  Buffer Size:    {BUFFER_SIZE}")
        logger.info(f"{'='*60}")
        logger.info("Press Ctrl+C to stop and merge data\n")
        
        # Initialize collector
        collector = DataCollector(
            data_dir=DATA_DIR,
            flush_interval=FLUSH_INTERVAL,
            buffer_size=BUFFER_SIZE,
        )
        
        # Initialize feed
        feed = OrderbookFeed(
            asset_ids=all_asset_ids,
            wss_url=POLYMARKET_WSS_URL,
            on_tick=on_tick,
        )
        
        # Run feed + healthcheck
        feed_task = asyncio.create_task(feed.run())
        health_task = asyncio.create_task(healthcheck(HEALTHCHECK_INTERVAL))
        
        await shutdown_event.wait()
        
        logger.info("\n🛑 Shutting down collector...")
        feed.stop()
        feed_task.cancel()
        health_task.cancel()
        
        try:
            await asyncio.gather(feed_task, health_task, return_exceptions=True)
        except Exception:
            pass
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        # Final cleanup
        if collector:
            collector.stop()
            
            # Print final stats
            stats = collector.get_stats()
            logger.info(f"\n{'='*60}")
            logger.info(f"  COLLECTOR SHUTDOWN COMPLETE")
            logger.info(f"{'='*60}")
            logger.info(f"  Total Records:   {stats.get('total_records', 0):,}")
            logger.info(f"  Data Size:       {format_bytes(get_data_size())}")
            logger.info(f"  Output File:     {collector.snapshot_file}")
            logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
