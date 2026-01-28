# ============================================
# FILE: main.py (FIXED SIGNAL HANDLING)
# ============================================

"""
Main entry point for Polymarket data collector
"""
import asyncio
import logging
import sys
import signal
import os
from datetime import datetime
from websocket_client import PolymarketWebSocketClient
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('polymarket_collector.log')
    ]
)

logger = logging.getLogger(__name__)

# Global references
client = None
start_time = None
shutdown_event = None


def format_duration(seconds: float) -> str:
    """Format seconds into human readable duration"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        mins = seconds / 60
        return f"{mins:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_bytes(bytes_size: int) -> str:
    """Format bytes into human readable size"""
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 * 1024:
        return f"{bytes_size / 1024:.1f} KB"
    elif bytes_size < 1024 * 1024 * 1024:
        return f"{bytes_size / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_size / (1024 * 1024 * 1024):.2f} GB"


def get_total_file_size(data_dir: str) -> int:
    """Get total size of all parquet files"""
    total = 0
    
    main_file = os.path.join(data_dir, "snapshots.parquet")
    if os.path.exists(main_file):
        total += os.path.getsize(main_file)
    
    chunks_dir = os.path.join(data_dir, "chunks")
    if os.path.exists(chunks_dir):
        for f in os.listdir(chunks_dir):
            if f.endswith('.parquet'):
                total += os.path.getsize(os.path.join(chunks_dir, f))
    
    return total


async def healthcheck(interval: int = 600):
    """Print healthcheck every N seconds"""
    global client, start_time, shutdown_event
    
    last_book_count = 0
    last_price_change_count = 0
    last_check_time = datetime.now()
    
    while not shutdown_event.is_set():
        try:
            # Wait with timeout so we can check shutdown_event
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break  # shutdown_event was set
        except asyncio.TimeoutError:
            pass  # Timeout = time for healthcheck
        
        if shutdown_event.is_set() or client is None:
            break
        
        now = datetime.now()
        stats = client.get_stats()
        
        # Calculate rates
        time_diff = (now - last_check_time).total_seconds()
        book_rate = (stats['book_messages'] - last_book_count) / time_diff * 60 if time_diff > 0 else 0
        price_change_rate = (stats['price_change_messages'] - last_price_change_count) / time_diff * 60 if time_diff > 0 else 0
        
        file_size = get_total_file_size(Config.DATA_DIR)
        uptime = (now - start_time).total_seconds() if start_time else 0
        
        is_healthy = (stats['book_messages'] > last_book_count) or (stats['price_change_messages'] > last_price_change_count)
        
        last_msg_ago = None
        if stats['last_message_time']:
            last_msg_ago = (now - stats['last_message_time']).total_seconds()
        
        buffer_size = client.db.get_buffer_size()
        chunk_count = client.db.get_chunk_count()
        
        status = "✅ HEALTHY" if is_healthy else "⚠️ NO NEW DATA"
        
        healthcheck_msg = f"""
================================================================================
📊 HEALTHCHECK - {now.strftime('%Y-%m-%d %H:%M:%S')}
================================================================================
Status:              {status}
Uptime:              {format_duration(uptime)}
Last Message:        {format_duration(last_msg_ago) + ' ago' if last_msg_ago else 'Never'}

MESSAGES:
  Book Snapshots:    {stats['book_messages']:,} total ({book_rate:.1f}/min)
  Price Changes:     {stats['price_change_messages']:,} total ({price_change_rate:.1f}/min)
  Total Messages:    {stats['messages_received']:,}

STORAGE:
  Snapshots Stored:  {stats['snapshot_count']:,}
  Buffer Pending:    {buffer_size:,}
  Chunk Files:       {chunk_count}
  Total Size:        {format_bytes(file_size)}
  Data Directory:    {Config.DATA_DIR}

SUBSCRIPTIONS:
  Assets:            {len(client.asset_ids)}
  Markets:           {len(client.markets)}
================================================================================
"""
        logger.info(healthcheck_msg)
        
        last_book_count = stats['book_messages']
        last_price_change_count = stats['price_change_messages']
        last_check_time = now
        
        if last_msg_ago and last_msg_ago > 300:
            logger.warning(f"⚠️ No messages received for {format_duration(last_msg_ago)}")


async def run_collector():
    """Run the WebSocket collector"""
    global client, shutdown_event
    
    while not shutdown_event.is_set():
        try:
            await client.run_once()
        except Exception as e:
            logger.error(f"Collector error: {e}")
        
        if shutdown_event.is_set():
            break
        
        logger.info(f"Reconnecting in {Config.RECONNECT_DELAY}s...")
        
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=Config.RECONNECT_DELAY)
            break
        except asyncio.TimeoutError:
            pass


def do_shutdown():
    """Perform shutdown tasks (runs in sync context)"""
    global client, start_time
    
    if client is None:
        return
    
    logger.info("=" * 60)
    logger.info("🛑 SHUTTING DOWN...")
    logger.info("=" * 60)
    
    # Stop WebSocket
    logger.info("Stopping WebSocket connection...")
    client.stop()
    
    # Shutdown database (flush + merge)
    logger.info("Flushing buffer to disk...")
    client.db.flush()
    
    logger.info("Merging chunk files...")
    client.db.merge_chunks()
    
    # Final stats
    stats = client.get_stats()
    uptime = (datetime.now() - start_time).total_seconds() if start_time else 0
    file_size = get_total_file_size(Config.DATA_DIR)
    
    shutdown_msg = f"""
================================================================================
✅ SHUTDOWN COMPLETE
================================================================================
Runtime:             {format_duration(uptime)}
Total Snapshots:     {stats['snapshot_count']:,}
Book Messages:       {stats['book_messages']:,}
Price Changes:       {stats['price_change_messages']:,}
Final File Size:     {format_bytes(file_size)}
Output File:         {client.db.snapshot_file}
================================================================================
"""
    logger.info(shutdown_msg)


async def main():
    """Main function"""
    global client, start_time, shutdown_event
    
    # Create shutdown event
    shutdown_event = asyncio.Event()
    
    # Setup signal handlers for async
    loop = asyncio.get_running_loop()
    
    def handle_signal():
        logger.info("Received shutdown signal (Ctrl+C)")
        shutdown_event.set()
    
    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)
    
    try:
        Config.validate()
        
        start_time = datetime.now()
        
        startup_msg = f"""
================================================================================
🚀 POLYMARKET DATA COLLECTOR (Parquet)
================================================================================
Started:             {start_time.strftime('%Y-%m-%d %H:%M:%S')}
Data Directory:      {Config.DATA_DIR}
Asset IDs:           {len(Config.ASSET_IDS)}
Markets:             {len(Config.MARKETS)}
Auto-flush:          Every 60 seconds
Healthcheck:         Every 10 minutes
Press Ctrl+C to stop and save data
================================================================================
"""
        logger.info(startup_msg)
        
        if Config.ASSET_IDS:
            logger.info(f"Subscribing to assets: {Config.ASSET_IDS[:5]}{'...' if len(Config.ASSET_IDS) > 5 else ''}")
        if Config.MARKETS:
            logger.info(f"Subscribing to markets: {Config.MARKETS[:5]}{'...' if len(Config.MARKETS) > 5 else ''}")
        
        # Create client
        client = PolymarketWebSocketClient(
            asset_ids=Config.ASSET_IDS,
            markets=Config.MARKETS
        )
        
        # Run collector and healthcheck
        await asyncio.gather(
            run_collector(),
            healthcheck(interval=600),
            return_exceptions=True
        )
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        # Always run shutdown
        do_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # Already handled by signal handler