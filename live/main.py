"""
Main entry point for the DCA Polymarket Live Trader.

DESIGNED FOR SPEED: This process handles trading decisions only.
For data collection, run `python -m live.collector` in a separate terminal.

Usage:
    # Paper mode (default) - trading only, fastest:
    python -m live.main

    # Real mode:
    TRADING_MODE=real python -m live.main

    # With data collection in same process (slower, for convenience):
    ENABLE_COLLECTION=true python -m live.main

    # Recommended production setup (2 terminals):
    # Terminal 1: python -m live.main         # Trading (fast)
    # Terminal 2: python -m live.collector    # Data collection (separate)

Configure via environment variables or .env file.
See live/config.py for all options.
"""

import asyncio
import logging
import signal
import sys
import os
from datetime import datetime
from typing import Optional

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from live.config import (
    TRADING_MODE,
    INITIAL_CAPITAL,
    CAPITAL_PER_MARKET,
    POLYMARKET_PRIVATE_KEY,
    POLYMARKET_CLOB_URL,
    POLYMARKET_WSS_URL,
    POLYMARKET_API_KEY,
    POLYMARKET_API_SECRET,
    POLYMARKET_API_PASSPHRASE,
    CHAIN_ID,
    STATE_FILE,
    TRADE_LOG_FILE,
    LOG_LEVEL,
    DEFAULT_STRATEGY_PARAMS,
    load_markets,
    validate_config,
)
from live.orderbook_feed import OrderbookFeed
from live.order_manager import create_order_manager
from live.position_tracker import PositionTracker
from live.strategy import LiveStrategy

# Optional data collection (disabled by default for speed)
ENABLE_COLLECTION = os.getenv("ENABLE_COLLECTION", "false").lower() == "true"
COLLECTOR_DATA_DIR = os.getenv("COLLECTOR_DATA_DIR", "live/data")

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("live/live_trader.log"),
    ],
)
logger = logging.getLogger("live.main")

# Globals
shutdown_event: asyncio.Event = None
strategy: LiveStrategy = None
feed: OrderbookFeed = None
tracker: PositionTracker = None
collector = None  # Optional data collector


async def status_printer(interval: int = 60):
    """Periodically print portfolio status."""
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass

        if tracker and not shutdown_event.is_set():
            tracker.print_status()
            feed_stats = feed.get_stats() if feed else {}
            logger.info(
                f"Feed: {feed_stats.get('book_messages', 0)} books, "
                f"{feed_stats.get('price_change_messages', 0)} price changes, "
                f"last msg: {feed_stats.get('last_message_time', 'never')}"
            )
            # Save state
            tracker.save_state()


async def main():
    global shutdown_event, strategy, feed, tracker, collector

    shutdown_event = asyncio.Event()

    # Signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: shutdown_event.set())

    try:
        # ============================================================
        # VALIDATE & CONFIGURE
        # ============================================================
        validate_config()

        # Load markets
        markets_config = load_markets()
        markets = markets_config.get("markets", [])
        if not markets:
            logger.error(
                "No markets configured. Create live/markets.json with market definitions.\n"
                "See README.md for format."
            )
            return

        logger.info(f"Loaded {len(markets)} market(s)")

        # ============================================================
        # INITIALIZE COMPONENTS
        # ============================================================

        # Position tracker
        tracker = PositionTracker(
            state_file=STATE_FILE,
            trade_log_file=TRADE_LOG_FILE,
            initial_capital=INITIAL_CAPITAL,
        )

        # Try to resume from saved state
        if tracker.load_state():
            logger.info("Resumed from saved state")
        else:
            logger.info("Starting fresh")

        # Calculate per-market allocation
        cap_per_market = CAPITAL_PER_MARKET if CAPITAL_PER_MARKET > 0 else INITIAL_CAPITAL / len(markets)

        # Initialize market states
        all_asset_ids = []
        market_assets_map = {}

        for m in markets:
            cond_id = m["condition_id"]
            asset_ids = m.get("asset_ids", [])
            name = m.get("name", cond_id[:20])
            end_time = m.get("end_time")
            start_time = m.get("start_time")
            alloc = m.get("capital", cap_per_market)

            tracker.init_market(
                market_id=cond_id,
                name=name,
                allocation=alloc,
                start_time=start_time,
                end_time=end_time,
            )
            all_asset_ids.extend(asset_ids)
            market_assets_map[cond_id] = asset_ids

        # Order manager
        if TRADING_MODE == "real":
            order_mgr = create_order_manager(
                mode="real",
                private_key=POLYMARKET_PRIVATE_KEY,
                clob_url=POLYMARKET_CLOB_URL,
                chain_id=CHAIN_ID,
                api_key=POLYMARKET_API_KEY,
                api_secret=POLYMARKET_API_SECRET,
                api_passphrase=POLYMARKET_API_PASSPHRASE,
            )
        else:
            order_mgr = create_order_manager(mode="paper")

        # Strategy
        strategy = LiveStrategy(
            tracker=tracker,
            order_manager=order_mgr,
            params=DEFAULT_STRATEGY_PARAMS,
            trading_mode=TRADING_MODE,
            market_assets=market_assets_map,
        )

        # Optional data collector (disabled by default for speed)
        if ENABLE_COLLECTION:
            from live.data_collector import DataCollector
            collector = DataCollector(
                data_dir=COLLECTOR_DATA_DIR,
                flush_interval=60,
                buffer_size=1000,
            )
            logger.info(f"Data collection ENABLED -> {COLLECTOR_DATA_DIR}")
            logger.info("  (For faster trading, disable collection and run collector separately)")

            # Tick handler that feeds both strategy and collector
            async def on_tick_with_collection(tick):
                collector.record_tick(tick)
                await strategy.on_tick(tick)

            tick_handler = on_tick_with_collection
        else:
            tick_handler = strategy.on_tick
            logger.info("Data collection DISABLED (run `python -m live.collector` separately)")

        # Orderbook feed
        feed = OrderbookFeed(
            asset_ids=all_asset_ids,
            wss_url=POLYMARKET_WSS_URL,
            on_tick=tick_handler,
        )

        # ============================================================
        # RUN
        # ============================================================
        logger.info(f"Starting live trader with {len(markets)} market(s), "
                    f"{len(all_asset_ids)} asset(s)")
        logger.info(f"Mode: {TRADING_MODE.upper()} | Capital: ${INITIAL_CAPITAL:.2f}")
        logger.info("Press Ctrl+C to stop\n")

        # Run feed + status printer concurrently
        feed_task = asyncio.create_task(feed.run())
        status_task = asyncio.create_task(status_printer(interval=60))

        # Wait for shutdown
        await shutdown_event.wait()

        logger.info("\n🛑 Shutting down...")
        feed.stop()
        feed_task.cancel()
        status_task.cancel()

        try:
            await asyncio.gather(feed_task, status_task, return_exceptions=True)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        # Final save
        if tracker:
            tracker.save_state()
            tracker.print_status()
            logger.info(f"State saved to {STATE_FILE}")
            logger.info(f"Trades logged to {TRADE_LOG_FILE}")

        # Stop collector if running
        if collector:
            collector.stop()
            logger.info(f"Data saved to {collector.snapshot_file}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
