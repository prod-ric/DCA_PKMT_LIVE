"""
Main entry point for the DCA Polymarket Live Trader.

Modes:
  paper   — live WebSocket data, simulated fills (default)
  live    — real orders via py-clob-client
  parquet — replay historical parquet (see backtest_main.py)

Usage:
  python -m live.main                       # paper mode
  TRADING_MODE=live python -m live.main     # live mode
  python -m live.backtest_main --parquet <file>  # parquet replay
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, List

from live.config import (
    TRADING_MODE,
    INITIAL_CAPITAL,
    CAPITAL_PER_MARKET,
    LOG_LEVEL,
    STATE_FILE,
    COLLECTOR_DATA_DIR,
    COLLECTOR_FLUSH_INTERVAL,
    COLLECTOR_BUFFER_SIZE,
    load_markets,
    validate_config,
)
from live.orderbook_feed import OrderbookFeed, OrderbookTick
from live.order_manager import create_order_manager
from live.position_tracker import PositionTracker, MarketState
from live.strategy import LiveStrategy
from live.data_collector import DataCollector

logger = logging.getLogger(__name__)


async def run(
    trading_mode: str = None,
    markets_override: Dict = None,
):
    """
    Run the live trader:

    1. Load market configuration
    2. Initialise tracker, order manager, strategy, data collector
    3. Connect to Polymarket WebSocket
    4. Feed ticks to strategy engine
    5. Persist state on shutdown
    """
    mode = trading_mode or TRADING_MODE
    validate_config()

    # ── Load markets ──
    markets_cfg = markets_override or load_markets()
    market_list = markets_cfg.get("markets", [])
    if not market_list:
        logger.error("No markets configured. Add markets to live/markets.json")
        return

    # ── Capital allocation ──
    cap_per = CAPITAL_PER_MARKET if CAPITAL_PER_MARKET > 0 else INITIAL_CAPITAL / len(market_list)

    # ── Build tracker ──
    tracker = PositionTracker(
        initial_capital=INITIAL_CAPITAL,
        state_file=STATE_FILE,
    )

    # Register markets
    market_assets: Dict[str, List[str]] = {}
    all_asset_ids: List[str] = []
    for mkt in market_list:
        condition_id = mkt.get("condition_id", "")
        asset_ids = mkt.get("asset_ids", [])
        name = mkt.get("name", condition_id[:20])
        start_time = mkt.get("start_time")
        end_time = mkt.get("end_time")
        alloc = mkt.get("capital", cap_per)

        # Use condition_id as market_id
        mid = condition_id
        if mid not in tracker.markets:
            ms = MarketState(
                market_id=mid,
                name=name,
                allocation=alloc,
                cash=alloc,
                start_time=start_time,
                end_time=end_time,
            )
            tracker.markets[mid] = ms
        else:
            ms = tracker.markets[mid]
            ms.name = name
            if start_time:
                ms.start_time = start_time
            if end_time:
                ms.end_time = end_time

        market_assets[mid] = asset_ids
        all_asset_ids.extend(asset_ids)

    logger.info(f"Tracking {len(market_list)} markets, {len(all_asset_ids)} assets")

    # ── Order manager ──
    order_mgr = create_order_manager(mode)

    # ── Strategy ──
    strategy = LiveStrategy(
        tracker=tracker,
        order_manager=order_mgr,
        trading_mode=mode,
        market_assets=market_assets,
    )

    # ── Data collector ──
    collector = DataCollector(
        data_dir=COLLECTOR_DATA_DIR,
        flush_interval=COLLECTOR_FLUSH_INTERVAL,
        buffer_size=COLLECTOR_BUFFER_SIZE,
    )

    # ── Orderbook feed ──
    feed = OrderbookFeed(asset_ids=all_asset_ids)

    # ── Shutdown handling ──
    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # ── Periodic dashboard + data stats ──
    async def dashboard_loop():
        while not shutdown_event.is_set():
            await asyncio.sleep(60)
            if shutdown_event.is_set():
                break
            strategy.print_dashboard(trigger="PERIODIC")
            stats = collector.get_stats()
            logger.info(
                f"[DATA] records={stats['total_records']:,} | "
                f"buffered={stats['records_buffered']} | "
                f"chunks={stats['chunk_count']}"
            )

    status_task = asyncio.create_task(dashboard_loop())

    # ── Main loop ──
    try:
        async for tick in feed.stream():
            if shutdown_event.is_set():
                break
            # Record data
            collector.record_tick(tick)
            # Feed to strategy
            await strategy.on_tick(tick)

            if tracker.global_tp_hit:
                logger.info("Global TP hit — shutting down")
                break
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        status_task.cancel()
        await feed.close()
        tracker.save_state()
        collector.stop()
        logger.info("Done.")


def main():
    """CLI entry point."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
