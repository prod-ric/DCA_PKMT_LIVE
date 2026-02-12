"""
Parquet replay entry point for the DCA strategy.

Loads a reconstructed parquet file, iterates rows as synthetic
OrderbookTick objects, and feeds them through the same LiveStrategy
engine used for paper/live modes.  This lets you debug/validate
strategy logic against historical data without touching the notebook.

Data is also saved via DataCollector so replay chunks are stored
identically to live collection.

Usage:
  python -m live.backtest_main --parquet snapshots_27jan_reconstructed.parquet
  python -m live.backtest_main --parquet data.parquet --resample 2s --capital 500
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List

import pandas as pd

from live.config import (
    INITIAL_CAPITAL,
    CAPITAL_PER_MARKET,
    PARQUET_FILE,
    PARQUET_RESAMPLE,
    LOG_LEVEL,
    COLLECTOR_DATA_DIR,
    COLLECTOR_FLUSH_INTERVAL,
    COLLECTOR_BUFFER_SIZE,
    normalize_strategy_params,
)
from live.orderbook_feed import OrderbookTick
from live.order_manager import create_order_manager
from live.position_tracker import PositionTracker, MarketState, Trade
from live.strategy import LiveStrategy
from live.data_collector import DataCollector

logger = logging.getLogger(__name__)


def _build_tick(row, asset_id: str, market_id: str) -> OrderbookTick:
    """Build an OrderbookTick from a DataFrame row."""
    ts = row["datetime"]
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()

    # Use ob_best_bid/ask (reconstructed) if available, fall back to best_bid/ask
    best_bid = row.get("ob_best_bid")
    if best_bid is None or (isinstance(best_bid, float) and pd.isna(best_bid)):
        best_bid = row.get("best_bid", 0)
    best_bid = float(best_bid)

    best_ask = row.get("ob_best_ask")
    if best_ask is None or (isinstance(best_ask, float) and pd.isna(best_ask)):
        best_ask = row.get("best_ask", 0)
    best_ask = float(best_ask)

    bids_raw = row.get("orderbook_bids", "[]")
    asks_raw = row.get("orderbook_asks", "[]")

    try:
        bids = json.loads(bids_raw) if isinstance(bids_raw, str) else (bids_raw if bids_raw is not None else [])
    except (json.JSONDecodeError, TypeError):
        bids = []
    try:
        asks = json.loads(asks_raw) if isinstance(asks_raw, str) else (asks_raw if asks_raw is not None else [])
    except (json.JSONDecodeError, TypeError):
        asks = []

    mid_price = row.get("ob_mid_price")
    if mid_price is None or (isinstance(mid_price, float) and pd.isna(mid_price)):
        mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0.0
    mid_price = float(mid_price)

    spread = best_ask - best_bid if best_ask > 0 and best_bid > 0 else 0.0

    return OrderbookTick.synthetic(
        asset_id=asset_id,
        market_id=market_id,
        timestamp=ts,
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=mid_price,
        spread=spread,
        bids=bids,
        asks=asks,
    )


async def run_parquet_backtest(
    parquet_file: str,
    resample: str = "2s",
    initial_capital: float = 500.0,
    verbose: bool = True,
    save_data: bool = True,
    state_file: str = "backtest_state.json",
    trade_log: str = "backtest_trades.json",
    known_winners: Dict[str, str] = None,
):
    """
    Run the strategy against a parquet file.

    Args:
        parquet_file: Path to the reconstructed parquet file
        resample: Resample frequency (e.g. '1s', '2s')
        initial_capital: Starting capital
        verbose: Print progress / results
        save_data: Also save ticks via DataCollector
        state_file: Where to save final state
        trade_log: Where to save trade log
        known_winners: Dict mapping market_id -> winning asset_id.
            When provided, open positions are settled at market close
            ($1.00 for winner, $0.00 for losers) instead of last price.
            Can also be a path to a JSON file.
    """
    if not os.path.exists(parquet_file):
        logger.error(f"Parquet file not found: {parquet_file}")
        return None

    # ── Load data ──
    df = pd.read_parquet(parquet_file)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")

    required = ["datetime", "asset_id", "market_id", "ob_best_bid", "ob_best_ask"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.error(f"Missing columns: {missing}")
        return None

    df = df[df["ob_best_bid"].notna() & df["ob_best_ask"].notna()].copy()

    # Filter to selected markets (winners keys) if provided
    if known_winners:
        selected = list(known_winners.keys())
        before = df["market_id"].nunique()
        df = df[df["market_id"].isin(selected)].copy()
        after = df["market_id"].nunique()
        if verbose:
            print(f"🎯 Filtered to {after}/{before} markets (from --winners)")

    if verbose:
        print(f"📂 Loaded {len(df):,} rows, {df['market_id'].nunique()} markets")
        print(f"   Time: {df['datetime'].min()} → {df['datetime'].max()}")

    # Resample
    if resample:
        df["bucket"] = df["datetime"].dt.floor(resample)
        df = df.groupby(["market_id", "asset_id", "bucket"]).last().reset_index()
        df = df.drop(columns=["datetime"]).rename(columns={"bucket": "datetime"})
        df = df.sort_values("datetime")
        if verbose:
            print(f"⏱️  Resampled to {resample}: {len(df):,} rows")

    # ── Discover markets & assets ──
    market_assets: Dict[str, List[str]] = {}
    for mid in df["market_id"].unique():
        aids = df[df["market_id"] == mid]["asset_id"].unique().tolist()
        market_assets[mid] = aids

    # ── Market time bounds ──
    market_times = {}
    for mid in df["market_id"].unique():
        mdf = df[df["market_id"] == mid]
        market_times[mid] = {
            "start": mdf["datetime"].min().isoformat(),
            "end": mdf["datetime"].max().isoformat(),
        }

    # ── Tracker ──
    cap_per = (
        CAPITAL_PER_MARKET
        if CAPITAL_PER_MARKET > 0
        else initial_capital / len(market_assets)
    )
    tracker = PositionTracker(
        initial_capital=initial_capital,
        state_file=state_file,
    )
    # Register markets
    for mid, aids in market_assets.items():
        if mid not in tracker.markets:
            tracker.markets[mid] = MarketState(
                market_id=mid,
                name=mid[:30],
                allocation=cap_per,
                cash=cap_per,
                start_time=market_times[mid]["start"],
                end_time=market_times[mid]["end"],
            )

    # ── Order manager (always paper for backtest) ──
    order_mgr = create_order_manager("paper")

    # ── Strategy ──
    strategy = LiveStrategy(
        tracker=tracker,
        order_manager=order_mgr,
        trading_mode="paper",
        market_assets=market_assets,
    )

    # ── Data collector ──
    collector = None
    if save_data:
        bt_data_dir = os.path.join(COLLECTOR_DATA_DIR, "backtest_replay")
        collector = DataCollector(
            data_dir=bt_data_dir,
            flush_interval=COLLECTOR_FLUSH_INTERVAL,
            buffer_size=COLLECTOR_BUFFER_SIZE,
        )

    # ── Replay ──
    t0 = time.time()
    total_rows = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        aid = row["asset_id"]
        mid = row["market_id"]
        tick = _build_tick(row, aid, mid)

        if collector:
            collector.record_tick(tick)

        await strategy.on_tick(tick)

        if tracker.global_tp_hit:
            if verbose:
                print(f"🎯 Global TP hit at row {i}")
            break

        if verbose and (i + 1) % 10000 == 0:
            elapsed = time.time() - t0
            pct = (i + 1) / total_rows * 100
            pnl = tracker.get_total_pnl()
            print(
                f"  [{pct:5.1f}%] {i + 1:,}/{total_rows:,} rows | "
                f"pnl=${pnl:+.2f} | {elapsed:.1f}s"
            )

    elapsed = time.time() - t0

    # ── Cleanup ──
    tracker.save_state()
    if collector:
        collector.stop()

    # ── Market close: settle open positions ──
    if not tracker.global_tp_hit:
        for mid, ms in tracker.markets.items():
            for aid, pos in ms.positions.items():
                if pos.shares <= 0:
                    continue
                if known_winners and mid in known_winners:
                    close_price = 1.0 if aid == known_winners[mid] else 0.0
                else:
                    close_price = tracker.get_price(mid, aid)

                proceeds = pos.shares * close_price
                pnl = proceeds - pos.cost
                ms.pnl += pnl
                ms.cash += proceeds

                reason = "MARKET_CLOSE"
                if aid == ms.hedge_asset:
                    reason = "MARKET_CLOSE_HEDGE"
                elif ms.promoted and aid == ms.main_asset:
                    reason = "MARKET_CLOSE_PROMOTED"

                tracker.record_trade(
                    Trade(
                        timestamp=datetime.utcnow().isoformat(),
                        market_id=mid,
                        asset_id=aid,
                        action="SELL",
                        reason=reason,
                        pnl=pnl,
                        shares=pos.shares,
                        price=close_price,
                        proceeds=proceeds,
                        mode="paper",
                    )
                )
                if verbose and known_winners and mid in known_winners:
                    tag = "WINNER" if aid == known_winners[mid] else "LOSER"
                    print(f"  📋 {tag} close {ms.name or mid[:20]}: ${pnl:+.2f}")
                pos.shares = 0
                pos.cost = 0

    # ── Results ──
    total_pnl = sum(ms.pnl for ms in tracker.markets.values())
    final_value = sum(ms.cash for ms in tracker.markets.values())

    total_return = (final_value - initial_capital) / initial_capital

    if verbose:
        print(f"\n{'=' * 50}")
        print("💰 PARQUET REPLAY RESULTS")
        print(f"{'=' * 50}")
        print(f"Capital: ${initial_capital:.2f} → ${final_value:.2f} ({total_return * 100:+.1f}%)")
        print(f"P&L: ${total_pnl:+.2f}")
        print(f"Trades: {len(tracker.trades)}")
        print(f"Time: {elapsed:.1f}s for {total_rows:,} rows")
        print()
        print("📋 Per Market:")
        for mid, ms in tracker.markets.items():
            status = ""
            if ms.early_entry_triggered:
                status += " [EARLY]"
            if ms.hedge_count > 0:
                status += f" [HEDGE x{ms.hedge_count}]"
            if ms.promoted:
                status += " [PROMOTED]"
            if ms.panic_flip_triggered:
                status += " [PANIC]"
            print(f"  {ms.name or mid[:30]}: ${ms.pnl:+.2f}{status}")
        print(f"{'=' * 50}")

    # ── Save trade log ──
    if tracker.trades:
        import json as json_mod

        with open(trade_log, "w") as f:
            json_mod.dump(
                [
                    {
                        "timestamp": t.timestamp,
                        "market_id": t.market_id,
                        "asset_id": t.asset_id,
                        "action": t.action,
                        "reason": t.reason,
                        "shares": t.shares,
                        "price": t.price,
                        "cost": t.cost,
                        "proceeds": t.proceeds,
                        "pnl": t.pnl,
                        "mode": t.mode,
                    }
                    for t in tracker.trades
                ],
                f,
                indent=2,
            )
        if verbose:
            print(f"📄 Trade log saved to {trade_log}")

    return {
        "total_return": total_return,
        "total_pnl": total_pnl,
        "final_value": final_value,
        "trades": len(tracker.trades),
        "elapsed_seconds": elapsed,
        "tracker": tracker,
    }


def main():
    parser = argparse.ArgumentParser(
        description="DCA Strategy Parquet Replay"
    )
    parser.add_argument(
        "--parquet",
        type=str,
        default=PARQUET_FILE,
        help="Path to reconstructed parquet file",
    )
    parser.add_argument(
        "--resample",
        type=str,
        default=PARQUET_RESAMPLE,
        help="Resample frequency (e.g. 1s, 2s, 5s)",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=INITIAL_CAPITAL,
        help="Initial capital",
    )
    parser.add_argument(
        "--winners",
        type=str,
        default=None,
        help=(
            "Known winners: JSON file or inline JSON mapping "
            "market_id -> winning asset_id. "
            'Example: --winners \'{"0xabc": "token_yes_id"}\' '
            "or --winners winners.json"
        ),
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save tick data via DataCollector",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="backtest_state.json",
        help="State file for persistence",
    )
    parser.add_argument(
        "--trade-log",
        type=str,
        default="backtest_trades.json",
        help="Trade log output file",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=True,
        help="Print progress",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )
    args = parser.parse_args()

    if not args.parquet:
        parser.error(
            "Parquet file required. Use --parquet <file> or set PARQUET_FILE env var."
        )

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Parse known winners
    winners = None
    if args.winners:
        if os.path.exists(args.winners):
            with open(args.winners) as f:
                winners = json.load(f)
        else:
            try:
                winners = json.loads(args.winners)
            except json.JSONDecodeError:
                parser.error(
                    f"--winners must be a JSON file path or valid JSON string. "
                    f"Got: {args.winners}"
                )

    asyncio.run(
        run_parquet_backtest(
            parquet_file=args.parquet,
            resample=args.resample,
            initial_capital=args.capital,
            verbose=not args.quiet,
            save_data=not args.no_save,
            state_file=args.state_file,
            trade_log=args.trade_log,
            known_winners=winners,
        )
    )


if __name__ == "__main__":
    main()
