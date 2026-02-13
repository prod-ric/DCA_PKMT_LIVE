"""
Live DCA Strategy Engine v19.

Mirrors the backtest logic from the notebook but operates tick-by-tick
in real time.  Receives OrderbookTick objects and makes trading decisions.

Key features ported from backtest:
- Multi-tier DCA entry
- Stop-loss / take-profit with confirmation
- Hedge on fast/slow drop (with confirmation)
- Hedge exit on drop (with confirmation)
- Hedge promotion
- Panic flip w/ surge detection
- Early entry for stable markets
- Late-game stop-loss disable
- Grace period after entry
- Instant exit at 0.99
- Global take-profit across all markets
"""

import logging
import time
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from live.config import (
    get_all_params,
)
from live.position_tracker import PositionTracker, MarketState, Trade
from live.orderbook_feed import OrderbookTick

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────
# HELPERS (ported from notebook)
# ────────────────────────────────────────────────────────────

def _calculate_stability_metrics(price_history: List) -> Dict:
    """Calculate stability metrics from price history list of (ts, price)."""
    if len(price_history) < 10:
        return {
            "volatility": float("inf"),
            "avg_price": 0,
            "range": float("inf"),
            "valid": False,
        }
    prices = [p for _, p in price_history]
    return {
        "volatility": float(np.std(prices)),
        "avg_price": float(np.mean(prices)),
        "min_price": float(min(prices)),
        "max_price": float(max(prices)),
        "range": float(max(prices) - min(prices)),
        "valid": True,
    }


def _check_early_entry_eligible(
    price_history: List,
    current_price: float,
    market_progress: float,
    ts: datetime,
    early_entry_min_progress: float,
    early_entry_max_progress: float,
    early_entry_price_threshold: float,
    early_entry_max_volatility: float,
    early_entry_max_range: float,
    early_entry_min_duration_minutes: float,
    early_entry_no_drop_threshold: float,
    early_entry_no_drop_window: int,
) -> Tuple[bool, str]:
    """Check whether early entry conditions are met for a stable market."""
    if (
        market_progress < early_entry_min_progress
        or market_progress >= early_entry_max_progress
    ):
        return False, "outside_early_window"
    if current_price < early_entry_price_threshold:
        return False, f"price_{current_price:.2f}_below_threshold"
    if len(price_history) < 20:
        return False, "insufficient_history"

    progress_in_window = (market_progress - early_entry_min_progress) / (
        early_entry_max_progress - early_entry_min_progress
    )
    relaxation = progress_in_window

    adj_max_vol = early_entry_max_volatility * (1 + relaxation * 1.5)
    adj_max_range = early_entry_max_range * (1 + relaxation * 1.5)
    adj_min_dur = early_entry_min_duration_minutes * (1 - relaxation * 0.6)

    metrics = _calculate_stability_metrics(price_history)
    if not metrics["valid"]:
        return False, "invalid_metrics"
    if metrics["volatility"] > adj_max_vol:
        return False, f"volatility_{metrics['volatility']:.3f}"
    if metrics["range"] > adj_max_range:
        return False, f"range_{metrics['range']:.3f}"

    # Duration check
    if price_history:
        first_ts = price_history[0][0]
        if isinstance(first_ts, str):
            first_ts = datetime.fromisoformat(first_ts)
        duration_min = (ts - first_ts).total_seconds() / 60
        if duration_min < adj_min_dur:
            return False, f"duration_{duration_min:.1f}min"

    # No recent drop
    cutoff = (ts - timedelta(seconds=early_entry_no_drop_window)).isoformat()
    recent = [(t, p) for t, p in price_history if t >= cutoff]
    if len(recent) >= 2:
        recent_max = max(p for _, p in recent)
        adj_drop = early_entry_no_drop_threshold * (1 + relaxation * 0.5)
        if recent_max - current_price > adj_drop:
            return False, f"recent_drop_{recent_max - current_price:.3f}"

    return True, f"stable_early_entry_{market_progress:.1%}"


# ────────────────────────────────────────────────────────────
# LIVE STRATEGY
# ────────────────────────────────────────────────────────────

class LiveStrategy:
    """
    Real-time DCA strategy engine.

    Receives ticks from OrderbookFeed and decides trades.
    Uses PositionTracker for state and order_manager for execution.
    """

    def __init__(
        self,
        tracker: PositionTracker,
        order_manager,
        params: Dict = None,
        trading_mode: str = "paper",
        market_assets: Dict[str, List[str]] = None,
    ):
        """
        Initialize the live strategy.
        
        Args:
            tracker: Position tracker instance
            order_manager: Order execution manager
            params: Full parameter dict (use get_all_params() to build with overrides)
            trading_mode: "paper" | "live" | "parquet"
            market_assets: Dict mapping market_id -> [asset_ids]
        """
        self.tracker = tracker
        self.order_mgr = order_manager
        self.mode = trading_mode
        self.market_assets = market_assets or {}

        # Get full params (merges with defaults)
        self.params = get_all_params(params)

        # Extract strategy params
        self.ENTRY = self.params["entry_threshold"]
        self.EXIT_SL = self.params["exit_stop_loss"]
        self.SL_PCT = self.params["stop_loss_pct"]
        self.TP_PCT = self.params["take_profit_pct"]
        self.COOLDOWN = self.params["cooldown_periods"]
        
        # Capital & risk
        self.GLOBAL_TP = tracker.initial_capital * self.params["global_tp_pct"]
        
        # Trading window
        self.LATE_GAME_THRESHOLD = self.params["late_game_threshold"]
        self.LAST_MINUTES_ONLY = self.params["last_minutes_only"]
        
        # Early entry
        self.EARLY_ENTRY_ENABLED = self.params["early_entry_enabled"]
        self.EARLY_ENTRY_MIN_PROGRESS = self.params["early_entry_min_progress"]
        self.EARLY_ENTRY_MAX_PROGRESS = self.params["early_entry_max_progress"]
        self.EARLY_ENTRY_PRICE_THRESHOLD = self.params["early_entry_price_threshold"]
        self.EARLY_ENTRY_MAX_VOLATILITY = self.params["early_entry_max_volatility"]
        self.EARLY_ENTRY_MAX_RANGE = self.params["early_entry_max_range"]
        self.EARLY_ENTRY_MIN_DURATION_MINUTES = self.params["early_entry_min_duration_minutes"]
        self.EARLY_ENTRY_NO_DROP_THRESHOLD = self.params["early_entry_no_drop_threshold"]
        self.EARLY_ENTRY_NO_DROP_WINDOW = self.params["early_entry_no_drop_window"]
        
        # Hedge
        self.HEDGE_ENABLED = self.params["hedge_enabled"]
        self.HEDGE_DROP_POINTS = self.params["hedge_drop_points"]
        self.HEDGE_DROP_WINDOW = self.params["hedge_drop_window"]
        self.HEDGE_DROP_FROM_ENTRY = self.params["hedge_drop_from_entry"]
        self.HEDGE_AMOUNT = self.params["hedge_amount"]
        self.HEDGE_LATE_GAME_ONLY = self.params["hedge_late_game_only"]
        self.HEDGE_COOLDOWN_SECONDS = self.params["hedge_cooldown_seconds"]
        self.HEDGE_DROP_CONFIRMATION_TICKS = self.params["hedge_drop_confirmation_ticks"]
        self.HEDGE_EXIT_CONFIRMATION_TICKS = self.params["hedge_exit_confirmation_ticks"]
        self.HEDGE_EXIT_DROP_POINTS = self.params["hedge_exit_drop_points"]
        
        # Panic flip
        self.PANIC_FLIP_ENABLED = self.params["panic_flip_enabled"]
        self.PANIC_FLIP_THRESHOLD = self.params["panic_flip_threshold"]
        self.PANIC_FLIP_WINDOW = self.params["panic_flip_window"]
        self.PANIC_FLIP_MIN_PRICE = self.params["panic_flip_min_price"]
        self.PANIC_FLIP_LATE_GAME_ONLY = self.params["panic_flip_late_game_only"]
        
        # Hedge promotion
        self.HEDGE_PROMOTION_PNL_PCT = self.params["hedge_promotion_pnl_pct"]
        self.HEDGE_PROMOTION_PRICE = self.params["hedge_promotion_price"]
        self.HEDGE_PROMOTION_DELTA = self.params["hedge_promotion_delta"]
        
        # Safety
        self.ENTRY_GRACE_PERIOD = self.params["entry_grace_period"]
        self.INSTANT_EXIT_PRICE = self.params["instant_exit_price"]
        self.MAX_SPREAD_FOR_SL = self.params["max_spread_for_sl"]
        self.SL_CONFIRMATION_TICKS = self.params["sl_confirmation_ticks"]

        self.DCA_TIERS = [
            ("entry", self.ENTRY, self.params["weight_entry"]),
            ("tier_1", self.params["dca_tier_1"], self.params["weight_tier_1"]),
            ("tier_2", self.params["dca_tier_2"], self.params["weight_tier_2"]),
            ("tier_3", self.params["dca_tier_3"], self.params["weight_tier_3"]),
        ]

        # Latest orderbook cache for cross-asset lookups
        self._latest_book: Dict[str, OrderbookTick] = {}  # asset_id -> tick

        self.tick_count = 0
        self._last_dashboard_time = 0.0  # epoch seconds
        self.DASHBOARD_INTERVAL = 60  # print every N seconds
        logger.info(
            f"Strategy initialised | mode={trading_mode} | "
            f"entry={self.ENTRY} | TP={self.TP_PCT} | SL={self.SL_PCT}"
        )

    # ──────────────────────── public API ───────────────────────

    def register_market_assets(self, market_id: str, asset_ids: List[str]):
        """Register asset IDs belonging to a market."""
        self.market_assets[market_id] = asset_ids

    async def on_tick(self, tick: OrderbookTick):
        """
        Main tick handler — called for every orderbook update.
        This is the live equivalent of the backtest main loop iteration.
        """
        if self.tracker.global_tp_hit:
            return

        self.tick_count += 1
        aid = tick.asset_id
        mid = tick.market_id
        ts = tick.timestamp
        best_bid = tick.best_bid
        best_ask = tick.best_ask
        bids = tick.bids
        asks = tick.asks

        if best_bid <= 0 or best_ask <= 0:
            return

        # Cache latest book for cross-asset lookups
        self._latest_book[aid] = tick

        # Must be a tracked market
        if mid not in self.tracker.markets:
            return
        ms = self.tracker.markets[mid]

        current_price = (best_bid + best_ask) / 2
        self.tracker.update_price(mid, aid, current_price, ts)

        has_pos = ms.has_position(aid)
        is_hedge = aid == ms.hedge_asset
        is_main = aid == ms.main_asset

        # Market progress
        market_progress = self._calc_progress(ms, ts)
        in_late_game = market_progress >= self.LATE_GAME_THRESHOLD

        # Trading window
        in_trading_window = self._in_trading_window(ms, ts)

        # Grace period
        in_grace_period = self._in_grace_period(ms, aid, ts)

        spread = best_ask - best_bid
        tick_is_noisy = spread > self.MAX_SPREAD_FOR_SL

        # ── GLOBAL P&L CHECK ──
        total_pnl = self.tracker.get_total_pnl()
        if total_pnl >= self.GLOBAL_TP:
            await self._handle_global_tp(ts)
            return

        # ── PANIC FLIP ──
        if self.PANIC_FLIP_ENABLED and not ms.panic_flip_triggered and not ms.hit_99:
            can_panic = not self.PANIC_FLIP_LATE_GAME_ONLY or in_late_game
            if can_panic:
                await self._check_panic_flip(ms, mid, ts)

        # ── HEDGE EXIT (hedge dropping → sell it) ──
        if ms.hedge_asset and aid == ms.hedge_asset and not ms.hit_99:
            await self._check_hedge_exit(
                ms, mid, aid, ts, best_bid, bids, tick_is_noisy
            )

        # ── HEDGE TRIGGER (main dropping → buy opposite) ──
        await self._check_hedge_trigger(
            ms, mid, aid, ts, current_price,
            best_bid, best_ask, tick_is_noisy, in_late_game,
        )

        # ── HEDGE PROMOTION ──
        await self._check_hedge_promotion(ms, mid, ts)

        # ── EXIT LOGIC ──
        if has_pos:
            exited = await self._check_exits(
                ms, mid, aid, ts, best_bid, best_ask, bids,
                is_hedge, is_main, in_late_game, in_grace_period, tick_is_noisy,
            )
            if exited:
                return

        # ── COOLDOWN ──
        if aid in ms.cooldowns:
            cooldown_until = ms.cooldowns[aid]
            if ts.isoformat() < cooldown_until:
                return
            del ms.cooldowns[aid]

        # ── ENTRY / DCA ──
        if ms.hit_99:
            return

        # Early entry check
        allow_early = False
        if self.EARLY_ENTRY_ENABLED and not in_trading_window and ms.active_asset is None:
            long_hist = self.tracker.get_long_price_history(mid, aid)
            eligible, reason = _check_early_entry_eligible(
                long_hist, current_price, market_progress, ts,
                self.EARLY_ENTRY_MIN_PROGRESS,
                self.EARLY_ENTRY_MAX_PROGRESS,
                self.EARLY_ENTRY_PRICE_THRESHOLD,
                self.EARLY_ENTRY_MAX_VOLATILITY,
                self.EARLY_ENTRY_MAX_RANGE,
                self.EARLY_ENTRY_MIN_DURATION_MINUTES,
                self.EARLY_ENTRY_NO_DROP_THRESHOLD,
                self.EARLY_ENTRY_NO_DROP_WINDOW,
            )
            if eligible:
                allow_early = True
                if not ms.early_entry_triggered:
                    logger.info(
                        f"🌅 EARLY ENTRY [{ms.name or mid[:20]}]: {reason}"
                    )

        if not in_trading_window and not allow_early:
            return

        if best_ask >= self.ENTRY and not ms.promoted:
            if ms.active_asset is None:
                ms.active_asset = aid
                ms.main_asset = aid
                if allow_early and not ms.early_entry_triggered:
                    ms.early_entry_triggered = True
                    ms.early_entry_reason = "stable_market"

            if ms.active_asset == aid:
                for tier_name, thresh, weight in self.DCA_TIERS:
                    if tier_name in ms.tiers_filled or best_ask < thresh:
                        continue
                    amt = min(ms.allocation * weight, ms.cash)
                    if amt <= 1:
                        continue
                    shares, cost, avg_p = self._exec_buy(
                        aid, amt, asks, best_ask
                    )
                    if shares > 0:
                        pos = ms.get_position(aid)
                        pos.shares += shares
                        pos.cost += cost
                        pos.update_avg()
                        ms.cash -= cost
                        ms.tiers_filled.append(tier_name)
                        ms.last_entry_time[aid] = ts.isoformat()

                        entry_reason = tier_name
                        if allow_early and tier_name == "entry":
                            entry_reason = "EARLY_" + tier_name

                        self.tracker.record_trade(
                            Trade(
                                timestamp=ts.isoformat(),
                                market_id=mid,
                                asset_id=aid,
                                action="BUY",
                                reason=entry_reason,
                                shares=shares,
                                price=avg_p,
                                cost=cost,
                                mode=self.mode,
                            )
                        )
                        logger.info(
                            f"📈 BUY [{ms.name or mid[:20]}] {entry_reason}: "
                            f"{shares:.2f} @ ${avg_p:.4f} = ${cost:.2f}"
                        )
                        self.print_dashboard(trigger=f"BUY_{entry_reason}")
                        break  # one tier per tick

        # Periodic state save
        if self.tick_count % 100 == 0:
            self.tracker.save_state()

        # Periodic dashboard
        now_epoch = time.time()
        if now_epoch - self._last_dashboard_time >= self.DASHBOARD_INTERVAL:
            self.print_dashboard()
            self._last_dashboard_time = now_epoch

    # ================================================================
    #  DASHBOARD
    # ================================================================

    def print_dashboard(self, trigger: str = "PERIODIC"):
        """Print a comprehensive overview of all positions to the console."""
        now = datetime.utcnow()
        total_pnl = self.tracker.get_total_pnl()
        total_value = self.tracker.get_total_value()
        realized = sum(ms.pnl for ms in self.tracker.markets.values())
        unrealized = total_pnl - realized

        lines = []
        sep = "=" * 90
        lines.append(f"\n{sep}")
        lines.append(
            f"  DASHBOARD [{trigger}]  {now.strftime('%Y-%m-%d %H:%M:%S')} UTC"
            f"  |  mode={self.mode}  |  ticks={self.tick_count:,}"
        )
        lines.append(sep)
        lines.append(
            f"  Capital: ${self.tracker.initial_capital:.2f}  |  "
            f"Portfolio: ${total_value:.2f}  |  "
            f"PnL Total: ${total_pnl:+.2f} ({total_pnl / self.tracker.initial_capital * 100:+.1f}%)"
        )
        lines.append(
            f"  Realized: ${realized:+.2f}  |  Unrealized: ${unrealized:+.2f}"
        )
        lines.append("-" * 90)

        has_any_position = False

        for mid, ms in self.tracker.markets.items():
            active_positions = [
                (aid, pos)
                for aid, pos in ms.positions.items()
                if pos.shares > 0
            ]

            label = ms.name or mid[:20]
            status_parts = []
            if ms.hit_99:
                status_parts.append("HIT_99")
            if ms.panic_flip_triggered:
                status_parts.append("PANIC_FLIP")
            if ms.promoted:
                status_parts.append("PROMOTED")
            if ms.hedge_asset:
                status_parts.append("HEDGED")
            if ms.early_entry_triggered:
                status_parts.append("EARLY")
            status_str = " | ".join(status_parts) if status_parts else ""

            if not active_positions:
                if ms.pnl != 0 or ms.sl_count or ms.tp_count:
                    lines.append(
                        f"  {label:30s}  FLAT  cash=${ms.cash:.2f}  "
                        f"realized=${ms.pnl:+.2f}  SL={ms.sl_count} TP={ms.tp_count}"
                        + (f"  [{status_str}]" if status_str else "")
                    )
                continue

            has_any_position = True
            tiers = ",".join(ms.tiers_filled) if ms.tiers_filled else "-"
            lines.append(
                f"  {label:30s}  cash=${ms.cash:.2f}  "
                f"tiers=[{tiers}]  SL={ms.sl_count} TP={ms.tp_count}"
                + (f"  [{status_str}]" if status_str else "")
            )

            for aid, pos in active_positions:
                cur_price = self.tracker.get_price(mid, aid)
                value = pos.shares * cur_price
                pnl_pos = value - pos.cost
                pnl_pct = (pnl_pos / pos.cost * 100) if pos.cost > 0 else 0.0
                role = ""
                if aid == ms.main_asset:
                    role = "MAIN"
                elif aid == ms.hedge_asset:
                    role = "HEDGE"
                lines.append(
                    f"    {role:6s}  {aid[:16]}..  "
                    f"shares={pos.shares:>8.2f}  "
                    f"avg=${pos.avg_price:.4f}  "
                    f"now=${cur_price:.4f}  "
                    f"val=${value:>8.2f}  "
                    f"pnl=${pnl_pos:>+7.2f} ({pnl_pct:>+5.1f}%)"
                )

        if not has_any_position:
            any_activity = any(
                ms.pnl != 0 or ms.sl_count or ms.tp_count
                for ms in self.tracker.markets.values()
            )
            if not any_activity:
                lines.append("  No active positions.")
        lines.append(sep + "\n")

        print("\n".join(lines), flush=True)

    def print_status(self):
        """Print a concise one-line status with position price info if present."""
        total_pnl = self.tracker.get_total_pnl()
        total_positions = sum(
            sum(1 for p in ms.positions.values() if p.shares > 0)
            for ms in self.tracker.markets.values()
        )
        
        status_parts = [
            f"ticks={self.tick_count:,}",
            f"pnl=${total_pnl:+.2f}",
            f"positions={total_positions}",
        ]
        
        # If there's an active position, show its prices
        if total_positions > 0:
            for mid, ms in self.tracker.markets.items():
                for aid, pos in ms.positions.items():
                    if pos.shares > 0:
                        cur_price = self.tracker.get_price(mid, aid)
                        status_parts.append(
                            f"[{ms.name or mid[:15]}] "
                            f"bought@${pos.avg_price:.4f} "
                            f"now@${cur_price:.4f}"
                        )
                        break  # just show first position for brevity
                if total_positions > 0:  # found a position
                    break
        
        logger.info(f"[STATUS] {' | '.join(status_parts)}")

    # ================================================================
    #  PRIVATE HELPERS
    # ================================================================

    def _calc_progress(self, ms: MarketState, ts: datetime) -> float:
        """Market game progress 0.0-1.0."""
        if not ms.start_time or not ms.end_time:
            return 0.5
        try:
            start = datetime.fromisoformat(ms.start_time)
            end = datetime.fromisoformat(ms.end_time)
            total = (end - start).total_seconds()
            if total <= 0:
                return 1.0
            return min(max((ts - start).total_seconds() / total, 0.0), 1.0)
        except Exception:
            return 0.5

    def _in_trading_window(self, ms: MarketState, ts: datetime) -> bool:
        if self.LAST_MINUTES_ONLY is None:
            return True
        if not ms.end_time:
            return True
        try:
            end = datetime.fromisoformat(ms.end_time)
            remaining = (end - ts).total_seconds()
            return remaining <= (self.LAST_MINUTES_ONLY * 60)
        except Exception:
            return True

    def _in_grace_period(self, ms: MarketState, aid: str, ts: datetime) -> bool:
        if aid not in ms.last_entry_time:
            return False
        try:
            entry_ts = datetime.fromisoformat(ms.last_entry_time[aid])
            return (ts - entry_ts).total_seconds() < self.ENTRY_GRACE_PERIOD
        except Exception:
            return False

    def _exec_buy(self, asset_id, amount, asks, best_ask):
        """Execute buy through order manager."""
        if self.mode == "live":
            return self.order_mgr.execute_buy(
                asset_id, amount, best_ask=best_ask, asks=asks
            )
        return self.order_mgr.execute_buy(asks, amount)

    def _exec_sell(self, asset_id, shares, bids, best_bid):
        """Execute sell through order manager."""
        if self.mode == "live":
            return self.order_mgr.execute_sell(
                asset_id, shares, best_bid=best_bid, bids=bids
            )
        return self.order_mgr.execute_sell(bids, shares)

    # ────────────── GLOBAL TP ──────────────

    async def _handle_global_tp(self, ts: datetime):
        self.tracker.global_tp_hit = True
        self.tracker.global_tp_ts = ts.isoformat()
        logger.info(f"🎯 GLOBAL TP HIT at {ts}")
        self.print_dashboard(trigger="GLOBAL_TP")
        for mid, ms in self.tracker.markets.items():
            for aid, pos in ms.positions.items():
                if pos.shares > 0:
                    price = self.tracker.get_price(mid, aid)
                    proceeds = pos.shares * price
                    pnl = proceeds - pos.cost
                    ms.pnl += pnl
                    ms.cash += proceeds
                    self.tracker.record_trade(
                        Trade(
                            timestamp=ts.isoformat(),
                            market_id=mid,
                            asset_id=aid,
                            action="SELL",
                            reason="GLOBAL_TP",
                            pnl=pnl,
                            shares=pos.shares,
                            price=price,
                            proceeds=proceeds,
                            mode=self.mode,
                        )
                    )
                    pos.shares = 0
                    pos.cost = 0
        self.tracker.save_state()

    # ────────────── PANIC FLIP ──────────────

    async def _check_panic_flip(self, ms: MarketState, mid: str, ts: datetime):
        all_assets = self.market_assets.get(mid, [])
        for check_aid in all_assets:
            if ms.panic_flip_triggered:
                break
            hist = self.tracker.get_price_history(mid, check_aid)
            if len(hist) < 5:
                continue
            check_price = self.tracker.get_price(mid, check_aid)
            if check_price < self.PANIC_FLIP_MIN_PRICE:
                continue

            old_price = (
                hist[-self.PANIC_FLIP_WINDOW][1]
                if len(hist) >= self.PANIC_FLIP_WINDOW
                else hist[0][1]
            )
            surge = check_price - old_price
            if surge < self.PANIC_FLIP_THRESHOLD:
                continue

            # Sell all losing positions
            for losing_aid in all_assets:
                if losing_aid == check_aid:
                    continue
                if ms.has_position(losing_aid):
                    losing_pos = ms.get_position(losing_aid)
                    tick = self._latest_book.get(losing_aid)
                    if tick:
                        sold, proceeds, _ = self._exec_sell(
                            losing_aid, losing_pos.shares, tick.bids, tick.best_bid
                        )
                        if sold > 0:
                            pnl = proceeds - losing_pos.cost
                            ms.pnl += pnl
                            ms.cash += proceeds
                            losing_pos.shares = 0
                            losing_pos.cost = 0
                            self.tracker.record_trade(
                                Trade(
                                    timestamp=ts.isoformat(),
                                    market_id=mid,
                                    asset_id=losing_aid,
                                    action="SELL",
                                    reason="PANIC_CLOSE_LOSER",
                                    pnl=pnl,
                                    proceeds=proceeds,
                                    mode=self.mode,
                                )
                            )

            # Buy winner
            winner_tick = self._latest_book.get(check_aid)
            if winner_tick and ms.cash > 1:
                buy_amount = ms.cash * 0.95
                shares, cost, avg_p = self._exec_buy(
                    check_aid, buy_amount, winner_tick.asks, winner_tick.best_ask
                )
                if shares > 0:
                    pos = ms.get_position(check_aid)
                    pos.shares += shares
                    pos.cost += cost
                    pos.update_avg()
                    ms.cash -= cost
                    self.tracker.record_trade(
                        Trade(
                            timestamp=ts.isoformat(),
                            market_id=mid,
                            asset_id=check_aid,
                            action="BUY",
                            reason="PANIC_BUY_WINNER",
                            shares=shares,
                            price=avg_p,
                            cost=cost,
                            mode=self.mode,
                        )
                    )

            ms.panic_flip_triggered = True
            ms.panic_flip_reason = f"surge_{surge:.2f}_winner@{check_price:.2f}"
            ms.main_asset = check_aid
            ms.hedge_asset = None
            ms.active_asset = check_aid
            logger.info(
                f"🚨 PANIC FLIP [{ms.name or mid[:20]}]: {ms.panic_flip_reason}"
            )
            self.print_dashboard(trigger="PANIC_FLIP")
            break

    # ────────────── HEDGE EXIT ──────────────

    async def _check_hedge_exit(
        self, ms: MarketState, mid: str, aid: str,
        ts: datetime, best_bid: float, bids, tick_is_noisy: bool,
    ):
        if tick_is_noisy:
            return
        pos = ms.get_position(aid)
        if pos.shares <= 0:
            return

        hedge_avg = pos.cost / pos.shares
        hedge_drop = hedge_avg - self.tracker.get_price(mid, aid)

        if hedge_drop >= self.HEDGE_EXIT_DROP_POINTS:
            ms.hedge_drop_confirm_count += 1
            if ms.hedge_drop_confirm_count >= self.HEDGE_EXIT_CONFIRMATION_TICKS:
                sold, proceeds, _ = self._exec_sell(
                    aid, pos.shares, bids, best_bid
                )
                if sold > 0:
                    pnl = proceeds - pos.cost
                    ms.pnl += pnl
                    ms.cash += proceeds
                    pos.shares = 0
                    pos.cost = 0
                    ms.hedge_asset = None
                    ms.last_hedge_time = ts.isoformat()
                    ms.hedge_drop_confirm_count = 0
                    self.tracker.record_trade(
                        Trade(
                            timestamp=ts.isoformat(),
                            market_id=mid,
                            asset_id=aid,
                            action="SELL",
                            reason="HEDGE_EXIT_DROP",
                            pnl=pnl,
                            proceeds=proceeds,
                            mode=self.mode,
                        )
                    )
                    logger.info(
                        f"🔻 HEDGE SOLD [{ms.name or mid[:20]}]: "
                        f"drop={hedge_drop:.2f} pnl=${pnl:+.2f}"
                    )
                    self.print_dashboard(trigger="HEDGE_EXIT")
        else:
            ms.hedge_drop_confirm_count = 0

    # ────────────── HEDGE TRIGGER ──────────────

    async def _check_hedge_trigger(
        self, ms: MarketState, mid: str, aid: str,
        ts: datetime, current_price: float,
        best_bid: float, best_ask: float,
        tick_is_noisy: bool, in_late_game: bool,
    ):
        main_aid = ms.main_asset
        if aid != main_aid:
            return
        if not ms.has_position(main_aid):
            return

        # Cooldown
        if ms.last_hedge_time:
            try:
                last = datetime.fromisoformat(ms.last_hedge_time)
                if (ts - last).total_seconds() < self.HEDGE_COOLDOWN_SECONDS:
                    return
            except Exception:
                pass

        can_hedge = (
            self.HEDGE_ENABLED
            and not ms.hedge_asset
            and not ms.hit_99
            and not tick_is_noisy
        )
        if self.HEDGE_LATE_GAME_ONLY and not in_late_game:
            can_hedge = False
        if not can_hedge:
            return

        pos = ms.get_position(aid)
        avg_entry = pos.cost / pos.shares if pos.shares > 0 else 0

        drop_detected = False
        drop_reason = ""

        # Fast drop
        hist = self.tracker.get_price_history(mid, aid)
        if len(hist) >= 2:
            cutoff = (ts - timedelta(seconds=self.HEDGE_DROP_WINDOW)).isoformat()
            old_prices = [(t, p) for t, p in hist if t <= cutoff]
            if old_prices:
                old_price = old_prices[-1][1]
                fast_drop = old_price - current_price
                if fast_drop >= self.HEDGE_DROP_POINTS:
                    drop_detected = True
                    drop_reason = f"FAST_DROP_{fast_drop:.2f}"

        # Slow drop
        if not drop_detected:
            drop_from_entry = avg_entry - current_price
            if drop_from_entry >= self.HEDGE_DROP_FROM_ENTRY:
                drop_detected = True
                drop_reason = f"SLOW_DROP_{drop_from_entry:.2f}"

        if drop_detected:
            ms.main_drop_confirm_count += 1
            if ms.main_drop_confirm_count >= self.HEDGE_DROP_CONFIRMATION_TICKS:
                opp_assets = [
                    a for a in self.market_assets.get(mid, []) if a != aid
                ]
                if opp_assets and ms.cash >= self.HEDGE_AMOUNT:
                    opp_aid = opp_assets[0]
                    opp_tick = self._latest_book.get(opp_aid)
                    if opp_tick:
                        shares, cost, avg_p = self._exec_buy(
                            opp_aid, HEDGE_AMOUNT,
                            opp_tick.asks, opp_tick.best_ask,
                        )
                        if shares > 0:
                            opp_pos = ms.get_position(opp_aid)
                            opp_pos.shares += shares
                            opp_pos.cost += cost
                            opp_pos.update_avg()
                            ms.cash -= cost
                            ms.hedge_asset = opp_aid
                            ms.hedge_count += 1
                            ms.last_hedge_time = ts.isoformat()
                            ms.main_drop_confirm_count = 0
                            self.tracker.record_trade(
                                Trade(
                                    timestamp=ts.isoformat(),
                                    market_id=mid,
                                    asset_id=opp_aid,
                                    action="BUY",
                                    reason=f"HEDGE_{drop_reason}",
                                    shares=shares,
                                    price=avg_p,
                                    cost=cost,
                                    mode=self.mode,
                                )
                            )
                            logger.info(
                                f"🛡️ HEDGE [{ms.name or mid[:20]}]: "
                                f"{drop_reason} ${cost:.2f} @ {avg_p:.2f}"
                            )
                            self.print_dashboard(trigger=f"HEDGE_{drop_reason}")
        else:
            ms.main_drop_confirm_count = 0

    # ────────────── HEDGE PROMOTION ──────────────

    async def _check_hedge_promotion(
        self, ms: MarketState, mid: str, ts: datetime
    ):
        if not ms.hedge_asset or not ms.main_asset or ms.promoted:
            return

        hedge_pos = ms.get_position(ms.hedge_asset)
        main_pos = ms.get_position(ms.main_asset)
        if hedge_pos.shares <= 0 or main_pos.shares <= 0:
            return

        hedge_price = self.tracker.get_price(mid, ms.hedge_asset)
        main_price = self.tracker.get_price(mid, ms.main_asset)

        hedge_value = hedge_pos.shares * hedge_price
        hedge_pnl = hedge_value - hedge_pos.cost
        hedge_ret = hedge_pnl / hedge_pos.cost if hedge_pos.cost > 0 else 0

        main_value = main_pos.shares * main_price
        main_pnl = main_value - main_pos.cost
        main_ret = main_pnl / main_pos.cost if main_pos.cost > 0 else 0

        promote = False
        reason = None

        if hedge_pnl >= self.HEDGE_PROMOTION_PNL_PCT * main_pos.cost:
            promote = True
            reason = f"HEDGE_PNL_{hedge_pnl:.2f}"
        elif hedge_price >= self.HEDGE_PROMOTION_PRICE:
            promote = True
            reason = f"HEDGE_PRICE_{hedge_price:.2f}"
        elif hedge_ret - main_ret >= self.HEDGE_PROMOTION_DELTA:
            promote = True
            reason = f"HEDGE_DELTA_{(hedge_ret - main_ret) * 100:.1f}%"

        if not promote:
            return

        # Sell main position
        main_tick = self._latest_book.get(ms.main_asset)
        if main_tick:
            sold, proceeds, _ = self._exec_sell(
                ms.main_asset, main_pos.shares, main_tick.bids, main_tick.best_bid
            )
            if sold > 0:
                pnl = proceeds - main_pos.cost
                ms.pnl += pnl
                ms.cash += proceeds
                main_pos.shares = 0
                main_pos.cost = 0
                self.tracker.record_trade(
                    Trade(
                        timestamp=ts.isoformat(),
                        market_id=mid,
                        asset_id=ms.main_asset,
                        action="SELL",
                        reason="PROMOTION_CLOSE_MAIN",
                        pnl=pnl,
                        proceeds=proceeds,
                        mode=self.mode,
                    )
                )

        ms.promoted = True
        ms.promotion_reason = reason
        ms.main_asset = ms.hedge_asset
        ms.hedge_asset = None
        ms.active_asset = ms.main_asset
        logger.info(f"🔄 PROMOTION [{ms.name or mid[:20]}]: {reason}")
        self.print_dashboard(trigger=f"PROMOTION_{reason}")

    # ────────────── EXITS ──────────────

    async def _check_exits(
        self, ms: MarketState, mid: str, aid: str, ts: datetime,
        best_bid: float, best_ask: float, bids,
        is_hedge: bool, is_main: bool,
        in_late_game: bool, in_grace_period: bool,
        tick_is_noisy: bool,
    ) -> bool:
        """Check exit conditions. Returns True if position was exited."""
        pos = ms.get_position(aid)
        if pos.shares <= 0:
            return False

        avg = pos.cost / pos.shares
        exit_reason = None
        has_hedge_now = ms.hedge_asset is not None

        # Instant exit at 0.99
        if best_bid >= self.INSTANT_EXIT_PRICE:
            exit_reason = "EXIT_99"
            ms.hit_99 = True

        elif is_hedge:
            pass  # Handled in _check_hedge_exit

        elif ms.promoted:
            pass  # Promoted position — just hold

        elif has_hedge_now and is_main:
            # Main with active hedge — only TP
            if best_bid >= avg * (1 + self.TP_PCT):
                exit_reason = "TAKE_PROFIT"

        else:
            # Normal position — SL + TP active
            sl_active = not in_late_game and not in_grace_period

            if sl_active and not tick_is_noisy:
                below_sl = False
                sl_type = None

                if best_bid < self.EXIT_SL:
                    below_sl = True
                    sl_type = "STOP_LOSS_ABS"
                elif best_bid < avg * (1 - self.SL_PCT):
                    below_sl = True
                    sl_type = "STOP_LOSS_PCT"

                if below_sl:
                    count = ms.sl_below_count.get(aid, 0) + 1
                    ms.sl_below_count[aid] = count
                    if count >= self.SL_CONFIRMATION_TICKS:
                        exit_reason = sl_type
                else:
                    ms.sl_below_count[aid] = 0

            if best_bid >= avg * (1 + self.TP_PCT):
                exit_reason = "TAKE_PROFIT"

        if not exit_reason:
            return False

        sold, proceeds, _ = self._exec_sell(aid, pos.shares, bids, best_bid)
        if sold <= 0:
            return False

        pnl = proceeds - pos.cost
        ms.pnl += pnl
        ms.cash += proceeds
        pos.shares = 0
        pos.cost = 0

        if is_main or (not ms.hedge_asset and not ms.promoted):
            ms.active_asset = None
            ms.tiers_filled = []

        cooldown_until = (
            ts + timedelta(seconds=30 * self.COOLDOWN)
        ).isoformat()
        ms.cooldowns[aid] = cooldown_until

        if "STOP" in exit_reason:
            ms.sl_count += 1
        else:
            ms.tp_count += 1

        self.tracker.record_trade(
            Trade(
                timestamp=ts.isoformat(),
                market_id=mid,
                asset_id=aid,
                action="SELL",
                reason=exit_reason,
                pnl=pnl,
                shares=sold,
                price=best_bid,
                proceeds=proceeds,
                mode=self.mode,
            )
        )
        icon = "🔴" if pnl < 0 else "🟢"
        logger.info(
            f"{icon} EXIT [{ms.name or mid[:20]}] {exit_reason}: pnl=${pnl:+.2f}"
        )
        self.print_dashboard(trigger=f"SELL_{exit_reason}")
        return True
