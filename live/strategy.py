"""
Live DCA Strategy Engine v19.

Mirrors the backtest logic from the notebook but operates tick-by-tick
in real time. Receives OrderbookTick objects and makes trading decisions.

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
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from live.config import (
    normalize_strategy_params,
    DEFAULT_STRATEGY_PARAMS,
    GLOBAL_TP_PCT,
    LAST_MINUTES_ONLY,
    LATE_GAME_THRESHOLD,
    EARLY_ENTRY_ENABLED,
    EARLY_ENTRY_MIN_PROGRESS,
    EARLY_ENTRY_MAX_PROGRESS,
    EARLY_ENTRY_PRICE_THRESHOLD,
    EARLY_ENTRY_MAX_VOLATILITY,
    EARLY_ENTRY_MAX_RANGE,
    EARLY_ENTRY_MIN_DURATION_MINUTES,
    EARLY_ENTRY_NO_DROP_THRESHOLD,
    EARLY_ENTRY_NO_DROP_WINDOW,
    HEDGE_ENABLED,
    HEDGE_DROP_POINTS,
    HEDGE_DROP_WINDOW,
    HEDGE_DROP_FROM_ENTRY,
    HEDGE_AMOUNT,
    HEDGE_LATE_GAME_ONLY,
    HEDGE_COOLDOWN_SECONDS,
    HEDGE_DROP_CONFIRMATION_TICKS,
    HEDGE_EXIT_CONFIRMATION_TICKS,
    HEDGE_EXIT_DROP_POINTS,
    PANIC_FLIP_ENABLED,
    PANIC_FLIP_THRESHOLD,
    PANIC_FLIP_WINDOW,
    PANIC_FLIP_MIN_PRICE,
    PANIC_FLIP_LATE_GAME_ONLY,
    HEDGE_PROMOTION_PNL_PCT,
    HEDGE_PROMOTION_PRICE,
    HEDGE_PROMOTION_DELTA,
    ENTRY_GRACE_PERIOD,
    INSTANT_EXIT_PRICE,
    MAX_SPREAD_FOR_SL,
    SL_CONFIRMATION_TICKS,
)
from live.position_tracker import PositionTracker, MarketState, Trade, Position
from live.orderbook_feed import OrderbookTick

logger = logging.getLogger(__name__)


def _calculate_stability_metrics(price_history: List) -> Dict:
    """Calculate stability metrics from price history list of (ts_str, price)."""
    if len(price_history) < 10:
        return {"volatility": float("inf"), "avg_price": 0, "range": float("inf"), "valid": False}
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
) -> Tuple[bool, str]:
    """Check if early entry conditions are met for a stable trending market."""
    if market_progress < EARLY_ENTRY_MIN_PROGRESS or market_progress >= EARLY_ENTRY_MAX_PROGRESS:
        return False, "outside_early_window"
    if current_price < EARLY_ENTRY_PRICE_THRESHOLD:
        return False, f"price_{current_price:.2f}_below_threshold"
    if len(price_history) < 20:
        return False, "insufficient_history"

    progress_in_window = (market_progress - EARLY_ENTRY_MIN_PROGRESS) / (
        EARLY_ENTRY_MAX_PROGRESS - EARLY_ENTRY_MIN_PROGRESS
    )
    relaxation_factor = progress_in_window

    adj_max_vol = EARLY_ENTRY_MAX_VOLATILITY * (1 + relaxation_factor * 1.5)
    adj_max_range = EARLY_ENTRY_MAX_RANGE * (1 + relaxation_factor * 1.5)
    adj_min_dur = EARLY_ENTRY_MIN_DURATION_MINUTES * (1 - relaxation_factor * 0.6)

    metrics = _calculate_stability_metrics(price_history)
    if not metrics["valid"]:
        return False, "invalid_metrics"
    if metrics["volatility"] > adj_max_vol:
        return False, f"volatility_{metrics['volatility']:.3f}"
    if metrics["range"] > adj_max_range:
        return False, f"range_{metrics['range']:.3f}"

    # check duration
    if price_history:
        first_ts_str = price_history[0][0]
        first_ts = datetime.fromisoformat(first_ts_str) if isinstance(first_ts_str, str) else first_ts_str
        duration_min = (ts - first_ts).total_seconds() / 60
        if duration_min < adj_min_dur:
            return False, f"duration_{duration_min:.1f}min"

    # check no recent drop
    cutoff = (ts - timedelta(seconds=EARLY_ENTRY_NO_DROP_WINDOW)).isoformat()
    recent = [(t, p) for t, p in price_history if t >= cutoff]
    if len(recent) >= 2:
        recent_max = max(p for _, p in recent)
        adj_drop = EARLY_ENTRY_NO_DROP_THRESHOLD * (1 + relaxation_factor * 0.5)
        if recent_max - current_price > adj_drop:
            return False, f"recent_drop_{recent_max - current_price:.3f}"

    return True, f"stable_early_entry_{market_progress:.1%}"


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
        Args:
            tracker: PositionTracker instance
            order_manager: PaperOrderManager or RealOrderManager
            params: Strategy params (uses defaults if None)
            trading_mode: "paper" or "real"
            market_assets: Dict mapping market_id -> list of asset_ids
        """
        self.tracker = tracker
        self.order_mgr = order_manager
        self.params = normalize_strategy_params(params)
        self.mode = trading_mode
        self.market_assets = market_assets or {}

        # Extract params
        self.ENTRY = self.params["entry_threshold"]
        self.EXIT_SL = self.params["exit_stop_loss"]
        self.SL_PCT = self.params["stop_loss_pct"]
        self.TP_PCT = self.params["take_profit_pct"]
        self.COOLDOWN = self.params["cooldown_periods"]
        self.GLOBAL_TP = tracker.initial_capital * GLOBAL_TP_PCT

        self.DCA_TIERS = [
            ("entry", self.ENTRY, self.params["weight_entry"]),
            ("tier_1", self.params["dca_tier_1"], self.params["weight_tier_1"]),
            ("tier_2", self.params["dca_tier_2"], self.params["weight_tier_2"]),
            ("tier_3", self.params["dca_tier_3"], self.params["weight_tier_3"]),
        ]

        # Latest orderbook data cache for cross-asset lookups
        self._latest_book: Dict[str, OrderbookTick] = {}  # asset_id -> last tick

        self.tick_count = 0
        logger.info(f"Strategy initialized | mode={trading_mode} | entry={self.ENTRY}")

    def register_market_assets(self, market_id: str, asset_ids: List[str]):
        """Register asset IDs belonging to a market."""
        self.market_assets[market_id] = asset_ids

    async def on_tick(self, tick: OrderbookTick):
        """
        Main tick handler. Called for every orderbook update.
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

        # Cache latest book for this asset
        self._latest_book[aid] = tick

        # Get market state
        if mid not in self.tracker.markets:
            return  # Not a tracked market
        ms = self.tracker.markets[mid]

        current_price = (best_bid + best_ask) / 2
        self.tracker.update_price(mid, aid, current_price, ts)

        has_pos = ms.has_position(aid)
        is_hedge = (aid == ms.hedge_asset)
        is_main = (aid == ms.main_asset)

        # Calculate market progress
        market_progress = self._calc_progress(ms, ts)
        in_late_game = market_progress >= LATE_GAME_THRESHOLD

        # Trading window
        in_trading_window = self._in_trading_window(ms, ts)

        # Grace period
        in_grace_period = self._in_grace_period(ms, aid, ts)

        spread = best_ask - best_bid
        tick_is_noisy = spread > MAX_SPREAD_FOR_SL

        # ============================================================
        # GLOBAL PnL CHECK
        # ============================================================
        total_pnl = self.tracker.get_total_pnl()
        if total_pnl >= self.GLOBAL_TP:
            await self._handle_global_tp(ts)
            return

        # ============================================================
        # PANIC FLIP
        # ============================================================
        if PANIC_FLIP_ENABLED and not ms.panic_flip_triggered and not ms.hit_99:
            can_panic = not PANIC_FLIP_LATE_GAME_ONLY or in_late_game
            if can_panic:
                await self._check_panic_flip(ms, mid, ts)

        # ============================================================
        # HEDGE EXIT (hedge dropping -> sell it)
        # ============================================================
        if ms.hedge_asset and aid == ms.hedge_asset and not ms.hit_99:
            await self._check_hedge_exit(ms, mid, aid, ts, best_bid, bids, tick_is_noisy)

        # ============================================================
        # HEDGE TRIGGER (main dropping -> buy opposite)
        # ============================================================
        await self._check_hedge_trigger(ms, mid, aid, ts, current_price, best_bid, best_ask, tick_is_noisy, in_late_game)

        # ============================================================
        # HEDGE PROMOTION
        # ============================================================
        await self._check_hedge_promotion(ms, mid, ts)

        # ============================================================
        # EXIT LOGIC
        # ============================================================
        if has_pos:
            exited = await self._check_exits(ms, mid, aid, ts, best_bid, best_ask, bids,
                                             is_hedge, is_main, in_late_game, in_grace_period, tick_is_noisy)
            if exited:
                return

        # Cooldown
        if aid in ms.cooldowns:
            cooldown_until = ms.cooldowns[aid]
            if ts.isoformat() < cooldown_until:
                return
            del ms.cooldowns[aid]

        # ============================================================
        # ENTRY / DCA LOGIC
        # ============================================================
        if ms.hit_99:
            return

        # Early entry
        allow_early = False
        if EARLY_ENTRY_ENABLED and not in_trading_window and ms.active_asset is None:
            long_hist = self.tracker.get_long_price_history(mid, aid)
            eligible, reason = _check_early_entry_eligible(long_hist, current_price, market_progress, ts)
            if eligible:
                allow_early = True
                if not ms.early_entry_triggered:
                    logger.info(f"🌅 EARLY ENTRY [{ms.name or mid[:20]}]: {reason}")

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

                    # Log orderbook state before execution
                    logger.info(f"💰 Executing BUY for {ms.name or mid[:20]} | "
                               f"Amount: ${amt:.2f} | "
                               f"Best ask: ${best_ask:.4f} | "
                               f"Ask levels: {len(asks) if asks else 0} | "
                               f"Bid levels: {len(bids) if bids else 0}")
                    if asks and len(asks) > 0:
                        logger.info(f"   First 3 asks: {asks[:3]}")
                    else:
                        logger.warning(f"⚠️  NO ASK LEVELS AVAILABLE for execution!")

                    shares, cost, avg_p = self._exec_buy(aid, amt, asks, best_ask)
                    if shares > 0:
                        pos = ms.get_position(aid)
                        
                        # Capture orderbook snapshot on first buy
                        self._capture_entry_orderbook(pos, ts, best_bid, best_ask, bids, asks)
                        
                        pos.shares += shares
                        pos.cost += cost
                        pos.update_avg()
                        ms.cash -= cost
                        ms.tiers_filled.append(tier_name)
                        ms.last_entry_time[aid] = ts.isoformat()

                        entry_reason = tier_name
                        if allow_early and tier_name == "entry":
                            entry_reason = "EARLY_" + tier_name

                        self.tracker.record_trade(Trade(
                            timestamp=ts.isoformat(), market_id=mid, asset_id=aid,
                            action="BUY", reason=entry_reason, shares=shares,
                            price=avg_p, cost=cost, mode=self.mode,
                        ))
                        logger.info(f"📈 BUY [{ms.name or mid[:20]}] {entry_reason}: "
                                    f"{shares:.2f} @ ${avg_p:.4f} = ${cost:.2f}")
                        break  # one tier per tick

        # Periodic save
        if self.tick_count % 100 == 0:
            self.tracker.save_state()

    # ================================================================
    # PRIVATE HELPERS
    # ================================================================

    def _calc_progress(self, ms: MarketState, ts: datetime) -> float:
        """Calculate market game progress 0.0 - 1.0."""
        if not ms.start_time or not ms.end_time:
            return 0.5  # Unknown, assume mid-game
        try:
            start = datetime.fromisoformat(ms.start_time)
            end = datetime.fromisoformat(ms.end_time)
            total = (end - start).total_seconds()
            if total <= 0:
                return 1.0
            elapsed = (ts - start).total_seconds()
            return min(max(elapsed / total, 0.0), 1.0)
        except Exception:
            return 0.5

    def _in_trading_window(self, ms: MarketState, ts: datetime) -> bool:
        if LAST_MINUTES_ONLY is None:
            return True
        if not ms.end_time:
            return True  # Can't determine, allow trading
        try:
            end = datetime.fromisoformat(ms.end_time)
            remaining = (end - ts).total_seconds()
            return remaining <= (LAST_MINUTES_ONLY * 60)
        except Exception:
            return True

    def _in_grace_period(self, ms: MarketState, aid: str, ts: datetime) -> bool:
        if aid not in ms.last_entry_time:
            return False
        try:
            entry_ts = datetime.fromisoformat(ms.last_entry_time[aid])
            return (ts - entry_ts).total_seconds() < ENTRY_GRACE_PERIOD
        except Exception:
            return False

    def _exec_buy(self, asset_id: str, amount: float, asks, best_ask: float):
        """Execute buy through order manager."""
        if self.mode == "real":
            return self.order_mgr.execute_buy(asset_id, amount, best_ask=best_ask, asks=asks)
        else:
            return self.order_mgr.execute_buy(asks, amount)

    def _exec_sell(self, asset_id: str, shares: float, bids, best_bid: float):
        """Execute sell through order manager."""
        if self.mode == "real":
            return self.order_mgr.execute_sell(asset_id, shares, best_bid=best_bid, bids=bids)
        else:
            return self.order_mgr.execute_sell(bids, shares)
    
    def _capture_entry_orderbook(self, pos: "Position", ts: datetime, best_bid: float, best_ask: float, bids, asks):
        """Capture orderbook snapshot for position entry (only if first buy)."""
        if pos.entry_timestamp is None:  # Only capture on first buy
            pos.entry_timestamp = ts.isoformat()
            
            # Parse and sort bids (highest first) and asks (lowest first)
            parsed_bids = []
            parsed_asks = []
            
            if bids:
                for b in bids:
                    price = float(b.get("price", 0))
                    size = float(b.get("size", 0))
                    if price > 0 and size > 0:
                        parsed_bids.append({"price": price, "size": size})
                parsed_bids.sort(key=lambda x: x["price"], reverse=True)  # Highest bid first
            
            if asks:
                for a in asks:
                    price = float(a.get("price", 0))
                    size = float(a.get("size", 0))
                    if price > 0 and size > 0:
                        parsed_asks.append({"price": price, "size": size})
                parsed_asks.sort(key=lambda x: x["price"])  # Lowest ask first
            
            # Get actual best bid/ask from sorted levels
            actual_best_bid = parsed_bids[0]["price"] if parsed_bids else 0.0
            actual_best_ask = parsed_asks[0]["price"] if parsed_asks else 0.0
            
            pos.entry_best_bid = actual_best_bid
            pos.entry_best_ask = actual_best_ask
            pos.entry_spread = actual_best_ask - actual_best_bid if actual_best_bid > 0 and actual_best_ask > 0 else 0.0
            
            # Store top 3 levels (already sorted)
            pos.entry_bids_top3 = parsed_bids[:3] if parsed_bids else []
            pos.entry_asks_top3 = parsed_asks[:3] if parsed_asks else []

    async def _handle_global_tp(self, ts: datetime):
        """Handle global take profit hit."""
        self.tracker.global_tp_hit = True
        self.tracker.global_tp_ts = ts.isoformat()
        logger.info(f"🎯 GLOBAL TP HIT at {ts}")

        for mid, ms in self.tracker.markets.items():
            for aid, pos in ms.positions.items():
                if pos.shares > 0:
                    price = self.tracker.get_price(mid, aid)
                    proceeds = pos.shares * price
                    pnl = proceeds - pos.cost
                    ms.pnl += pnl
                    ms.cash += proceeds
                    self.tracker.record_trade(Trade(
                        timestamp=ts.isoformat(), market_id=mid, asset_id=aid,
                        action="SELL", reason="GLOBAL_TP", pnl=pnl,
                        shares=pos.shares, price=price, proceeds=proceeds, mode=self.mode,
                    ))
                    pos.shares = 0
                    pos.cost = 0

        self.tracker.save_state()

    async def _check_panic_flip(self, ms: MarketState, mid: str, ts: datetime):
        """Check for panic flip conditions across all assets in market."""
        all_assets = self.market_assets.get(mid, [])
        for check_aid in all_assets:
            if ms.panic_flip_triggered:
                break
            hist = self.tracker.get_price_history(mid, check_aid)
            if len(hist) < 5:
                continue
            check_price = self.tracker.get_price(mid, check_aid)
            if check_price < PANIC_FLIP_MIN_PRICE:
                continue

            if len(hist) >= PANIC_FLIP_WINDOW:
                old_price = hist[-PANIC_FLIP_WINDOW][1]
            else:
                old_price = hist[0][1]

            surge = check_price - old_price
            if surge >= PANIC_FLIP_THRESHOLD:
                # Sell losing positions, buy winner
                for losing_aid in all_assets:
                    if losing_aid == check_aid:
                        continue
                    if ms.has_position(losing_aid):
                        losing_pos = ms.get_position(losing_aid)
                        # Sell the loser
                        tick = self._latest_book.get(losing_aid)
                        if tick:
                            sold, proceeds, _ = self._exec_sell(losing_aid, losing_pos.shares, tick.bids, tick.best_bid)
                            if sold > 0:
                                pnl = proceeds - losing_pos.cost
                                ms.pnl += pnl
                                ms.cash += proceeds
                                losing_pos.shares = 0
                                losing_pos.cost = 0
                                self.tracker.record_trade(Trade(
                                    timestamp=ts.isoformat(), market_id=mid, asset_id=losing_aid,
                                    action="SELL", reason="PANIC_CLOSE_LOSER", pnl=pnl,
                                    proceeds=proceeds, mode=self.mode,
                                ))

                # Buy winner
                winner_tick = self._latest_book.get(check_aid)
                if winner_tick and ms.cash > 1:
                    buy_amount = ms.cash * 0.95
                    shares, cost, avg_p = self._exec_buy(check_aid, buy_amount, winner_tick.asks, winner_tick.best_ask)
                    if shares > 0:
                        pos = ms.get_position(check_aid)
                        
                        # Capture orderbook snapshot on first buy
                        self._capture_entry_orderbook(pos, ts, winner_tick.best_bid, winner_tick.best_ask, 
                                                     winner_tick.bids, winner_tick.asks)
                        
                        pos.shares += shares
                        pos.cost += cost
                        pos.update_avg()
                        ms.cash -= cost
                        self.tracker.record_trade(Trade(
                            timestamp=ts.isoformat(), market_id=mid, asset_id=check_aid,
                            action="BUY", reason="PANIC_BUY_WINNER", shares=shares,
                            price=avg_p, cost=cost, mode=self.mode,
                        ))

                ms.panic_flip_triggered = True
                ms.panic_flip_reason = f"surge_{surge:.2f}_winner@{check_price:.2f}"
                ms.main_asset = check_aid
                ms.hedge_asset = None
                ms.active_asset = check_aid
                logger.info(f"🚨 PANIC FLIP [{ms.name or mid[:20]}]: {ms.panic_flip_reason}")
                break

    async def _check_hedge_exit(self, ms: MarketState, mid: str, aid: str,
                                ts: datetime, best_bid: float, bids, tick_is_noisy: bool):
        """Check if hedge should be sold (dropping)."""
        if tick_is_noisy:
            return
        pos = ms.get_position(aid)
        if pos.shares <= 0:
            return

        hedge_avg = pos.cost / pos.shares
        hedge_drop = hedge_avg - self.tracker.get_price(mid, aid)

        if hedge_drop >= HEDGE_EXIT_DROP_POINTS:
            ms.hedge_drop_confirm_count += 1
            if ms.hedge_drop_confirm_count >= HEDGE_EXIT_CONFIRMATION_TICKS:
                sold, proceeds, _ = self._exec_sell(aid, pos.shares, bids, best_bid)
                if sold > 0:
                    pnl = proceeds - pos.cost
                    ms.pnl += pnl
                    ms.cash += proceeds
                    pos.shares = 0
                    pos.cost = 0
                    ms.hedge_asset = None
                    ms.last_hedge_time = ts.isoformat()
                    ms.hedge_drop_confirm_count = 0
                    self.tracker.record_trade(Trade(
                        timestamp=ts.isoformat(), market_id=mid, asset_id=aid,
                        action="SELL", reason="HEDGE_EXIT_DROP", pnl=pnl,
                        proceeds=proceeds, mode=self.mode,
                    ))
                    logger.info(f"🔻 HEDGE SOLD [{ms.name or mid[:20]}]: drop={hedge_drop:.2f} pnl=${pnl:+.2f}")
        else:
            ms.hedge_drop_confirm_count = 0

    async def _check_hedge_trigger(self, ms: MarketState, mid: str, aid: str,
                                   ts: datetime, current_price: float,
                                   best_bid: float, best_ask: float,
                                   tick_is_noisy: bool, in_late_game: bool):
        """Check if we should hedge the main position."""
        main_aid = ms.main_asset
        if aid != main_aid:
            return
        if not ms.has_position(main_aid):
            return

        # Cooldown
        if ms.last_hedge_time:
            try:
                last_hedge = datetime.fromisoformat(ms.last_hedge_time)
                if (ts - last_hedge).total_seconds() < HEDGE_COOLDOWN_SECONDS:
                    return
            except Exception:
                pass

        can_hedge = (
            HEDGE_ENABLED
            and not ms.hedge_asset
            and not ms.hit_99
            and not tick_is_noisy
        )
        if HEDGE_LATE_GAME_ONLY and not in_late_game:
            can_hedge = False

        if not can_hedge:
            return

        pos = ms.get_position(aid)
        avg_entry = pos.cost / pos.shares if pos.shares > 0 else 0

        drop_detected = False
        drop_reason = ""

        # Fast drop check
        hist = self.tracker.get_price_history(mid, aid)
        if len(hist) >= 2:
            cutoff = (ts - timedelta(seconds=HEDGE_DROP_WINDOW)).isoformat()
            old_prices = [(t, p) for t, p in hist if t <= cutoff]
            if old_prices:
                old_price = old_prices[-1][1]
                fast_drop = old_price - current_price
                if fast_drop >= HEDGE_DROP_POINTS:
                    drop_detected = True
                    drop_reason = f"FAST_DROP_{fast_drop:.2f}"

        # Slow drop check
        if not drop_detected:
            drop_from_entry = avg_entry - current_price
            if drop_from_entry >= HEDGE_DROP_FROM_ENTRY:
                drop_detected = True
                drop_reason = f"SLOW_DROP_{drop_from_entry:.2f}"

        if drop_detected:
            ms.main_drop_confirm_count += 1
            if ms.main_drop_confirm_count >= HEDGE_DROP_CONFIRMATION_TICKS:
                opp_assets = [a for a in self.market_assets.get(mid, []) if a != aid]
                if opp_assets and ms.cash >= HEDGE_AMOUNT:
                    opp_aid = opp_assets[0]
                    opp_tick = self._latest_book.get(opp_aid)
                    if opp_tick:
                        shares, cost, avg_p = self._exec_buy(
                            opp_aid, HEDGE_AMOUNT, opp_tick.asks, opp_tick.best_ask
                        )
                        if shares > 0:
                            opp_pos = ms.get_position(opp_aid)
                            
                            # Capture orderbook snapshot on first buy
                            self._capture_entry_orderbook(opp_pos, ts, opp_tick.best_bid, opp_tick.best_ask,
                                                         opp_tick.bids, opp_tick.asks)
                            
                            opp_pos.shares += shares
                            opp_pos.cost += cost
                            opp_pos.update_avg()
                            ms.cash -= cost
                            ms.hedge_asset = opp_aid
                            ms.hedge_count += 1
                            ms.last_hedge_time = ts.isoformat()
                            ms.main_drop_confirm_count = 0

                            self.tracker.record_trade(Trade(
                                timestamp=ts.isoformat(), market_id=mid, asset_id=opp_aid,
                                action="BUY", reason=f"HEDGE_{drop_reason}",
                                shares=shares, price=avg_p, cost=cost, mode=self.mode,
                            ))
                            logger.info(
                                f"🛡️ HEDGE [{ms.name or mid[:20]}]: {drop_reason} "
                                f"${cost:.2f} @ {avg_p:.2f}"
                            )
        else:
            ms.main_drop_confirm_count = 0

    async def _check_hedge_promotion(self, ms: MarketState, mid: str, ts: datetime):
        """Check if hedge should be promoted to main position."""
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
        hedge_return = hedge_pnl / hedge_pos.cost if hedge_pos.cost > 0 else 0

        main_value = main_pos.shares * main_price
        main_pnl = main_value - main_pos.cost
        main_return = main_pnl / main_pos.cost if main_pos.cost > 0 else 0

        promote = False
        reason = None

        if hedge_pnl >= HEDGE_PROMOTION_PNL_PCT * main_pos.cost:
            promote = True
            reason = f"HEDGE_PNL_{hedge_pnl:.2f}"
        elif hedge_price >= HEDGE_PROMOTION_PRICE:
            promote = True
            reason = f"HEDGE_PRICE_{hedge_price:.2f}"
        elif hedge_return - main_return >= HEDGE_PROMOTION_DELTA:
            promote = True
            reason = f"HEDGE_DELTA_{(hedge_return - main_return)*100:.1f}%"

        if promote:
            # Sell main
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
                    self.tracker.record_trade(Trade(
                        timestamp=ts.isoformat(), market_id=mid, asset_id=ms.main_asset,
                        action="SELL", reason="PROMOTION_CLOSE_MAIN", pnl=pnl,
                        proceeds=proceeds, mode=self.mode,
                    ))

            ms.promoted = True
            ms.promotion_reason = reason
            ms.main_asset = ms.hedge_asset
            ms.hedge_asset = None
            ms.active_asset = ms.main_asset
            logger.info(f"🔄 PROMOTION [{ms.name or mid[:20]}]: {reason}")

    async def _check_exits(self, ms: MarketState, mid: str, aid: str, ts: datetime,
                           best_bid: float, best_ask: float, bids,
                           is_hedge: bool, is_main: bool,
                           in_late_game: bool, in_grace_period: bool,
                           tick_is_noisy: bool) -> bool:
        """Check exit conditions for a position. Returns True if position was exited."""
        pos = ms.get_position(aid)
        if pos.shares <= 0:
            return False

        avg = pos.cost / pos.shares
        exit_reason = None
        has_hedge_now = ms.hedge_asset is not None

        # Instant exit at 0.99
        if best_bid >= INSTANT_EXIT_PRICE:
            exit_reason = "EXIT_99"
            ms.hit_99 = True

        elif is_hedge:
            pass  # Hedge exits handled separately

        elif ms.promoted:
            pass  # Promoted - just hold

        elif has_hedge_now and is_main:
            # Main with hedge - only TP
            if best_bid >= avg * (1 + self.TP_PCT):
                exit_reason = "TAKE_PROFIT"

        else:
            # Normal: SL + TP
            sl_active = True
            if in_late_game:
                sl_active = False
            if in_grace_period:
                sl_active = False

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
                    if count >= SL_CONFIRMATION_TICKS:
                        exit_reason = sl_type
                else:
                    ms.sl_below_count[aid] = 0

            if best_bid >= avg * (1 + self.TP_PCT):
                exit_reason = "TAKE_PROFIT"

        if exit_reason:
            sold, proceeds, _ = self._exec_sell(aid, pos.shares, bids, best_bid)
            if sold > 0:
                pnl = proceeds - pos.cost
                ms.pnl += pnl
                ms.cash += proceeds
                pos.shares = 0
                pos.cost = 0

                if is_main or (not ms.hedge_asset and not ms.promoted):
                    ms.active_asset = None
                    ms.tiers_filled = []

                cooldown_until = (ts + timedelta(seconds=30 * self.COOLDOWN)).isoformat()
                ms.cooldowns[aid] = cooldown_until

                if "STOP" in exit_reason:
                    ms.sl_count += 1
                else:
                    ms.tp_count += 1

                self.tracker.record_trade(Trade(
                    timestamp=ts.isoformat(), market_id=mid, asset_id=aid,
                    action="SELL", reason=exit_reason, pnl=pnl,
                    shares=sold, price=best_bid, proceeds=proceeds, mode=self.mode,
                ))
                logger.info(f"{'🔴' if pnl < 0 else '🟢'} EXIT [{ms.name or mid[:20]}] "
                            f"{exit_reason}: pnl=${pnl:+.2f}")
                return True
        return False
