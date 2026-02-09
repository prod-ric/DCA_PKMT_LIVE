"""
Position tracking and state persistence for live DCA trading.

Tracks all positions, cash, PnL, and persists state to disk
for crash recovery.
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field, asdict

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """A single position in an asset."""
    asset_id: str
    shares: float = 0.0
    cost: float = 0.0
    avg_price: float = 0.0

    def update_avg(self):
        if self.shares > 0:
            self.avg_price = self.cost / self.shares
        else:
            self.avg_price = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        return cls(**d)


@dataclass
class MarketState:
    """State for a single market."""
    market_id: str
    name: str = ""
    allocation: float = 0.0
    cash: float = 0.0
    pnl: float = 0.0
    active_asset: Optional[str] = None
    main_asset: Optional[str] = None
    hedge_asset: Optional[str] = None
    tiers_filled: List[str] = field(default_factory=list)
    positions: Dict[str, Position] = field(default_factory=dict)
    cooldowns: Dict[str, str] = field(default_factory=dict)  # asset_id -> ISO timestamp
    sl_count: int = 0
    tp_count: int = 0
    hedge_count: int = 0
    promoted: bool = False
    promotion_reason: Optional[str] = None
    hit_99: bool = False
    sl_below_count: Dict[str, int] = field(default_factory=dict)
    early_entry_triggered: bool = False
    early_entry_reason: Optional[str] = None
    last_entry_time: Dict[str, str] = field(default_factory=dict)  # asset_id -> ISO timestamp
    last_hedge_time: Optional[str] = None
    main_drop_confirm_count: int = 0
    hedge_drop_confirm_count: int = 0
    panic_flip_triggered: bool = False
    panic_flip_reason: Optional[str] = None
    # end_time for market progress calculation
    start_time: Optional[str] = None
    end_time: Optional[str] = None

    def get_position(self, asset_id: str) -> Position:
        if asset_id not in self.positions:
            self.positions[asset_id] = Position(asset_id=asset_id)
        return self.positions[asset_id]

    def has_position(self, asset_id: str) -> bool:
        p = self.positions.get(asset_id)
        return p is not None and p.shares > 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["positions"] = {k: v.to_dict() if isinstance(v, Position) else v
                          for k, v in self.positions.items()}
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "MarketState":
        positions_raw = d.pop("positions", {})
        positions = {}
        for k, v in positions_raw.items():
            if isinstance(v, dict):
                positions[k] = Position.from_dict(v)
            else:
                positions[k] = v
        obj = cls(**d)
        obj.positions = positions
        return obj


@dataclass
class Trade:
    """A single trade record."""
    timestamp: str
    market_id: str
    asset_id: str
    action: str  # BUY or SELL
    reason: str
    shares: float = 0.0
    price: float = 0.0
    cost: float = 0.0
    proceeds: float = 0.0
    pnl: float = 0.0
    mode: str = "paper"  # paper or real

    def to_dict(self) -> dict:
        return asdict(self)


class PositionTracker:
    """
    Manages all market states, positions, and trade history.
    Persists to disk for crash recovery.
    """

    def __init__(self, state_file: str = "live/state.json",
                 trade_log_file: str = "live/trades.jsonl",
                 initial_capital: float = 500.0):
        self.state_file = state_file
        self.trade_log_file = trade_log_file
        self.initial_capital = initial_capital
        self.markets: Dict[str, MarketState] = {}
        self.trades: List[Trade] = []
        self.global_tp_hit = False
        self.global_tp_ts: Optional[str] = None
        self.price_cache: Dict[str, float] = {}  # (market_id, asset_id) key as string

        # Price history for strategy (kept in memory, not persisted)
        self.price_history: Dict[str, Dict[str, List]] = {}  # market -> asset -> [(ts, price)]
        self.long_price_history: Dict[str, Dict[str, List]] = {}

        os.makedirs(os.path.dirname(state_file) if os.path.dirname(state_file) else ".", exist_ok=True)

    def init_market(self, market_id: str, name: str = "",
                    allocation: float = 0.0,
                    start_time: str = None, end_time: str = None) -> MarketState:
        """Initialize or get a market state."""
        if market_id not in self.markets:
            self.markets[market_id] = MarketState(
                market_id=market_id,
                name=name,
                allocation=allocation,
                cash=allocation,
                start_time=start_time,
                end_time=end_time,
            )
            logger.info(f"Initialized market {name or market_id[:20]}... alloc=${allocation:.2f}")
        return self.markets[market_id]

    def record_trade(self, trade: Trade):
        """Record a trade and append to log file."""
        self.trades.append(trade)
        # Append to JSONL file
        try:
            os.makedirs(os.path.dirname(self.trade_log_file) if os.path.dirname(self.trade_log_file) else ".", exist_ok=True)
            with open(self.trade_log_file, "a") as f:
                f.write(json.dumps(trade.to_dict()) + "\n")
        except Exception as e:
            logger.error(f"Failed to write trade log: {e}")

    def update_price(self, market_id: str, asset_id: str, price: float, ts: datetime):
        """Update price cache and history."""
        cache_key = f"{market_id}:{asset_id}"
        self.price_cache[cache_key] = price

        # Short-term history (5 min)
        if market_id not in self.price_history:
            self.price_history[market_id] = {}
        if asset_id not in self.price_history[market_id]:
            self.price_history[market_id][asset_id] = []
        self.price_history[market_id][asset_id].append((ts.isoformat(), price))
        # Trim to 5 minutes
        cutoff = (ts - __import__("datetime").timedelta(seconds=300)).isoformat()
        self.price_history[market_id][asset_id] = [
            (t, p) for t, p in self.price_history[market_id][asset_id] if t >= cutoff
        ]

        # Long-term history (60 min)
        if market_id not in self.long_price_history:
            self.long_price_history[market_id] = {}
        if asset_id not in self.long_price_history[market_id]:
            self.long_price_history[market_id][asset_id] = []
        self.long_price_history[market_id][asset_id].append((ts.isoformat(), price))
        long_cutoff = (ts - __import__("datetime").timedelta(minutes=60)).isoformat()
        self.long_price_history[market_id][asset_id] = [
            (t, p) for t, p in self.long_price_history[market_id][asset_id] if t >= long_cutoff
        ]

    def get_price(self, market_id: str, asset_id: str) -> float:
        cache_key = f"{market_id}:{asset_id}"
        return self.price_cache.get(cache_key, 0.0)

    def get_price_history(self, market_id: str, asset_id: str) -> List:
        return self.price_history.get(market_id, {}).get(asset_id, [])

    def get_long_price_history(self, market_id: str, asset_id: str) -> List:
        return self.long_price_history.get(market_id, {}).get(asset_id, [])

    def get_total_pnl(self) -> float:
        """Get total realized + unrealized PnL across all markets."""
        realized = sum(ms.pnl for ms in self.markets.values())
        unrealized = 0.0
        for ms in self.markets.values():
            for aid, pos in ms.positions.items():
                if pos.shares > 0:
                    price = self.get_price(ms.market_id, aid)
                    unrealized += price * pos.shares - pos.cost
        return realized + unrealized

    def get_total_value(self) -> float:
        """Get total portfolio value (cash + positions)."""
        total = 0.0
        for ms in self.markets.values():
            total += ms.cash
            for aid, pos in ms.positions.items():
                if pos.shares > 0:
                    price = self.get_price(ms.market_id, aid)
                    total += price * pos.shares
        return total

    def save_state(self):
        """Persist current state to disk."""
        state = {
            "timestamp": datetime.utcnow().isoformat(),
            "initial_capital": self.initial_capital,
            "global_tp_hit": self.global_tp_hit,
            "global_tp_ts": self.global_tp_ts,
            "markets": {k: v.to_dict() for k, v in self.markets.items()},
            "price_cache": self.price_cache,
        }
        try:
            tmp = self.state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f, indent=2, default=str)
            os.replace(tmp, self.state_file)
            logger.debug("State saved")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def load_state(self) -> bool:
        """Load state from disk. Returns True if loaded successfully."""
        if not os.path.exists(self.state_file):
            return False
        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)
            self.initial_capital = state.get("initial_capital", self.initial_capital)
            self.global_tp_hit = state.get("global_tp_hit", False)
            self.global_tp_ts = state.get("global_tp_ts")
            self.price_cache = state.get("price_cache", {})
            for k, v in state.get("markets", {}).items():
                self.markets[k] = MarketState.from_dict(v)
            logger.info(f"Loaded state: {len(self.markets)} markets")
            return True
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return False

    def print_status(self):
        """Print current portfolio status."""
        total_value = self.get_total_value()
        total_pnl = self.get_total_pnl()
        ret = (total_value - self.initial_capital) / self.initial_capital if self.initial_capital > 0 else 0

        print(f"\n{'='*60}")
        print(f"  PORTFOLIO STATUS  |  {datetime.utcnow().strftime('%H:%M:%S')} UTC")
        print(f"{'='*60}")
        print(f"  Capital: ${self.initial_capital:.2f} -> ${total_value:.2f} ({ret*100:+.1f}%)")
        print(f"  P&L:     ${total_pnl:+.2f}")
        print(f"  Trades:  {len(self.trades)}")
        if self.global_tp_hit:
            print(f"  ** GLOBAL TP HIT at {self.global_tp_ts} **")
        print(f"{'='*60}")
        for mid, ms in self.markets.items():
            open_pos = sum(1 for p in ms.positions.values() if p.shares > 0)
            status = ""
            if ms.early_entry_triggered:
                status += " [EARLY]"
            if ms.hedge_count > 0:
                status += f" [HEDGE x{ms.hedge_count}]"
            if ms.promoted:
                status += " [PROMOTED]"
            if ms.panic_flip_triggered:
                status += " [PANIC]"
            print(f"  {ms.name or mid[:25]:25s} | pnl=${ms.pnl:+.2f} | pos={open_pos}{status}")
        print(f"{'='*60}\n")
