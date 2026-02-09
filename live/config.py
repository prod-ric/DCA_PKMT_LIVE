"""
Configuration for DCA Polymarket Live Trading System.

Supports both paper and real trading modes.
Set TRADING_MODE=paper for paper trading, TRADING_MODE=real for live.
"""

import os
import json
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# TRADING MODE
# ============================================================
# "paper" = simulated fills against live orderbook
# "real"  = actual orders via Polymarket CLOB API
TRADING_MODE = os.getenv("TRADING_MODE", "paper")

# ============================================================
# POLYMARKET API (required for real mode)
# ============================================================
POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY", "")
POLYMARKET_API_SECRET = os.getenv("POLYMARKET_API_SECRET", "")
POLYMARKET_API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "")
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")

# CLOB endpoints
POLYMARKET_CLOB_URL = os.getenv("POLYMARKET_CLOB_URL", "https://clob.polymarket.com")
POLYMARKET_WSS_URL = os.getenv("POLYMARKET_WSS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")

# Chain ID (137 = Polygon mainnet)
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))

# ============================================================
# STRATEGY PARAMETERS (from Optuna-optimized backtest v19)
# ============================================================
DEFAULT_STRATEGY_PARAMS = {
    # DCA entry thresholds
    "entry_threshold": 0.7412,
    "dca_tier_1": 0.7916,
    "dca_tier_2": 0.8382,
    "dca_tier_3": 0.8501,

    # Position sizing weights (normalized to sum=1)
    "weight_entry": 0.1064,
    "weight_tier_1": 0.3337,
    "weight_tier_2": 0.2735,
    "weight_tier_3": 0.2382,

    # Exit parameters
    "exit_stop_loss": 0.5440,
    "stop_loss_pct": 0.1354,
    "take_profit_pct": 0.3406,
    "cooldown_periods": 21,
}

# ============================================================
# CAPITAL & RISK
# ============================================================
INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", "500"))
GLOBAL_TP_PCT = float(os.getenv("GLOBAL_TP_PCT", "0.20"))
CAPITAL_PER_MARKET = float(os.getenv("CAPITAL_PER_MARKET", "0"))  # 0 = auto-divide

# ============================================================
# TIMING & WINDOWS
# ============================================================
LAST_MINUTES_ONLY = int(os.getenv("LAST_MINUTES_ONLY", "10"))        # Trading window (last N minutes)
LATE_GAME_THRESHOLD = float(os.getenv("LATE_GAME_THRESHOLD", "0.90"))

# ============================================================
# EARLY ENTRY
# ============================================================
EARLY_ENTRY_ENABLED = os.getenv("EARLY_ENTRY_ENABLED", "true").lower() == "true"
EARLY_ENTRY_MIN_PROGRESS = float(os.getenv("EARLY_ENTRY_MIN_PROGRESS", "0.40"))
EARLY_ENTRY_MAX_PROGRESS = float(os.getenv("EARLY_ENTRY_MAX_PROGRESS", "0.90"))
EARLY_ENTRY_PRICE_THRESHOLD = float(os.getenv("EARLY_ENTRY_PRICE_THRESHOLD", "0.70"))
EARLY_ENTRY_MAX_VOLATILITY = float(os.getenv("EARLY_ENTRY_MAX_VOLATILITY", "0.025"))
EARLY_ENTRY_MAX_RANGE = float(os.getenv("EARLY_ENTRY_MAX_RANGE", "0.07"))
EARLY_ENTRY_MIN_DURATION_MINUTES = float(os.getenv("EARLY_ENTRY_MIN_DURATION_MINUTES", "25"))
EARLY_ENTRY_NO_DROP_THRESHOLD = float(os.getenv("EARLY_ENTRY_NO_DROP_THRESHOLD", "0.05"))
EARLY_ENTRY_NO_DROP_WINDOW = int(os.getenv("EARLY_ENTRY_NO_DROP_WINDOW", "900"))

# ============================================================
# HEDGE PARAMETERS
# ============================================================
HEDGE_ENABLED = os.getenv("HEDGE_ENABLED", "true").lower() == "true"
HEDGE_DROP_POINTS = float(os.getenv("HEDGE_DROP_POINTS", "0.12"))
HEDGE_DROP_WINDOW = int(os.getenv("HEDGE_DROP_WINDOW", "90"))
HEDGE_DROP_FROM_ENTRY = float(os.getenv("HEDGE_DROP_FROM_ENTRY", "0.10"))
HEDGE_AMOUNT = float(os.getenv("HEDGE_AMOUNT", "30.0"))
HEDGE_LATE_GAME_ONLY = os.getenv("HEDGE_LATE_GAME_ONLY", "true").lower() == "true"
HEDGE_COOLDOWN_SECONDS = int(os.getenv("HEDGE_COOLDOWN_SECONDS", "30"))
HEDGE_DROP_CONFIRMATION_TICKS = int(os.getenv("HEDGE_DROP_CONFIRMATION_TICKS", "20"))
HEDGE_EXIT_CONFIRMATION_TICKS = int(os.getenv("HEDGE_EXIT_CONFIRMATION_TICKS", "15"))
HEDGE_EXIT_DROP_POINTS = float(os.getenv("HEDGE_EXIT_DROP_POINTS", "0.08"))

# ============================================================
# PANIC FLIP
# ============================================================
PANIC_FLIP_ENABLED = os.getenv("PANIC_FLIP_ENABLED", "true").lower() == "true"
PANIC_FLIP_THRESHOLD = float(os.getenv("PANIC_FLIP_THRESHOLD", "0.25"))
PANIC_FLIP_WINDOW = int(os.getenv("PANIC_FLIP_WINDOW", "10"))
PANIC_FLIP_MIN_PRICE = float(os.getenv("PANIC_FLIP_MIN_PRICE", "0.85"))
PANIC_FLIP_LATE_GAME_ONLY = os.getenv("PANIC_FLIP_LATE_GAME_ONLY", "true").lower() == "true"

# ============================================================
# HEDGE PROMOTION
# ============================================================
HEDGE_PROMOTION_PNL_PCT = float(os.getenv("HEDGE_PROMOTION_PNL_PCT", "0.20"))
HEDGE_PROMOTION_PRICE = float(os.getenv("HEDGE_PROMOTION_PRICE", "0.50"))
HEDGE_PROMOTION_DELTA = float(os.getenv("HEDGE_PROMOTION_DELTA", "0.15"))

# ============================================================
# SAFETY
# ============================================================
ENTRY_GRACE_PERIOD = int(os.getenv("ENTRY_GRACE_PERIOD", "180"))
INSTANT_EXIT_PRICE = float(os.getenv("INSTANT_EXIT_PRICE", "0.99"))
MAX_SPREAD_FOR_SL = float(os.getenv("MAX_SPREAD_FOR_SL", "0.10"))
SL_CONFIRMATION_TICKS = int(os.getenv("SL_CONFIRMATION_TICKS", "10"))

# ============================================================
# PERSISTENCE & LOGGING
# ============================================================
STATE_FILE = os.getenv("STATE_FILE", "live/state.json")
TRADE_LOG_FILE = os.getenv("TRADE_LOG_FILE", "live/trades.jsonl")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ============================================================
# MARKET SCHEDULE  (market_id -> expected end time ISO)
# Set by the user before running, or loaded from a JSON file
# ============================================================
MARKETS_FILE = os.getenv("MARKETS_FILE", "live/markets.json")


def load_markets() -> Dict:
    """
    Load markets config from JSON file.
    Expected format:
    {
        "markets": [
            {
                "condition_id": "0xabc...",
                "asset_ids": ["12345...", "67890..."],
                "name": "nba-lal-nyk",
                "end_time": "2026-02-10T03:00:00Z",
                "capital": 100.0  // optional override
            }
        ]
    }
    """
    path = MARKETS_FILE
    if not os.path.exists(path):
        return {"markets": []}
    with open(path, "r") as f:
        return json.load(f)


def normalize_strategy_params(params: Optional[Dict] = None) -> Dict:
    """Normalize strategy parameters with defaults and weight normalization."""
    p = DEFAULT_STRATEGY_PARAMS.copy()
    if params:
        p.update(params)

    # Normalize weights to sum to 1
    w_keys = ["weight_entry", "weight_tier_1", "weight_tier_2", "weight_tier_3"]
    total = sum(p[k] for k in w_keys)
    if total > 0:
        for k in w_keys:
            p[k] /= total
    return p


def validate_config():
    """Validate configuration before starting."""
    if TRADING_MODE == "real":
        if not POLYMARKET_PRIVATE_KEY:
            raise ValueError(
                "POLYMARKET_PRIVATE_KEY required for real trading mode. "
                "Set in .env or environment."
            )
    
    if INITIAL_CAPITAL <= 0:
        raise ValueError("INITIAL_CAPITAL must be positive")
    
    print(f"{'='*60}")
    print(f"  DCA POLYMARKET LIVE TRADER")
    print(f"{'='*60}")
    print(f"  Mode:           {TRADING_MODE.upper()}")
    print(f"  Capital:        ${INITIAL_CAPITAL:.2f}")
    print(f"  Global TP:      {GLOBAL_TP_PCT*100:.1f}%")
    print(f"  Last mins only: {LAST_MINUTES_ONLY}")
    print(f"  Hedge:          {'ON' if HEDGE_ENABLED else 'OFF'}")
    print(f"  Panic flip:     {'ON' if PANIC_FLIP_ENABLED else 'OFF'}")
    print(f"  Early entry:    {'ON' if EARLY_ENTRY_ENABLED else 'OFF'}")
    print(f"{'='*60}")
