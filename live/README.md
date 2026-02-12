# DCA Polymarket Live Trading System

Live trading module for the DCA strategy (v19), supporting three modes:

| Mode | Data Source | Execution | Use Case |
|------|------------|-----------|----------|
| **paper** | Live WebSocket | Simulated fills | Test without risking money |
| **live** | Live WebSocket | Real orders via CLOB API | Production trading |
| **parquet** | Historical parquet file | Simulated fills | Debug / validate strategy |

## Quick Start

### 1. Configure

```bash
cp live/.env.example .env
cp live/markets.example.json live/markets.json
# Edit .env with your settings
# Edit markets.json with your target markets
```

### 2. Paper Trading (default)

```bash
python -m live.main
```

### 3. Live Trading

```bash
# Set credentials in .env first
TRADING_MODE=live python -m live.main
```

### 4. Parquet Replay

```bash
python -m live.backtest_main --parquet snapshots_27jan_reconstructed.parquet
python -m live.backtest_main --parquet data.parquet --resample 2s --capital 500
```

## Markets Configuration

Create `live/markets.json` (see `markets.example.json` for format):

```json
{
    "markets": [
        {
            "condition_id": "0xabc...",
            "asset_ids": ["token_yes_id", "token_no_id"],
            "name": "nba-lal-nyk",
            "start_time": "2026-02-10T00:00:00Z",
            "end_time": "2026-02-10T03:00:00Z",
            "capital": 100.0
        }
    ]
}
```

Get `condition_id` and `asset_ids` from the Polymarket API or use `extract_clob_token_ids.py`.

## Architecture

```
live/
├── config.py           # All config via env vars / .env
├── orderbook_feed.py   # WebSocket client for live orderbook data
├── order_manager.py    # Paper (simulated) + Real (CLOB API) execution
├── position_tracker.py # Position, market state, trade persistence
├── data_collector.py   # Parquet chunk writer (same format as collector_parquet/)
├── strategy.py         # DCA v19 strategy engine (tick-by-tick)
├── main.py             # Entry point for paper/live modes
└── backtest_main.py    # Entry point for parquet replay mode
```

## Data Collection

The live system saves orderbook data in the exact same format as `collector_parquet/`:

- Chunks saved to `data/chunks/chunk_XXXXXX.parquet`
- Merged to `data/snapshots.parquet` on shutdown
- Same schema: `timestamp, datetime, asset_id, market_id, mid_price, best_bid, best_ask, spread, orderbook_bids, orderbook_asks`

## State Persistence

- **state.json** — market positions, hedge tracking, cash balances (auto-saved every 100 ticks)
- **trades.jsonl** — append-only trade log with full details
- On crash/restart, the system resumes from the last saved state

## Strategy Features (v19)

- Multi-tier DCA entries (entry + 3 tiers with configurable weights)
- Stop-loss / take-profit with tick confirmation
- Dynamic hedging with fast/slow drop detection
- Hedge exit on confirmed drop
- Hedge promotion (hedge wins → becomes main position)
- Panic flip (detects rapid resolution, flips to winner)
- Early entry for stable-trending markets
- Late-game stop-loss disable
- Grace period after entry
- Instant exit at 0.99
- Global take-profit across all markets
- Noise filtering (wide spreads ignored for SL)
