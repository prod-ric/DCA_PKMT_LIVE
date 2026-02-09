# DCA Polymarket Live Trader

Live trading system for the DCA strategy on Polymarket prediction markets.  
Supports **paper** (simulated) and **real** (actual money) trading modes.

**SPEED-OPTIMIZED**: Trading and data collection run as separate processes.

## Architecture

```
live/
├── main.py              # Trading entry point (fast, no I/O blocking)
├── collector.py         # Data collection entry point (separate process)
├── config.py            # All configuration (env vars / .env)
├── strategy.py          # DCA strategy engine (v19) – tick-by-tick logic
├── orderbook_feed.py    # WebSocket connection to Polymarket
├── order_manager.py     # Paper & real order execution
├── position_tracker.py  # Positions, PnL, state persistence
├── data_collector.py    # Parquet chunk writer for orderbook data
├── markets.example.json # Example market definitions
├── .env.example         # Example environment config
└── README.md            # This file
```

## Quick Start

### 1. Install dependencies

```bash
pip install websockets numpy python-dotenv pyarrow
# For real trading also install:
pip install py-clob-client
```

### 2. Configure markets

Copy and edit the example:

```bash
cp live/markets.example.json live/markets.json
```

Edit `live/markets.json` with today's markets:

```json
{
    "markets": [
        {
            "condition_id": "0xabc123...",
            "asset_ids": ["12345...", "67890..."],
            "name": "nba-lal-nyk",
            "start_time": "2026-02-10T00:00:00Z",
            "end_time": "2026-02-10T03:00:00Z",
            "capital": 100.0
        }
    ]
}
```

Fields:
- **condition_id**: The Polymarket market condition ID (hex)
- **asset_ids**: Token IDs for each side of the market (Yes/No or Team A/Team B)
- **name**: Human-readable label
- **start_time / end_time**: Expected game time (UTC ISO format). Used to calculate market progress for early entry, late-game, and trading window.
- **capital** (optional): Override per-market allocation

### 3. Configure environment

```bash
cp live/.env.example live/.env
# Edit live/.env with your settings
```

Or set environment variables directly:

```bash
export TRADING_MODE=paper
export INITIAL_CAPITAL=500
```

### 4. Run

**Recommended: Trading + Data Collection in separate processes (fastest)**

```bash
cd /home/ripa/DCA_PKMT_LIVE

# Terminal 1 - Trading (fast, no I/O blocking):
python -m live.main

# Terminal 2 - Data collection (saves orderbook to parquet):
python -m live.collector
```

**Alternative: Single process with collection (convenience, slightly slower)**

```bash
ENABLE_COLLECTION=true python -m live.main
```

**Real trading mode:**

```bash
TRADING_MODE=real python -m live.main
```

## Process Separation

For optimal trading speed, the system is designed to run as two separate processes:

| Process | Purpose | Command |
|---------|---------|---------|
| **Trader** | Strategy decisions, order execution | `python -m live.main` |
| **Collector** | Orderbook data → parquet chunks | `python -m live.collector` |

Both processes connect to the same Polymarket WebSocket independently.
The trader process has **zero I/O blocking** from data collection.

### Collector Output

Data is saved to `live/data/`:
- `chunks/chunk_000001.parquet` - incremental chunks (flushed every 60s)
- `snapshots.parquet` - merged file (created on shutdown)

Compatible with the backtest notebook for analysis.

### Collector Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `COLLECTOR_DATA_DIR` | `live/data` | Output directory |
| `COLLECTOR_FLUSH_INTERVAL` | `60` | Seconds between flushes |
| `COLLECTOR_BUFFER_SIZE` | `1000` | Records before forced flush |

## Modes

### Paper Mode (default)
- Connects to live Polymarket orderbook WebSocket
- Simulates fills against the real orderbook (walking the book)
- Tracks positions, PnL, and trade history
- No real money at risk
- Perfect for validating the strategy in real time

### Real Mode
- Places actual limit orders via the Polymarket CLOB API
- Uses `py-clob-client` for authenticated order signing
- Orders are placed as marketable limits at best bid/ask
- **Requires** `POLYMARKET_PRIVATE_KEY` in `.env`

## Strategy (v19)

The strategy is a direct port of the backtest engine from the notebook:

1. **DCA Entry**: Multi-tier entries (entry → tier_1 → tier_2 → tier_3) with weighted position sizing
2. **Trading Window**: Only enters in the last N minutes (configurable) unless early entry triggers
3. **Early Entry**: If a market is stable and trending (low volatility, high price), enters before the normal window
4. **Stop Loss**: Absolute + percentage stop loss with confirmation ticks (disabled in late game)
5. **Take Profit**: Percentage-based take profit
6. **Hedge**: If main position drops (confirmed), buys opposite side. If hedge drops, sells it.
7. **Hedge Promotion**: If hedge outperforms main significantly, closes main and promotes hedge
8. **Panic Flip**: If opponent surges (detected across all assets), closes losers and buys winner
9. **Global TP**: If total portfolio PnL exceeds threshold, closes everything
10. **Instant Exit**: Sells at 0.99 price

## State & Recovery

- State is saved to `live/state.json` every ~100 ticks and on shutdown
- Trades are logged to `live/trades.jsonl` (append-only)
- On restart, the system attempts to resume from saved state
- Press `Ctrl+C` to gracefully shut down

## Configuration Reference

All settings can be overridden via environment variables. See `live/config.py` for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `TRADING_MODE` | `paper` | `paper` or `real` |
| `INITIAL_CAPITAL` | `500` | Starting capital |
| `GLOBAL_TP_PCT` | `0.20` | Global take profit % |
| `LAST_MINUTES_ONLY` | `10` | Trading window (last N minutes) |
| `HEDGE_ENABLED` | `true` | Enable hedging |
| `HEDGE_AMOUNT` | `30.0` | USD amount per hedge |
| `EARLY_ENTRY_ENABLED` | `true` | Enable early entry |
| `PANIC_FLIP_ENABLED` | `true` | Enable panic flip |
| `MARKETS_FILE` | `live/markets.json` | Path to market definitions |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `ENABLE_COLLECTION` | `false` | Collect data in same process |

## Finding Market IDs

You can find condition IDs and token IDs from:
1. The Polymarket UI (inspect network requests)
2. The `clob_token_ids_by_file.json` in this repo
3. The `extract_clob_token_ids.py` helper script
4. The Polymarket CLOB API: `GET https://clob.polymarket.com/markets`
