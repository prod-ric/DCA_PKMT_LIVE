# Polymarket DCA Trading System

A complete system for **collecting real-time orderbook data** from [Polymarket](https://polymarket.com/) prediction markets, **backtesting** a multi-tier DCA strategy with realistic orderbook execution, and **live/paper trading** via the Polymarket CLOB API.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.0+-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Unified Parquet Schema](#unified-parquet-schema)
- [Data Collection (`collector_parquet/`)](#data-collection-collector_parquet)
  - [Market Discovery](#market-discovery)
  - [Real-Time Collection](#real-time-collection)
  - [Chunk Merging](#chunk-merging)
- [Live Trading (`live/`)](#live-trading-live)
  - [Modes](#modes)
  - [Architecture](#architecture)
  - [Configuration](#configuration)
  - [Running](#running)
  - [Position Dashboard](#position-dashboard)
- [Backtest Notebook](#backtest-notebook)
  - [Strategy Overview](#strategy-overview)
  - [The Eight Pillars](#the-eight-pillars)
  - [Running a Backtest](#running-a-backtest)
- [Setup & Installation](#setup--installation)
- [Technologies](#technologies)

---

## Overview

Prediction markets have binary outcomes (pays $1.00 or $0.00), defined endpoints, and price movements driven by information flow. This creates unique challenges for automated trading:

- **Binary risk** — being on the wrong side means total loss on that position
- **Late-game volatility** — markets become chaotic in the final minutes before resolution
- **Thin liquidity** — orderbooks can be sparse, making execution quality critical
- **Information cascades** — prices can move violently as new information emerges

This project addresses them with three integrated components:

1. **`collector_parquet/`** — a standalone data collector that records full orderbook data (with on-the-fly reconstruction) to Parquet files for later backtesting
2. **`live/`** — a live/paper trading engine that runs the DCA v19 strategy in real-time against the Polymarket CLOB, with an optional parquet recording mode
3. **`dca_backtest_v11_organized.ipynb`** — a Jupyter notebook that backtests the strategy against collected data with realistic orderbook execution, visualization, and Optuna optimization

Both the collector and the live system reconstruct orderbooks on-the-fly from WebSocket `book` + `price_change` events, producing the same unified Parquet schema. There is no separate reconstruction step.

---

## Project Structure

```
├── dca_backtest_v11_organized.ipynb   # Backtest notebook (DCA v19)
│
├── collector_parquet/                 # Standalone data collector
│   ├── main.py                       # Entry point
│   ├── websocket_client.py           # WebSocket subscription & message routing
│   ├── database.py                   # On-the-fly orderbook reconstruction + Parquet storage
│   ├── config.py                     # Configuration (asset IDs, URLs, intervals)
│   ├── merger.py                     # Merges chunk files into a single sorted parquet
│   ├── reconstruct_orderbooks.py     # Legacy script (for old-format files only)
│   ├── init_database.py              # Database directory init utility
│   └── requirements.txt              # Dependencies
│
├── live/                             # Live / paper trading engine
│   ├── main.py                       # Entry point — paper / live / parquet modes
│   ├── orderbook_feed.py             # WebSocket feed with _BookState reconstruction
│   ├── strategy.py                   # DCA v19 strategy logic + position dashboard
│   ├── order_manager.py              # Order execution (live CLOB, paper sim, or noop)
│   ├── position_tracker.py           # Position & PnL tracking
│   ├── data_collector.py             # Parquet recording (parquet mode)
│   ├── config.py                     # Configuration loader
│   ├── backtest_main.py              # Replay parquet files through the strategy
│   ├── markets.json                  # Market definitions (condition IDs + asset IDs)
│   └── .env.example                  # Environment variable template
│
├── extract_clob_token_ids.py         # Extracts market/token IDs from API responses
├── json_joiner.py                    # Combines market JSON files
├── *_markets/                        # Raw Polymarket API responses (market metadata)
└── data_markets/                     # Current day's market JSON files
```

---

## Unified Parquet Schema

Both `collector_parquet/` and `live/` produce parquet files with the same schema. The backtest notebook accepts either format transparently.

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | int64 | Unix epoch in milliseconds |
| `datetime` | timestamp[ms] | Human-readable timestamp |
| `asset_id` | string | CLOB token ID (YES or NO side) |
| `market_id` | string | Market condition ID |
| `mid_price` | float64 | (best_bid + best_ask) / 2 |
| `best_bid` | float64 | Best (highest) bid — from reconstructed book |
| `best_ask` | float64 | Best (lowest) ask — from reconstructed book |
| `spread` | float64 | best_ask − best_bid |
| `orderbook_bids` | string (JSON) | Full bid side: `[{"price": 0.95, "size": 150}, ...]` |
| `orderbook_asks` | string (JSON) | Full ask side: `[{"price": 0.96, "size": 200}, ...]` |
| `event_type` | string | `"book"`, `"price_change"`, or `"synthetic"` |
| `raw_json` | string | Original WebSocket message (for debugging/replay) |

> **Note on legacy files:** Older parquet files produced by the previous pipeline used `ob_best_bid` / `ob_best_ask` / `ob_mid_price` instead of `best_bid` / `best_ask` / `mid_price`, and did not have `event_type` or `raw_json`. The notebook's `load_data()` function normalizes both formats automatically.

### How Orderbooks Are Reconstructed

Both modules use a `_BookState` class that maintains `{price: size}` dicts for each asset's bids and asks:

- **`book` events** → full reset: replace entire bid/ask state from the snapshot
- **`price_change` events** → incremental delta: set `bids[price] = size` or `asks[price] = size`; if `size == 0`, remove that level

This happens *during collection*, so every recorded row already contains the full reconstructed orderbook. No post-processing step is needed.

---

## Data Collection (`collector_parquet/`)

The standalone collector records orderbook data to Parquet files for later backtesting.

### Market Discovery

**`extract_clob_token_ids.py`** processes Polymarket API responses (in `*_markets/` directories) to extract condition IDs and CLOB token IDs:

```bash
python extract_clob_token_ids.py
```

### Real-Time Collection

```
Polymarket WSS API
        │
        ▼
websocket_client.py    ─── Subscribes to book + price_change channels
        │                   Handles list-wrapped messages
        ▼
database.py            ─── _BookState reconstructs orderbooks on-the-fly
        │                   Buffers rows → writes Parquet chunks every 60s
        ▼
data/chunks/           ─── chunk_000000.parquet, chunk_000001.parquet, ...
```

1. `websocket_client.py` connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market` and subscribes to specified assets
2. `database.py` maintains a `_BookState` per `(market_id, asset_id)`, applies each event to reconstruct the full book, and buffers the resulting rows
3. Every 60 seconds (or 100 rows), the buffer is flushed as a Parquet chunk
4. On shutdown (Ctrl+C), remaining data is flushed and chunks are auto-merged

```bash
cd collector_parquet
export POLYMARKET_ASSET_IDS="token_id_1,token_id_2,..."
python main.py
# Ctrl+C to stop — data is flushed and merged automatically
```

### Chunk Merging

Chunks provide crash resilience — if the process dies, only the in-memory buffer (≤60s) is lost.

```bash
cd collector_parquet
python merger.py merge --data-dir data       # merge all chunks
python merger.py list --data-dir data        # list chunk info
python merger.py info data/snapshots.parquet  # inspect a file
```

The merger uses `promote_options="default"` to handle schema differences between old-format and new-format chunks seamlessly.

---

## Live Trading (`live/`)

The live module runs the DCA v19 strategy in real-time against Polymarket.

### Modes

| Mode | `--mode` | What it does |
|------|----------|--------------|
| **Paper** | `paper` | Simulates orders locally — no real money. Default. |
| **Live** | `live` | Places real orders via the Polymarket CLOB API. |
| **Parquet** | `parquet` | Records orderbook data to Parquet only (no trading). |

### Architecture

```
orderbook_feed.py      ─── WebSocket + _BookState → OrderbookTick stream
        │
        ├──▶ strategy.py          ─── DCA v19 logic (entry, hedge, panic flip, etc.)
        │       │
        │       └──▶ order_manager.py    ─── Execute trades (paper/live)
        │       └──▶ position_tracker.py ─── Track shares, avg cost, PnL
        │
        └──▶ data_collector.py    ─── Record ticks to Parquet (parquet mode)
```

### Configuration

1. Copy `.env.example` → `.env` and fill in your API keys (live mode only)
2. Edit `markets.json` with the markets you want to trade:

```json
[
  {
    "condition_id": "0xabc...",
    "asset_ids": ["token_yes", "token_no"],
    "name": "NBA - LAL vs NYK"
  }
]
```

### Running

```bash
cd live

# Paper trading (default)
python main.py --mode paper

# Live trading
python main.py --mode live

# Data collection only
python main.py --mode parquet
```

### Position Dashboard

The strategy prints a position dashboard every 60 seconds and after every trade event (BUY, SELL, HEDGE, PANIC_FLIP, PROMOTION, GLOBAL_TP):

```
╔══════════════════════════════════════════════════════════╗
║                  POSITION DASHBOARD                     ║
║  Trigger: BUY  |  2025-01-25 14:32:01                  ║
╠══════════════════════════════════════════════════════════╣
║  Capital: $850.00 / $1000.00                            ║
║  Portfolio Value: $1,023.45                              ║
║  Total PnL: +$23.45 (+2.35%)                            ║
║  Realized: +$5.20  |  Unrealized: +$18.25               ║
╠══════════════════════════════════════════════════════════╣
║  NBA-LAL-NYK                                            ║
║    YES: 50 shares @ 0.9200 → 0.9500  $47.50  +$1.50    ║
║    NO:  — no position —                                 ║
╚══════════════════════════════════════════════════════════╝
```

---

## Backtest Notebook

### Strategy Overview

The strategy is a **multi-tier DCA (Dollar-Cost Averaging)** approach for binary prediction markets:

> **Enter gradually as confidence increases, but protect aggressively when things go wrong.**

Capital is deployed across 4 tiers:

| Tier | Default Threshold | Capital Weight | Purpose |
|------|------------------|----------------|---------|
| Entry | 0.96 | 49% | Initial position when price shows confidence |
| DCA 1 | 0.96+ | 28% | Add as price climbs |
| DCA 2 | 0.97+ | 10% | Increase as conviction grows |
| DCA 3 | 0.99+ | 6% | Final add at high confidence |

### The Eight Pillars

1. **Multi-Tier DCA Entries** — gradual capital deployment across configurable thresholds
2. **Dynamic Hedging** — buy the opposite outcome when the main position drops (confirmed over N ticks to filter noise); sell hedge if it drops too; can re-hedge later
3. **Panic Flip Detection** — when an asset surges 25+ points in 10 seconds above 0.85, immediately flip to it
4. **Hedge Promotion** — if hedge outperforms main (20%+ PnL, price ≥0.50, or return delta ≥15%), promote it to main
5. **Early Entry Detection** — enter markets trading stably for 25+ min with low volatility, before the standard window
6. **Late-Game Stop-Loss Disable** — in the final 10% of market life, stop-losses are disabled to avoid pre-resolution shakeouts
7. **Noise Filtering** — ticks with spread >10% are ignored for exit/hedge decisions; N consecutive confirming ticks required
8. **Portfolio Management** — equal capital per market; global take-profit (default 20%) closes all positions

### Running a Backtest

1. **Open** `dca_backtest_v11_organized.ipynb`
2. **Set `DATA_PATHS`** (Section 2) — point to any parquet files (new or legacy format)
3. **Configure parameters** (Section 9) — or use the defaults
4. **Run All** — the notebook loads data (auto-normalizing columns), simulates the strategy, and prints results
5. **Visualize** — price charts with trade markers
6. **Optimize** (optional) — run Optuna across multiple days to find robust parameters

The notebook's `load_data()` auto-detects and handles both the new unified schema (`best_bid`, `best_ask`) and the legacy reconstructed format (`ob_best_bid`, `ob_best_ask`).

---

## Setup & Installation

### Requirements

```bash
# Core (notebook + backtest)
pip install pandas numpy pyarrow matplotlib optuna

# Collector & live trading
pip install websockets python-dotenv aiohttp
```

### Quick Start — Backtest Only

```bash
pip install pandas numpy pyarrow matplotlib optuna
jupyter notebook dca_backtest_v11_organized.ipynb
# Update DATA_PATHS in Section 2, then Run All
```

### Full Pipeline — Collect → Backtest

```bash
# 1. Install dependencies
pip install -r collector_parquet/requirements.txt
pip install matplotlib optuna

# 2. Extract market/token IDs
python extract_clob_token_ids.py

# 3. Collect data (orderbooks reconstructed on-the-fly)
cd collector_parquet
export POLYMARKET_ASSET_IDS="id1,id2,..."
python main.py          # Ctrl+C to stop

# 4. Merge chunks (if not auto-merged)
python merger.py merge --data-dir data

# 5. Backtest — output is ready to use directly, no reconstruction needed
cd ..
jupyter notebook dca_backtest_v11_organized.ipynb
```

### Live/Paper Trading

```bash
cd live
cp .env.example .env   # fill in API keys for live mode
# edit markets.json with your target markets

python main.py --mode paper    # or --mode live / --mode parquet
```

---

## Technologies

| Tool | Purpose |
|------|---------|
| **Python 3.10+** | Core language |
| **Pandas / NumPy** | Data manipulation and numerical computing |
| **PyArrow** | Parquet I/O with Snappy compression |
| **websockets** | Async WebSocket client for real-time data |
| **Matplotlib** | Price charts and trade visualizations |
| **Optuna** | Bayesian hyperparameter optimization |
| **Jupyter** | Interactive backtest development |

---

## License

MIT — see [LICENSE](LICENSE) for details.
