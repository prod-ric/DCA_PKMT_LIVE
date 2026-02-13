# Polymarket DCA Backtesting Framework

A complete system for **collecting real-time orderbook data** from [Polymarket](https://polymarket.com/) prediction markets and **backtesting a multi-tier Dollar-Cost Averaging (DCA) strategy** with realistic execution against reconstructed orderbooks.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.0+-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
  - [1. Market Discovery](#1-market-discovery)
  - [2. Real-Time Collection](#2-real-time-collection-collector_parquet)
  - [3. Chunk Merging](#3-chunk-merging-mergerpy)
  - [4. Orderbook Reconstruction](#4-orderbook-reconstruction-reconstruct_orderbookspy)
- [Backtest Notebook](#backtest-notebook)
  - [Strategy Overview](#strategy-overview)
  - [The Eight Pillars](#the-eight-pillars-of-the-strategy)
  - [How to Run](#how-to-run-a-backtest)
- [Results](#results)
- [Setup & Installation](#setup--installation)
- [Technologies](#technologies)

---

## Overview

Prediction markets have binary outcomes (pays $1.00 or $0.00), defined endpoints, and price movements driven by information flow. This creates unique challenges for automated trading:

- **Binary risk** — being on the wrong side means total loss on that position
- **Late-game volatility** — markets become chaotic in the final minutes before resolution
- **Thin liquidity** — orderbooks can be sparse, making execution quality critical
- **Information cascades** — prices can move violently as new information emerges

This project addresses these challenges with:

1. **A real-time data collector** that captures full orderbook snapshots via Polymarket's WebSocket API
2. **An orderbook reconstruction pipeline** that replays book snapshots and price_change deltas to rebuild the complete orderbook at every tick
3. **A backtesting engine** that simulates trades against the actual orderbook — not just mid-prices — accounting for spreads, partial fills, and liquidity depth
4. **Optuna-based optimization** that finds robust strategy parameters across multiple days of data

---

## Project Structure

```
├── dca_backtest_v11_organized.ipynb  # Main backtest notebook
├── collector_parquet/                # Real-time data collection system
│   ├── main.py                      # Entry point — runs the WebSocket collector
│   ├── websocket_client.py          # Polymarket WebSocket subscription & message handling
│   ├── database.py                  # Parquet storage with buffered writes & chunk management
│   ├── config.py                    # Configuration (asset IDs, WebSocket URL, etc.)
│   ├── merger.py                    # Merges chunk files into a single parquet
│   ├── reconstruct_orderbooks.py    # Rebuilds full orderbooks from snapshots + deltas
│   ├── init_database.py             # Database initialization utility
│   └── requirements.txt             # Python dependencies for the collector
├── extract_clob_token_ids.py        # Extracts market/token IDs from Polymarket API responses
├── json_joiner.py                   # Utility to combine market JSON files
├── medium_article.md                # Detailed strategy write-up
├── *_markets/                        # Raw Polymarket API responses per day (market metadata)
└── data_markets/                    # Current day's market JSON files
```

---

## Data Pipeline

The data pipeline transforms live Polymarket WebSocket messages into analysis-ready parquet files with fully reconstructed orderbooks. Here is each stage in detail:

### 1. Market Discovery

Before collecting data, we need to identify which markets to track.

**`extract_clob_token_ids.py`** processes Polymarket API response JSON files (stored in the `*_markets/` directories) and extracts:
- **Condition IDs** — unique market identifiers on Polymarket's CLOB
- **CLOB Token IDs** — the YES/NO asset token IDs needed for WebSocket subscriptions
- **Market metadata** — title, start/end dates, question text

```bash
# Extract token IDs from today's market files
python extract_clob_token_ids.py
```

The extracted IDs are used to configure the collector's subscriptions.

### 2. Real-Time Collection (`collector_parquet/`)

The collector is an async WebSocket client that connects to Polymarket's orderbook feed and stores every message.

#### Architecture

```
Polymarket WSS API
        │
        ▼
websocket_client.py    ─── Subscribes to book + price_change channels
        │
        ▼
database.py            ─── Buffers messages → writes Parquet chunks every 60s
        │
        ▼
data/chunks/           ─── chunk_000000.parquet, chunk_000001.parquet, ...
```

#### How it works

1. **`websocket_client.py`** connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market` and subscribes to specified asset IDs or market condition IDs
2. Two types of messages are received:
   - **`book`** — a complete orderbook snapshot (all bid and ask levels)
   - **`price_change`** — incremental updates (a single level changed)
3. **`database.py`** buffers incoming messages in memory and flushes them to a new Parquet chunk file every 60 seconds (or when the buffer reaches 100 records)
4. On shutdown (Ctrl+C), the system flushes remaining data, merges all chunks, and produces a single `snapshots.parquet`

#### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | int64 | Unix milliseconds |
| `datetime` | timestamp[ms] | Human-readable timestamp |
| `asset_id` | string | CLOB token ID |
| `market_id` | string | Market condition ID |
| `mid_price` | float64 | (best_bid + best_ask) / 2 |
| `best_bid` | float64 | Highest bid price |
| `best_ask` | float64 | Lowest ask price |
| `spread` | float64 | best_ask - best_bid |
| `raw_json` | string | Full JSON message (for orderbook reconstruction) |

#### Running the collector

```bash
cd collector_parquet

# Set environment variables (or use .env file)
export POLYMARKET_ASSET_IDS="token_id_1,token_id_2,..."
export DATA_DIR="data"

# Start collecting
python main.py

# Ctrl+C to stop — data is automatically flushed and merged
```

### 3. Chunk Merging (`merger.py`)

During collection, data is written as many small chunk files for reliability (no data loss on crash). The merger combines them into a single sorted parquet.

#### What it does

1. Scans `data/chunks/` for all `chunk_*.parquet` files
2. Reads each chunk into memory as a PyArrow table
3. If an existing `snapshots.parquet` already exists, includes it in the merge
4. Concatenates all tables and **sorts by timestamp**
5. Writes the merged result with Snappy compression
6. Optionally deletes the chunk files after successful merge

```bash
cd collector_parquet

# Merge all chunks
python merger.py merge --data-dir data

# List chunk info
python merger.py list --data-dir data

# Show info about a parquet file
python merger.py info data/snapshots.parquet
```

#### Why chunks?

Writing to many small files instead of appending to one large file provides:
- **Crash resilience** — if the process dies, you only lose the in-memory buffer (≤60s of data), not the entire file
- **No rewrite overhead** — appending to Parquet requires reading and rewriting the entire file; chunks avoid this
- **Parallel-friendly** — chunks can be processed independently if needed

### 4. Orderbook Reconstruction (`reconstruct_orderbooks.py`)

This is the most critical step. The raw collected data stores `book` snapshots and `price_change` deltas as JSON strings. The reconstruction script **replays these events in order to rebuild the full orderbook state at every single tick**.

#### The Problem

Polymarket's WebSocket sends two types of events:
- **`book`** events — a full snapshot of all bid and ask levels (sent periodically or on subscription)
- **`price_change`** events — a single level update: `{side: "BUY"/"SELL", price: 0.95, size: 150}` where `size: 0` means remove that level

Between full snapshots, you only receive deltas. To know the complete orderbook at any point in time, you must replay all events in order.

#### How it works

```
For each (market_id, asset_id) group, sorted by datetime:

    current_bids = {}
    current_asks = {}

    for each row:
        if event is "book":
            # Full reset — replace entire orderbook
            current_bids = {price: size for each bid}
            current_asks = {price: size for each ask}

        elif event is "price_change":
            # Incremental update — modify one level
            if side == "BUY":
                if size > 0: current_bids[price] = size
                else:        del current_bids[price]   # level removed
            elif side == "SELL":
                if size > 0: current_asks[price] = size
                else:        del current_asks[price]

        # Write reconstructed orderbook to output arrays
        output_bids[i] = sorted(current_bids, descending)[:50]
        output_asks[i] = sorted(current_asks, ascending)[:50]
        output_best_bid[i] = max(current_bids)
        output_best_ask[i] = min(current_asks)
```

#### Performance optimizations

- **Pre-allocated NumPy arrays** — output is written directly to pre-allocated arrays, avoiding DataFrame overhead
- **Group-based processing** — data is sorted by `(market_id, asset_id, datetime)` and processed in contiguous groups
- **Chunked datetime resorting** — after group processing, a global datetime re-sort uses NumPy argsort with chunked column gathering for memory efficiency
- Processes ~50,000+ rows/second on typical hardware

#### Output columns added

| Column | Type | Description |
|--------|------|-------------|
| `orderbook_bids` | string (JSON) | Full bid side: `[{"price": 0.95, "size": 150}, ...]` |
| `orderbook_asks` | string (JSON) | Full ask side: `[{"price": 0.96, "size": 200}, ...]` |
| `ob_best_bid` | float64 | Best (highest) bid price |
| `ob_best_ask` | float64 | Best (lowest) ask price |
| `ob_mid_price` | float64 | (ob_best_bid + ob_best_ask) / 2 |

#### Running reconstruction

```bash
python reconstruct_orderbooks.py data/snapshots.parquet data/snapshots_reconstructed.parquet
```

The output `*_reconstructed.parquet` file is what the backtest notebook consumes.

---

## Backtest Notebook

### Strategy Overview

The strategy is a **multi-tier DCA (Dollar-Cost Averaging)** approach designed for prediction markets:

> **Enter gradually as confidence increases, but protect aggressively when things go wrong.**

Instead of going all-in at a single price, capital is deployed across 4 tiers:

| Tier | Default Threshold | Capital Weight | Purpose |
|------|------------------|----------------|---------|
| Entry | 0.96 | 49% | Initial position when price shows confidence |
| DCA 1 | 0.96+ | 28% | Add as price climbs |
| DCA 2 | 0.97+ | 10% | Increase as conviction grows |
| DCA 3 | 0.99+ | 6% | Final add at high confidence |

### The Eight Pillars of the Strategy

#### 1. Multi-Tier DCA Entries
Gradual capital deployment as the price crosses configurable thresholds, avoiding all-in risk.

#### 2. Dynamic Hedging
When the main position drops significantly (confirmed over 20 ticks to avoid noise), the system buys the **opposite outcome** as insurance. If the hedge also drops, it's sold and can re-hedge later.

#### 3. Panic Flip Detection
Detects rapid resolution events — when an asset surges 25+ points in 10 seconds above 0.85, the system immediately sells any losing position and buys the surging asset.

#### 4. Hedge Promotion
If a hedge position outperforms the main (20%+ PnL, price ≥0.50, or return delta ≥15%), it gets promoted to main and the original position is closed.

#### 5. Early Entry Detection
Markets that trade stably for 25+ minutes with <2.5% volatility and <7% price range can be entered before the standard trading window (40-90% of market life).

#### 6. Late-Game Stop-Loss Disable
In the final 10% of a market's life, stop-losses are disabled to avoid getting shaken out during pre-resolution volatility.

#### 7. Noise Filtering
Ticks with spread > 10% are considered noisy and ignored for exit/hedge decisions. Stop-losses and hedges require N consecutive confirming ticks before triggering.

#### 8. Portfolio Management
Capital is allocated equally across all selected markets. A global take-profit (default 20%) closes all positions once hit.

### How to Run a Backtest

1. **Prepare data** — run the collection & reconstruction pipeline (or use provided parquet files)
2. **Open the notebook** — `dca_backtest_v11_organized.ipynb`
3. **Set data paths** (Section 2) — point to your reconstructed parquet files
4. **Configure parameters** (Section 9) — or use the defaults
5. **Run all cells** — the backtest loads data, simulates the strategy, and prints results
6. **Visualize** — run the plotting cells to see price charts with trade markers
7. **Optimize** (optional, Section 10) — run Optuna across multiple days to find optimal parameters

---


## Setup & Installation

### Requirements

```bash
# Core dependencies
pip install pandas numpy pyarrow matplotlib optuna

# For the real-time collector only
pip install websockets python-dotenv
```

Or use the collector's requirements file:

```bash
pip install -r collector_parquet/requirements.txt
pip install matplotlib optuna  # additional for the notebook
```

### Quick Start (Backtest Only)

If you already have reconstructed parquet files:

```bash
# 1. Install dependencies
pip install pandas numpy pyarrow matplotlib optuna

# 2. Open the notebook
jupyter notebook dca_backtest_v11_organized.ipynb

# 3. Update DATA_PATHS in Section 2, then Run All
```

### Full Pipeline (Collect → Reconstruct → Backtest)

```bash
# 1. Install all dependencies
pip install -r collector_parquet/requirements.txt
pip install matplotlib optuna

# 2. Get market IDs from Polymarket API responses
#    Place JSON files in data_markets/ directory
python extract_clob_token_ids.py

# 3. Configure and start the collector
cd collector_parquet
export POLYMARKET_ASSET_IDS="id1,id2,..."
python main.py
# ... let it run, then Ctrl+C to stop

# 4. If needed, merge chunks manually
python merger.py merge --data-dir data

# 5. Reconstruct orderbooks
python reconstruct_orderbooks.py data/snapshots.parquet data/snapshots_reconstructed.parquet

# 6. Backtest
cd ..
jupyter notebook dca_backtest_v11_organized.ipynb
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
