"""
Orderbook Reconstruction Script (Optimized v2.1)
===============================================

Preprocesses a Polymarket snapshots parquet file by reconstructing full orderbooks
from 'book' snapshots and 'price_change' deltas.

Changes vs v2:
- Faster final reorder by datetime using NumPy argsort + chunked gather
- Progress logging during the reorder step

Usage:
    python reconstruct_orderbooks.py input.parquet output.parquet
"""

import pandas as pd
import numpy as np
import json
import sys
from typing import Dict, Optional
import time


def parse_raw_json_fast(raw_json: str) -> Optional[Dict]:
    """Fast JSON parsing with minimal checks."""
    if not raw_json or not isinstance(raw_json, str):
        return None
    try:
        s = raw_json.strip()
        if s.startswith('"'):
            s = s[1:-1].replace('""', '"')
        return json.loads(s)
    except Exception:
        return None


def process_asset_stream_inplace(
    raw_jsons: np.ndarray,
    out_bids: np.ndarray,
    out_asks: np.ndarray,
    out_best_bid: np.ndarray,
    out_best_ask: np.ndarray,
    start_idx: int,
    max_levels: int = 50
) -> tuple:
    """Process asset stream and write directly to output arrays."""
    n = len(raw_jsons)
    current_bids = {}
    current_asks = {}
    n_books = 0
    n_deltas = 0

    for i in range(n):
        raw = raw_jsons[i]
        data = parse_raw_json_fast(raw) if raw else None

        if data is not None:
            if 'bids' in data and 'asks' in data:
                # BOOK event - full reset
                current_bids = {float(b['price']): float(b['size']) for b in data.get('bids', [])}
                current_asks = {float(a['price']): float(a['size']) for a in data.get('asks', [])}
                n_books += 1

            elif 'side' in data and 'price' in data and 'size' in data:
                # PRICE_CHANGE event
                price = float(data['price'])
                size = float(data['size'])
                side = str(data['side']).upper()

                if side == 'BUY':
                    if size > 0:
                        current_bids[price] = size
                    else:
                        current_bids.pop(price, None)

                elif side == 'SELL':
                    if size > 0:
                        current_asks[price] = size
                    else:
                        current_asks.pop(price, None)

                n_deltas += 1

        # Write directly to output arrays
        idx = start_idx + i
        if current_bids or current_asks:
            sorted_bids = sorted(current_bids.items(), key=lambda x: -x[0])[:max_levels]
            sorted_asks = sorted(current_asks.items(), key=lambda x: x[0])[:max_levels]

            out_bids[idx] = json.dumps([{'price': p, 'size': s} for p, s in sorted_bids])
            out_asks[idx] = json.dumps([{'price': p, 'size': s} for p, s in sorted_asks])
            out_best_bid[idx] = sorted_bids[0][0] if sorted_bids else np.nan
            out_best_ask[idx] = sorted_asks[0][0] if sorted_asks else np.nan

    return n_books, n_deltas


def resort_by_datetime_fast_with_log(df: pd.DataFrame, chunk_rows: int = 1_000_000) -> pd.DataFrame:
    """
    Faster global resort by datetime using NumPy argsort + chunked column gathering.
    Prints progress while materializing the reordered DataFrame.

    Notes:
    - Still O(n log n). This just reduces pandas overhead and gives logging.
    - chunk_rows controls memory pressure vs speed.
    """
    if 'datetime' not in df.columns:
        return df

    n = len(df)
    if n == 0:
        return df.reset_index(drop=True)

    t0 = time.time()

    # Ensure datetime64[ns] for fast argsort
    dt = pd.to_datetime(df['datetime'], errors='coerce').to_numpy(dtype='datetime64[ns]')

    print("⏳ Building datetime sort index (NumPy argsort)...")
    t1 = time.time()
    order = np.argsort(dt, kind='mergesort')  # stable sort
    print(f"   ✓ sort index in {time.time() - t1:.2f}s")

    print("⏳ Reordering columns (chunked gather)...")

    cols = list(df.columns)

    # Extract arrays once (avoid Series overhead in the hot loop)
    arrays = {c: df[c].to_numpy(copy=False) for c in cols}

    # Allocate output arrays
    out = {}
    for c in cols:
        arr = arrays[c]
        # Ensure object columns remain object; others keep dtype
        out[c] = np.empty(n, dtype=object) if arr.dtype == object else np.empty_like(arr)

    done = 0
    start = time.time()
    last_log = start

    while done < n:
        end = min(done + chunk_rows, n)
        idx = order[done:end]

        # Copy chunk for each column
        for c in cols:
            out[c][done:end] = arrays[c][idx]

        done = end

        now = time.time()
        if now - last_log >= 0.5 or done == n:
            pct = done / n * 100.0
            rate = done / (now - start) if now > start else 0.0
            eta = (n - done) / rate if rate > 0 else 0.0

            bar_width = 30
            filled = int(bar_width * pct / 100)
            bar = '█' * filled + '░' * (bar_width - filled)

            print(
                f"\r   [{bar}] {pct:5.1f}% | {done:>10,}/{n:,} rows | "
                f"{rate:>8,.0f}/s | ETA {eta:>5.0f}s",
                end='',
                flush=True
            )
            last_log = now

    print()

    df2 = pd.DataFrame(out)
    df2.reset_index(drop=True, inplace=True)

    print(f"   ✓ datetime reorder complete in {time.time() - t0:.2f}s")
    return df2


def reconstruct_orderbooks(df: pd.DataFrame, max_levels: int = 50) -> pd.DataFrame:
    """Reconstruct orderbooks with progress logging."""
    print(f"\n📊 Input: {len(df):,} rows")

    # Sort once for correct reconstruction per (market_id, asset_id, datetime)
    print("⏳ Sorting by (market, asset, time)...")
    t0 = time.time()
    df = df.sort_values(['market_id', 'asset_id', 'datetime']).reset_index(drop=True)
    print(f"   Done in {time.time() - t0:.1f}s")

    n_total = len(df)

    # Pre-allocate output arrays (object arrays for JSON strings)
    print("⏳ Allocating output arrays...")
    t0 = time.time()
    out_bids = np.empty(n_total, dtype=object)
    out_asks = np.empty(n_total, dtype=object)
    out_best_bid = np.full(n_total, np.nan, dtype=np.float64)
    out_best_ask = np.full(n_total, np.nan, dtype=np.float64)
    print(f"   Done in {time.time() - t0:.1f}s")

    # Get raw_json as numpy array for fast access
    raw_json_arr = df['raw_json'].values

    # Find group boundaries using market_id + asset_id
    print("⏳ Finding group boundaries...")
    t0 = time.time()

    group_key = df['market_id'].astype(str) + '|' + df['asset_id'].astype(str)
    group_key_arr = group_key.values

    group_starts = [0]
    for i in range(1, n_total):
        if group_key_arr[i] != group_key_arr[i - 1]:
            group_starts.append(i)
    group_starts.append(n_total)  # End marker

    n_groups = len(group_starts) - 1
    print(f"   Found {n_groups} groups in {time.time() - t0:.1f}s")

    # Process each group
    print(f"\n🔧 Processing {n_groups} asset streams...")
    print("-" * 70)

    total_rows_done = 0
    total_books = 0
    total_deltas = 0
    start_time = time.time()
    last_log_time = start_time

    for gi in range(n_groups):
        start_idx = group_starts[gi]
        end_idx = group_starts[gi + 1]
        group_size = end_idx - start_idx

        group_raw = raw_json_arr[start_idx:end_idx]

        n_books, n_deltas = process_asset_stream_inplace(
            group_raw,
            out_bids,
            out_asks,
            out_best_bid,
            out_best_ask,
            start_idx,
            max_levels
        )

        total_rows_done += group_size
        total_books += n_books
        total_deltas += n_deltas

        now = time.time()
        if now - last_log_time >= 1.0 or gi == n_groups - 1:
            elapsed = now - start_time
            pct = (gi + 1) / n_groups * 100.0
            rate = total_rows_done / elapsed if elapsed > 0 else 0.0
            eta = (n_total - total_rows_done) / rate if rate > 0 else 0.0

            bar_width = 30
            filled = int(bar_width * pct / 100)
            bar = '█' * filled + '░' * (bar_width - filled)

            print(
                f"\r   [{bar}] {pct:5.1f}% | "
                f"{total_rows_done:>10,}/{n_total:,} rows | "
                f"{rate:>8,.0f}/s | "
                f"ETA {eta:>5.0f}s | "
                f"📚{total_books:,} 📝{total_deltas:,}",
                end='',
                flush=True
            )
            last_log_time = now

    print()
    print("-" * 70)

    # Assign to dataframe (single vectorized operation)
    print("\n⏳ Assigning columns...")
    t0 = time.time()
    df['orderbook_bids'] = out_bids
    df['orderbook_asks'] = out_asks
    df['ob_best_bid'] = out_best_bid
    df['ob_best_ask'] = out_best_ask
    df['ob_mid_price'] = (out_best_bid + out_best_ask) / 2.0
    print(f"   Done in {time.time() - t0:.1f}s")

    # Faster reorder by datetime with progress logging
    df = resort_by_datetime_fast_with_log(df, chunk_rows=1_000_000)

    # Stats
    valid = np.isfinite(out_best_bid).sum()
    print(f"\n✅ Reconstructed: {total_books:,} book snapshots + {total_deltas:,} price_change deltas")
    print(f"✅ Valid rows: {valid:,} / {len(df):,} ({valid / len(df) * 100:.1f}%)")

    return df


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        print("❌ Error: Please provide input and output file paths")
        print("   Usage: python reconstruct_orderbooks.py input.parquet output.parquet")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    print("=" * 70)
    print("🔄 ORDERBOOK RECONSTRUCTION (v2.1)")
    print("=" * 70)
    print(f"📂 Input:  {input_path}")
    print(f"📂 Output: {output_path}")

    # Load
    print("\n📖 Loading parquet...")
    t0 = time.time()
    df = pd.read_parquet(input_path)
    load_time = time.time() - t0
    print(f"   ✓ {len(df):,} rows in {load_time:.1f}s")
    print(f"   ✓ Markets: {df['market_id'].nunique()} | Assets: {df['asset_id'].nunique()}")

    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
        print(f"   ✓ Time: {df['datetime'].min()} → {df['datetime'].max()}")

    # Process
    total_start = time.time()
    df_out = reconstruct_orderbooks(df)
    total_elapsed = time.time() - total_start

    # Save
    print(f"\n💾 Saving to {output_path}...")
    t0 = time.time()
    df_out.to_parquet(output_path, index=False)
    save_time = time.time() - t0
    print(f"   ✓ Saved in {save_time:.1f}s")

    # Summary
    print("\n" + "=" * 70)
    print(f"✅ COMPLETE | Total: {total_elapsed:.1f}s | {len(df) / total_elapsed:,.0f} rows/s")
    print("=" * 70)
    print(f"\n📋 New columns:")
    print(f"   • orderbook_bids  - JSON array of bid levels")
    print(f"   • orderbook_asks  - JSON array of ask levels")
    print(f"   • ob_best_bid     - Best bid price")
    print(f"   • ob_best_ask     - Best ask price")
    print(f"   • ob_mid_price    - Mid price")


if __name__ == "__main__":
    main()
