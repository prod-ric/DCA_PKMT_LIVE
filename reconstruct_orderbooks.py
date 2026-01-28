"""
Orderbook Reconstruction Script (Optimized + datetime sort progress)
===================================================================

Preprocesses a Polymarket snapshots parquet file by reconstructing full orderbooks
from 'book' snapshots and 'price_change' deltas.

Usage:
    python reconstruct_orderbooks.py input.parquet output.parquet
"""

import pandas as pd
import numpy as np
import json
import sys
import time
import heapq
from typing import Dict, Optional


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


def process_asset_stream(group_df: pd.DataFrame, max_levels: int = 50):
    """Process a single asset stream efficiently. Returns arrays."""
    n = len(group_df)

    # Pre-allocate arrays
    ob_bids = [None] * n
    ob_asks = [None] * n
    best_bids = np.full(n, np.nan)
    best_asks = np.full(n, np.nan)

    current_bids = {}  # price -> size
    current_asks = {}  # price -> size

    raw_jsons = group_df["raw_json"].values
    n_books = 0
    n_deltas = 0

    for i in range(n):
        raw = raw_jsons[i]
        data = parse_raw_json_fast(raw) if raw else None

        if data is not None:
            # BOOK event - full reset
            if "bids" in data and "asks" in data:
                current_bids = {float(b["price"]): float(b["size"]) for b in data.get("bids", [])}
                current_asks = {float(a["price"]): float(a["size"]) for a in data.get("asks", [])}
                n_books += 1

            # PRICE_CHANGE event - delta update
            elif "side" in data and "price" in data and "size" in data:
                price = float(data["price"])
                size = float(data["size"])
                side = str(data["side"]).upper()

                if side == "BUY":
                    if size > 0:
                        current_bids[price] = size
                    else:
                        current_bids.pop(price, None)
                elif side == "SELL":
                    if size > 0:
                        current_asks[price] = size
                    else:
                        current_asks.pop(price, None)

                n_deltas += 1

        # Store current state
        if current_bids or current_asks:
            # Sort and truncate
            sorted_bids = sorted(current_bids.items(), key=lambda x: -x[0])[:max_levels]
            sorted_asks = sorted(current_asks.items(), key=lambda x: x[0])[:max_levels]

            ob_bids[i] = json.dumps([{"price": p, "size": s} for p, s in sorted_bids])
            ob_asks[i] = json.dumps([{"price": p, "size": s} for p, s in sorted_asks])
            best_bids[i] = sorted_bids[0][0] if sorted_bids else np.nan
            best_asks[i] = sorted_asks[0][0] if sorted_asks else np.nan

    return ob_bids, ob_asks, best_bids, best_asks, n_books, n_deltas


def sort_by_datetime_fast_with_progress(
    df: pd.DataFrame,
    col: str = "datetime",
    chunk_rows: int = 2_000_000,
) -> pd.DataFrame:
    """
    Sort df by datetime with:
      - fast path (numpy argsort) when df is not huge
      - chunked sort+merge with progress when df is huge

    Note: pandas.sort_values() cannot expose real progress; this does by chunking.
    """
    n = len(df)
    print("⏳ Sorting by datetime...")

    # Convert once to int64 nanoseconds (NaT -> very negative int64)
    dt = pd.to_datetime(df[col], errors="coerce").values.astype("datetime64[ns]")
    dt_i8 = dt.view("int64")

    t0 = time.time()

    # Fast path
    if n <= chunk_rows:
        order = np.argsort(dt_i8, kind="mergesort")  # stable
        out = df.take(order).reset_index(drop=True)
        print(f"   Done in {time.time()-t0:.1f}s")
        return out

    # Chunked path
    chunks = []
    n_chunks = (n + chunk_rows - 1) // chunk_rows

    # 1) Chunk-sort
    for k in range(n_chunks):
        start = k * chunk_rows
        end = min((k + 1) * chunk_rows, n)

        idx = np.arange(start, end, dtype=np.int64)
        local_order = np.argsort(dt_i8[start:end], kind="mergesort")
        sorted_idx = idx[local_order]
        chunks.append(sorted_idx)

        pct = (k + 1) / n_chunks * 100
        print(f"\r   Chunk-sorting: {k+1}/{n_chunks} ({pct:5.1f}%)", end="", flush=True)

    print()  # newline

    # 2) K-way merge with progress
    print("   Merging chunks...")
    merged_order = np.empty(n, dtype=np.int64)

    heap = []
    for cid, sidx in enumerate(chunks):
        first_idx = sidx[0]
        heap.append((dt_i8[first_idx], cid, 0))
    heapq.heapify(heap)

    filled = 0
    last_log = time.time()
    while heap:
        _, cid, pos = heapq.heappop(heap)
        row_idx = chunks[cid][pos]
        merged_order[filled] = row_idx
        filled += 1

        nxt = pos + 1
        if nxt < len(chunks[cid]):
            nxt_idx = chunks[cid][nxt]
            heapq.heappush(heap, (dt_i8[nxt_idx], cid, nxt))

        now = time.time()
        if now - last_log >= 1.0 or filled == n:
            pct = filled / n * 100
            rate = filled / (now - t0) if now > t0 else 0
            eta = (n - filled) / rate if rate > 0 else 0
            bar_w = 30
            filled_w = int(bar_w * pct / 100)
            bar = "█" * filled_w + "░" * (bar_w - filled_w)
            print(
                f"\r   [{bar}] {pct:5.1f}% | {filled:>10,}/{n:,} rows | "
                f"{rate:>8,.0f}/s | ETA {eta:>5.0f}s",
                end="",
                flush=True,
            )
            last_log = now

    print()  # newline
    out = df.take(merged_order).reset_index(drop=True)
    print(f"   Done in {time.time()-t0:.1f}s")
    return out


def reconstruct_orderbooks(df: pd.DataFrame, max_levels: int = 50) -> pd.DataFrame:
    """Reconstruct orderbooks with progress logging."""
    print(f"\n📊 Input: {len(df):,} rows")

    # Sort once for correct reconstruction
    print("⏳ Sorting data by (market_id, asset_id, datetime)...")
    t0 = time.time()
    df = df.sort_values(["market_id", "asset_id", "datetime"], kind="mergesort").reset_index(drop=True)
    print(f"   Done in {time.time()-t0:.1f}s")

    n_total = len(df)

    # Use numpy object arrays so we can assign by indices in one shot (FAST)
    all_ob_bids = np.empty(n_total, dtype=object)
    all_ob_asks = np.empty(n_total, dtype=object)
    all_ob_bids[:] = None
    all_ob_asks[:] = None
    all_best_bids = np.full(n_total, np.nan)
    all_best_asks = np.full(n_total, np.nan)

    # Group by asset stream
    print("⏳ Grouping by (market, asset)...")
    t0 = time.time()
    grouped = df.groupby(["market_id", "asset_id"], sort=False)
    n_groups = grouped.ngroups
    print(f"   {n_groups} asset streams in {time.time()-t0:.1f}s")

    # Process each group
    print(f"\n🔧 Processing {n_groups} streams...")
    print("-" * 70)

    total_rows_done = 0
    total_books = 0
    total_deltas = 0
    start_time = time.time()
    last_log_time = start_time

    for i, (key, group) in enumerate(grouped):
        indices = group.index.to_numpy(dtype=np.int64)
        group_size = len(group)

        ob_bids, ob_asks, best_bids, best_asks, n_books, n_deltas = process_asset_stream(group, max_levels)

        # Vectorized assignment (much faster than Python loop)
        all_ob_bids[indices] = ob_bids
        all_ob_asks[indices] = ob_asks
        all_best_bids[indices] = best_bids
        all_best_asks[indices] = best_asks

        total_rows_done += group_size
        total_books += n_books
        total_deltas += n_deltas

        # Progress logging
        now = time.time()
        if now - last_log_time >= 1.0 or i == n_groups - 1:
            elapsed = now - start_time
            pct = (i + 1) / n_groups * 100
            rate = total_rows_done / elapsed if elapsed > 0 else 0
            eta = (n_total - total_rows_done) / rate if rate > 0 else 0

            bar_width = 30
            filled = int(bar_width * pct / 100)
            bar = "█" * filled + "░" * (bar_width - filled)

            print(
                f"\r   [{bar}] {pct:5.1f}% | "
                f"{total_rows_done:>10,}/{n_total:,} rows | "
                f"{rate:>8,.0f}/s | "
                f"ETA {eta:>5.0f}s | "
                f"📚{total_books:,} 📝{total_deltas:,}",
                end="",
                flush=True,
            )
            last_log_time = now

    print()  # newline
    print("-" * 70)

    # Assign columns
    print("\n⏳ Assigning columns...")
    t0 = time.time()
    df["orderbook_bids"] = all_ob_bids
    df["orderbook_asks"] = all_ob_asks
    df["ob_best_bid"] = all_best_bids
    df["ob_best_ask"] = all_best_asks
    df["ob_mid_price"] = (all_best_bids + all_best_asks) / 2
    print(f"   Done in {time.time()-t0:.1f}s")

    # Sort by datetime with progress + faster path
    df = sort_by_datetime_fast_with_progress(df, col="datetime", chunk_rows=2_000_000)

    # Stats
    valid = np.isfinite(df["ob_best_bid"].to_numpy()).sum()
    print(f"\n✅ Reconstructed: {total_books:,} book snapshots + {total_deltas:,} price_change deltas")
    print(f"✅ Valid rows: {valid:,} / {len(df):,} ({valid/len(df)*100:.1f}%)")

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
    print("🔄 ORDERBOOK RECONSTRUCTION")
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

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
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
    print(f"✅ COMPLETE | Total: {total_elapsed:.1f}s | {len(df)/total_elapsed:,.0f} rows/s")
    print("=" * 70)
    print("\n📋 New columns:")
    print("   • orderbook_bids  - JSON array of bid levels")
    print("   • orderbook_asks  - JSON array of ask levels")
    print("   • ob_best_bid     - Best bid price")
    print("   • ob_best_ask     - Best ask price")
    print("   • ob_mid_price    - Mid price")


if __name__ == "__main__":
    main()
