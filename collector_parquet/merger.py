# ============================================
# FILE: merge_chunks.py
# ============================================

"""
Standalone script to merge parquet chunk files
Run this if the collector crashed or you want to merge manually
"""

import os
import sys
import argparse
import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def format_bytes(bytes_size: int) -> str:
    """Format bytes into human readable size"""
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 * 1024:
        return f"{bytes_size / 1024:.1f} KB"
    elif bytes_size < 1024 * 1024 * 1024:
        return f"{bytes_size / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_size / (1024 * 1024 * 1024):.2f} GB"


def merge_chunks(data_dir: str = "data/chunks", output_file: str = None, delete_chunks: bool = False):
    """
    Merge all chunk files into a single parquet file
    
    Args:
        data_dir: Directory containing chunks/ folder
        output_file: Output file path (default: data_dir/snapshots.parquet)
        delete_chunks: Whether to delete chunk files after merge
    
    Returns:
        Path to merged file
    """
    chunks_dir = os.path.join(data_dir, "chunks")
    print(chunks_dir)
    if output_file is None:
        output_file = os.path.join(data_dir, "snapshots.parquet")
    
    # Check chunks directory exists
    if not os.path.exists(chunks_dir):
        logger.error(f"Chunks directory not found: {chunks_dir}")
        return None
    
    # Find all chunk files
    chunk_files = sorted([
        os.path.join(chunks_dir, f)
        for f in os.listdir(chunks_dir)
        if f.startswith('chunk_') and f.endswith('.parquet')
    ])
    
    if not chunk_files:
        logger.warning("No chunk files found")
        return None
    
    logger.info(f"Found {len(chunk_files)} chunk files")
    
    # Calculate total size before merge
    total_chunk_size = sum(os.path.getsize(f) for f in chunk_files)
    logger.info(f"Total chunk size: {format_bytes(total_chunk_size)}")
    
    # Read all chunks
    tables = []
    total_rows = 0
    
    # Include existing main file if exists
    if os.path.exists(output_file):
        logger.info(f"Including existing file: {output_file}")
        existing_table = pq.read_table(output_file)
        tables.append(existing_table)
        total_rows += len(existing_table)
        logger.info(f"  - Existing: {len(existing_table):,} rows")
    
    # Read chunks
    for i, chunk_file in enumerate(chunk_files):
        try:
            table = pq.read_table(chunk_file)
            tables.append(table)
            total_rows += len(table)
            
            if (i + 1) % 10 == 0 or i == len(chunk_files) - 1:
                logger.info(f"  - Read {i + 1}/{len(chunk_files)} chunks ({total_rows:,} rows)")
        except Exception as e:
            logger.error(f"Error reading {chunk_file}: {e}")
    
    if not tables:
        logger.error("No data to merge")
        return None
    
    # Concatenate all tables
    logger.info("Concatenating tables...")
    combined = pa.concat_tables(tables)
    
    # Sort by timestamp
    logger.info("Sorting by timestamp...")
    combined = combined.sort_by('timestamp')
    
    # Write merged file
    logger.info(f"Writing to {output_file}...")
    pq.write_table(combined, output_file, compression='snappy')
    
    # Get final file size
    final_size = os.path.getsize(output_file)
    
    logger.info(f"""
================================================================================
✅ MERGE COMPLETE
================================================================================
Chunks merged:       {len(chunk_files)}
Total rows:          {len(combined):,}
Output file:         {output_file}
File size:           {format_bytes(final_size)}
================================================================================
""")
    
    # Delete chunks if requested
    if delete_chunks:
        logger.info("Deleting chunk files...")
        for chunk_file in chunk_files:
            try:
                os.remove(chunk_file)
            except Exception as e:
                logger.error(f"Error deleting {chunk_file}: {e}")
        logger.info(f"Deleted {len(chunk_files)} chunk files")
    
    return output_file


def list_chunks(data_dir: str = "data"):
    """List all chunk files and their info"""
    chunks_dir = os.path.join(data_dir, "chunks")
    main_file = os.path.join(data_dir, "snapshots.parquet")
    
    print(f"\n{'='*70}")
    print(f"DATA DIRECTORY: {data_dir}")
    print(f"{'='*70}\n")
    
    # Main file
    if os.path.exists(main_file):
        size = os.path.getsize(main_file)
        rows = pq.read_metadata(main_file).num_rows
        print(f"📦 MAIN FILE: {main_file}")
        print(f"   Size: {format_bytes(size)}")
        print(f"   Rows: {rows:,}")
    else:
        print(f"📦 MAIN FILE: Not found")
    
    print()
    
    # Chunks
    if not os.path.exists(chunks_dir):
        print(f"📁 CHUNKS: Directory not found")
        return
    
    chunk_files = sorted([
        f for f in os.listdir(chunks_dir)
        if f.startswith('chunk_') and f.endswith('.parquet')
    ])
    
    if not chunk_files:
        print(f"📁 CHUNKS: None")
        return
    
    print(f"📁 CHUNKS: {len(chunk_files)} files\n")
    
    total_size = 0
    total_rows = 0
    
    print(f"   {'File':<30} {'Size':>12} {'Rows':>12}")
    print(f"   {'-'*30} {'-'*12} {'-'*12}")
    
    for chunk_file in chunk_files:
        path = os.path.join(chunks_dir, chunk_file)
        size = os.path.getsize(path)
        rows = pq.read_metadata(path).num_rows
        
        total_size += size
        total_rows += rows
        
        print(f"   {chunk_file:<30} {format_bytes(size):>12} {rows:>12,}")
    
    print(f"   {'-'*30} {'-'*12} {'-'*12}")
    print(f"   {'TOTAL':<30} {format_bytes(total_size):>12} {total_rows:>12,}")
    print()


def main():
    parser = argparse.ArgumentParser(description='Merge parquet chunk files')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Merge command
    merge_parser = subparsers.add_parser('merge', help='Merge chunk files')
    merge_parser.add_argument('--data-dir', '-d', default='data', help='Data directory (default: data)')
    merge_parser.add_argument('--output', '-o', help='Output file path')
    merge_parser.add_argument('--delete', action='store_true', help='Delete chunks after merge')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List chunk files')
    list_parser.add_argument('--data-dir', '-d', default='data', help='Data directory (default: data)')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show info about parquet file')
    info_parser.add_argument('file', help='Parquet file path')
    
    args = parser.parse_args()
    
    if args.command == 'merge':
        merge_chunks(args.data_dir, args.output, args.delete)
    
    elif args.command == 'list':
        list_chunks(args.data_dir)
    
    elif args.command == 'info':
        if not os.path.exists(args.file):
            print(f"File not found: {args.file}")
            return
        
        metadata = pq.read_metadata(args.file)
        size = os.path.getsize(args.file)
        
        # Read sample to get date range
        df = pq.read_table(args.file, columns=['timestamp', 'datetime']).to_pandas()
        
        print(f"""
================================================================================
FILE INFO: {args.file}
================================================================================
Rows:            {metadata.num_rows:,}
Columns:         {metadata.num_columns}
Size:            {format_bytes(size)}
Row groups:      {metadata.num_row_groups}

Date range:      {df['datetime'].min()} to {df['datetime'].max()}

Schema:
{metadata.schema.to_arrow_schema()}
================================================================================
""")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()