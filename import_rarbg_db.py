#!/usr/bin/env python3

import random
import sqlite3
import json
import string
import sys
import time
import os
import requests
import argparse
from datetime import datetime
from typing import Dict, Optional, Any

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Import RARBG SQLite database into bitmagnet.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        'db_path',
        help='Path to the RARBG SQLite database file'
    )
    
    parser.add_argument(
        'bitmagnet_url',
        help='Base URL of the bitmagnet server (e.g., http://localhost:3333)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of items to process in each batch'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without sending data to bitmagnet. Will still create checkpoint file.'
    )
    
    parser.add_argument(
        '--no-checkpoint',
        action='store_true',
        help='Disable checkpoint functionality'
    )
    
    parser.add_argument(
        '--keep-checkpoint',
        action='store_true',
        help='Keep checkpoint file after successful completion'
    )
    
    parser.add_argument(
        '--filter-category',
        action='append',
        help='Only process items in these categories. Can be specified multiple times.'
    )
    
    parser.add_argument(
        '--exclude-category',
        action='append',
        help='Exclude items in these categories. Can be specified multiple times.'
    )

    return parser.parse_args()

def map_category_to_content_type(cat: str) -> Optional[str]:
    """Map RARBG category to bitmagnet content type."""
    if cat.startswith('ebooks'):
        return 'ebook'
    elif cat.startswith(('games_', 'software_')):
        return 'software'
    elif cat.startswith('movies'):
        return 'movie'
    elif cat.startswith('tv'):
        return 'tv_show'
    elif cat.startswith('music'):
        return 'music'
    elif cat == 'xxx':
        return 'xxx'
    return None

def get_video_resolution(cat: str) -> Optional[str]:
    """
    Infer a video resolution from the category string.

    Returns one of:
    - 'V2160p' (for 4K, UHD)
    - 'V720p'
    - 'V480p' (for SD)
    - 'V1080p' (fallback for most 'movies' or 'tv' categories)
    - None otherwise
    """

    cat_lower = cat.lower()

    # Check for 4K or UHD first
    if '4k' in cat_lower or 'uhd' in cat_lower:
        return 'V2160p'

    # Then check for explicit 720
    if '720' in cat_lower:
        return 'V720p'

    # Then check for SD
    if 'sd' in cat_lower:
        return 'V480p'

    # Fallback: if it's a movie or TV category (e.g. movies_x264, tv_sd, etc.)
    if cat_lower.startswith('movies') or cat_lower.startswith('tv'):
        return 'V1080p'

    # No match
    return None


def get_video_source(cat: str) -> Optional[str]:
    """Extract video source from category."""
    if '_bd_' in cat:
        return 'BluRay'
    return None

def get_video_modifier(cat: str) -> Optional[str]:
    """Extract video modifier from category."""
    if '_bd_full' in cat:
        return 'BRDISK'
    elif '_bd_remux' in cat:
        return 'REMUX'
    return None

def get_video_codec(cat: str) -> Optional[str]:
    """Extract video codec from category."""
    if '_x264' in cat:
        return 'x264'
    elif '_x265' in cat:
        return 'x265'
    elif '_xvid' in cat:
        return 'XviD'
    return None

def get_video_3d(cat: str) -> Optional[str]:
    """Extract 3D information from category."""
    if '_3d' in cat:
        return 'V3D'
    return None

def transform_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a database row into the format expected by bitmagnet."""
    result = {
        'source': 'rarbg',
        'infoHash': row['hash'].lower(),  # Convert to lowercase as it's more common
        'name': row['title'],
        'size': row['size'],
        'publishedAt': datetime.strptime(row['dt'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S.000Z')
    }

    # Add content type and related fields
    content_type = map_category_to_content_type(row['cat'])
    if content_type:
        result['contentType'] = content_type

    # Add IMDB reference if available
    if row['imdb']:
        result['contentSource'] = 'imdb'
        result['contentId'] = row['imdb']

    # Add video-specific fields for movies and TV shows
    if content_type in ('movie', 'tv_show'):
        for field, value in {
            'videoResolution': get_video_resolution(row['cat']),
            'videoSource': get_video_source(row['cat']),
            'videoModifier': get_video_modifier(row['cat']),
            'videoCodec': get_video_codec(row['cat']),
            'video3D': get_video_3d(row['cat'])
        }.items():
            if value:
                result[field] = value

    return {k: v for k, v in result.items() if v is not None}

def get_checkpoint_file(db_path: str) -> str:
    """Get the path to the checkpoint file based on the database path."""
    return f"{db_path}.checkpoint"

def load_checkpoint(checkpoint_file: str) -> Optional[str]:
    """Load the last processed info hash from checkpoint file."""
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                return f.read().strip()
    except Exception as e:
        print(f"Warning: Failed to read checkpoint file: {e}")
    return None

def save_checkpoint(checkpoint_file: str, info_hash: str) -> None:
    """Save the last processed info hash to checkpoint file."""
    try:
        with open(checkpoint_file, 'w') as f:
            f.write(info_hash)
    except Exception as e:
        print(f"Warning: Failed to write checkpoint file: {e}")

def main():
    args = parse_args()
    
    if not os.path.isfile(args.db_path):
        print(f"Error: Database file not found: {args.db_path}")
        sys.exit(1)

    bitmagnet_url = args.bitmagnet_url.rstrip('/') + '/import'
    checkpoint_file = get_checkpoint_file(args.db_path) if not args.no_checkpoint else None
    
    # Load last processed info hash from checkpoint
    last_processed_hash = load_checkpoint(checkpoint_file) if checkpoint_file else None
    if last_processed_hash:
        print(f"Resuming from last processed info hash: {last_processed_hash}")

    # Connect to the SQLite database
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    # Prepare the cursor and query
    cursor = conn.cursor()
    
    # Build WHERE clause for category filtering
    where_clauses = []
    params = []
    
    if last_processed_hash:
        where_clauses.append("LOWER(hash) > ?")
        params.append(last_processed_hash)
    
    if args.filter_category:
        placeholders = ','.join(['?' for _ in args.filter_category])
        where_clauses.append(f"cat IN ({placeholders})")
        params.extend(args.filter_category)
    
    if args.exclude_category:
        placeholders = ','.join(['?' for _ in args.exclude_category])
        where_clauses.append(f"cat NOT IN ({placeholders})")
        params.extend(args.exclude_category)
    
    where_clause = " AND ".join(where_clauses) if where_clauses else ""
    
    # Count total remaining rows
    count_query = "SELECT COUNT(*) FROM items"
    if where_clause:
        count_query += f" WHERE {where_clause}"
    
    cursor.execute(count_query, params)
    total_rows = cursor.fetchone()[0]
    print(f"Total rows to process: {total_rows}")

    if args.dry_run:
        print("Running in dry-run mode - no data will be sent to bitmagnet")
    
    # Build the main query
    query = f"""
        SELECT hash, title, dt, cat, size, imdb
        FROM items
        {f'WHERE {where_clause}' if where_clause else ''}
        ORDER BY LOWER(hash)
    """
    
    cursor.execute(query, params)

    session = requests.Session()
    processed = 0
    
    try:
        while True:
            batch = cursor.fetchmany(args.batch_size)
            if not batch:
                break

            # Transform the batch into NDJSON
            json_lines = '\n'.join(
                json.dumps(transform_row(dict(row)), separators=(',', ':'))
                for row in batch
            )

            if not args.dry_run:
                # Send to bitmagnet
                try:
                    suffix = "".join(random.choices(string.ascii_letters + string.digits, k=5))
                    response = session.post(
                        bitmagnet_url,
                        data=json_lines + '\n',  # Add final newline
                        headers={
                            'Content-Type': 'application/json',
                            'x-import-id': f'rarbg-import-{int(time.time())}-{suffix}'
                        }
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Error sending batch: {e}")
                    if hasattr(e.response, 'text'):
                        print(f"Response text: {e.response.text}")
                    sys.exit(1)

            processed += len(batch)
            if checkpoint_file:
                last_hash = batch[-1]['hash'].lower()
                save_checkpoint(checkpoint_file, last_hash)
            
            print(f"Processed {processed}/{total_rows} items ({(processed/total_rows*100):.1f}%)")

    except KeyboardInterrupt:
        print("\nImport interrupted. Progress has been saved. You can resume later from the last processed item.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError occurred: {e}")
        print("Progress has been saved. You can resume later from the last processed item.")
        sys.exit(1)
    finally:
        conn.close()

    print("Import completed successfully!")
    # Remove checkpoint file after successful completion if not keeping it
    if checkpoint_file and not args.keep_checkpoint and not args.dry_run:
        try:
            os.remove(checkpoint_file)
            print("Checkpoint file removed.")
        except:
            pass

if __name__ == '__main__':
    main() 