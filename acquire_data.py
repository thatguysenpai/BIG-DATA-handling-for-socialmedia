"""Dataset acquisition: streams English tweets (DLT-Tweets) and Reddit comments (r/askscience, size target reached in the first split)
from HuggingFace and writes 50,000-row Parquet chunks to data/raw/ (config.RAW_DIR).

    python acquire_data.py

Chunked writing keeps memory low on a 12 GB machine. Re-running deletes old chunks first.
"""
import os
import gc
import pandas as pd
from pathlib import Path
from datasets import load_dataset

import config

OUTPUT_DIR = config.RAW_DIR

TWEET_TARGET_BYTES = 1_200_000_000   
REDDIT_TARGET_BYTES = 1_200_000_000  
REDDIT_SUBREDDITS = ["askscience", "gaming", "technology", "todayilearned", "programming"]
CHUNK_SIZE = 50_000  # Write to disk every 50k rows to guarantee low memory usage

os.makedirs(OUTPUT_DIR, exist_ok=True)

def estimate_row_bytes(row: dict) -> int:
    return sum(len(str(v).encode("utf-8", errors="ignore")) for v in row.values())

def pull_tweets():
    print("Streaming ExponentialScience/DLT-Tweets, filtering to English...")
    ds = load_dataset("ExponentialScience/DLT-Tweets", split="train", streaming=True)

    rows = []
    running_bytes = 0
    total_rows = 0
    chunk_idx = 0
    
    for row in ds:
        if row.get("language") != "en":
            continue
        
        rows.append(row)
        running_bytes += estimate_row_bytes(row)
        total_rows += 1
        
        if total_rows % 50_000 == 0:
            print(f"  ...{total_rows:,} rows processed, ~{running_bytes / 1e9:.2f} GB accumulated")
            
        if len(rows) >= CHUNK_SIZE:
            print(f"  -> Writing chunk {chunk_idx} to disk...")
            df = pd.DataFrame(rows)
            out_path = os.path.join(OUTPUT_DIR, f"tweets_chunk_{chunk_idx}.parquet")
            df.to_parquet(out_path, index=False)
            print(f"  -> Written chunk {chunk_idx} ({len(rows):,} rows)")
            rows = []  # Clear memory immediately
            gc.collect() # Force garbage collection
            chunk_idx += 1
            
        if running_bytes >= TWEET_TARGET_BYTES:
            print("  -> Target size reached. Stopping tweet download.")
            break

    # Write any remaining rows
    if rows:
        print(f"  -> Writing final chunk {chunk_idx} to disk...")
        df = pd.DataFrame(rows)
        out_path = os.path.join(OUTPUT_DIR, f"tweets_chunk_{chunk_idx}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"  -> Written final chunk {chunk_idx} ({len(rows):,} rows)")

    print(f"Tweets: {total_rows:,} rows, ~{running_bytes / 1e9:.2f} GB total")
    return total_rows, running_bytes

def pull_reddit():
    print(f"Pulling Reddit comments from subreddit splits: {REDDIT_SUBREDDITS}")
    all_rows = []
    running_bytes = 0
    total_rows = 0
    chunk_idx = 0

    for subreddit in REDDIT_SUBREDDITS:
        if running_bytes >= REDDIT_TARGET_BYTES:
            break
        try:
            ds = load_dataset("HuggingFaceGECLM/REDDIT_comments", split=subreddit, streaming=True)
        except Exception as e:
            print(f"  Skipping subreddit '{subreddit}': {e}")
            continue

        for row in ds:
            row.setdefault("subreddit", subreddit)   # keep the community name so per-subreddit sentiment works
            all_rows.append(row)
            running_bytes += estimate_row_bytes(row)
            total_rows += 1
            
            if total_rows % 50_000 == 0:
                print(f"  ...{total_rows:,} rows processed, ~{running_bytes / 1e9:.2f} GB accumulated")
                
            if len(all_rows) >= CHUNK_SIZE:
                print(f"  -> Writing Reddit chunk {chunk_idx} to disk...")
                df = pd.DataFrame(all_rows)
                out_path = os.path.join(OUTPUT_DIR, f"reddit_chunk_{chunk_idx}.parquet")
                df.to_parquet(out_path, index=False)
                print(f"  -> Written Reddit chunk {chunk_idx} ({len(all_rows):,} rows)")
                all_rows = []
                gc.collect()
                chunk_idx += 1
                
            if running_bytes >= REDDIT_TARGET_BYTES:
                print("  -> Target size reached. Stopping Reddit download.")
                break

    if all_rows:
        print(f"  -> Writing final Reddit chunk {chunk_idx} to disk...")
        df = pd.DataFrame(all_rows)
        out_path = os.path.join(OUTPUT_DIR, f"reddit_chunk_{chunk_idx}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"  -> Written final Reddit chunk {chunk_idx} ({len(all_rows):,} rows)")

    print(f"Reddit: {total_rows:,} rows, ~{running_bytes / 1e9:.2f} GB total")
    return total_rows, running_bytes

if __name__ == "__main__":
    # Clean up any partial chunks from previous failed (OOM) runs
    for f in Path(OUTPUT_DIR).glob("tweets_chunk_*.parquet"):
        f.unlink()
    for f in Path(OUTPUT_DIR).glob("reddit_chunk_*.parquet"):
        f.unlink()
        
    print("Starting data acquisition...\n")
    tweet_rows, tweet_bytes = pull_tweets()
    reddit_rows, reddit_bytes = pull_reddit()

    total_gb = (tweet_bytes + reddit_bytes) / 1e9
    print("\n=== Phase 3 summary ===")
    print(f"Tweets:  {tweet_rows:,} rows, ~{tweet_bytes/1e9:.2f} GB -> data/raw/tweets_chunk_*.parquet")
    print(f"Reddit:  {reddit_rows:,} rows, ~{reddit_bytes/1e9:.2f} GB -> data/raw/reddit_chunk_*.parquet")
    print(f"Combined: ~{total_gb:.2f} GB")
    print("\nRecord these exact numbers in PROGRESS_LOG.md.")
