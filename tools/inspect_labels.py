"""Diagnose the sentiment-validation problem: which label columns and values do the tweet chunks actually contain?

    python tools/inspect_labels.py            # looks at the first 3 tweet chunks
    python tools/inspect_labels.py 10         # first 10 chunks
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pandas as pd
import config
import pipeline_lib as pl

n_chunks = int(sys.argv[1]) if len(sys.argv) > 1 else 3
files = sorted(config.RAW_DIR.glob("tweets_chunk_*.parquet"))[:n_chunks]
if not files:
    sys.exit(f"No tweets_chunk_*.parquet found in {config.RAW_DIR}")
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
print(f"{len(df):,} tweets from {len(files)} chunks. Columns: {list(df.columns)}\n")
for name, cands in (("label", pl.LABEL_CANDS), ("score", pl.SCORE_CANDS)):
    col = pl.pick(list(df.columns), cands)
    print(f"--- {name} column used by the pipeline: {col}")
    if col is None:
        continue
    print(f"null share: {df[col].isna().mean():.1%}")
    print(df[col].value_counts(dropna=False).head(10) if name == "label" else df[col].describe())
    print()
rd = sorted(config.RAW_DIR.glob("reddit_chunk_*.parquet"))[:1]
if rd:
    r = pd.read_parquet(rd[0])
    print("Reddit columns:", list(r.columns))
    print("'subreddit' column present:", "subreddit" in r.columns)
