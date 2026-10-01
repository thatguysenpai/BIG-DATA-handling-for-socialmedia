"""Creates small fake tweet/reddit parquet chunks for OFFLINE testing only."""
import random, sys
from pathlib import Path
import pandas as pd
out = Path(sys.argv[1] if len(sys.argv) > 1 else "data/raw"); out.mkdir(parents=True, exist_ok=True)
random.seed(1)
pos = ["love this great project", "amazing #bitcoin rally today", "really happy with the new update"]
neg = ["terrible awful crash again", "this is bad #crypto scam", "hate the slow network"]
neu = ["the meeting is at noon", "reading about ethereum blocks", "new release notes published"]
def txt(): return random.choice(pos + neg + neu) + " " + random.choice(["", "http://x.co/a", "@bob", f"v{random.randint(0,9)}"])
def lab(t):
    b = t.split(" http")[0]
    return "positive" if any(b.startswith(p) for p in pos) else "negative" if any(b.startswith(n) for n in neg) else "neutral"
for i in range(3):
    rows = []
    for _ in range(4000):
        t = txt(); ts = pd.Timestamp("2021-01-01") + pd.Timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86000))
        rows.append(dict(id=random.randint(1, 10**9), text=t, created_at=ts, language="en",
                         sentiment_label=lab(t), sentiment_score=random.random(), confidence_level=random.random()))
    pd.DataFrame(rows).to_parquet(out / f"tweets_chunk_{i}.parquet")
    rows = []
    for _ in range(4000):
        t = txt(); ts = int(pd.Timestamp("2021-01-01").timestamp()) + random.randint(0, 400 * 86400)
        rows.append(dict(id=str(random.randint(1, 10**9)), content=t, created_utc=ts,
                         subreddit=random.choice(["gaming", "technology", "askscience"]), score=random.randint(0, 50)))
    pd.DataFrame(rows).to_parquet(out / f"reddit_chunk_{i}.parquet")
print("synthetic data written to", out)
