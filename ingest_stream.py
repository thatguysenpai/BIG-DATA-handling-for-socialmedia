"""Streaming ingestion path  (parquet -> Kafka producer -> Kafka topic -> consumer -> MongoDB).

  python ingest_stream.py produce --n 200000
  python ingest_stream.py consume

Records per second for both sides go to results/kafka_metrics.csv.
Messages use the same unified schema as the bulk path and land in posts_stream.
"""
import argparse, json, random, time
import pandas as pd
import pyarrow.parquet as pq

import config
import pipeline_lib as pl

CSV = config.RESULTS_DIR / "kafka_metrics.csv"


def to_unified(pdf, source):
    cols = list(pdf.columns)
    text_c, time_c = pl.pick(cols, pl.TEXT_CANDS), pl.pick(cols, pl.TIME_CANDS)
    if text_c is None or time_c is None:
        raise ValueError(f"[{source}] text/time column not found in {cols}")
    id_c = pl.pick(cols, pl.ID_CANDS)
    lab_c = pl.pick(cols, pl.LABEL_CANDS) if source == "twitter" else None
    sc_c = pl.pick(cols, pl.SCORE_CANDS) if source == "twitter" else None
    sub_c = pl.pick(cols, pl.SUBREDDIT_CANDS) if source == "reddit" else None
    t = pdf[time_c]
    if pd.api.types.is_numeric_dtype(t):
        m = t.astype("float64")
        secs = m.where(m < 1e11, m / 1e3).where(m < 1e14, m / 1e6).where(m < 1e17, m / 1e9)
        ts = pd.to_datetime(secs, unit="s", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(t, utc=True, errors="coerce")
    out = pd.DataFrame({
        "post_id": source + "-" + (pdf[id_c].astype(str) if id_c else pd.Series(range(len(pdf))).astype(str)),
        "source": source, "text": pdf[text_c].astype(str), "created_ts": ts,
        "community": (pdf[sub_c].astype(str).str.replace(r"^r/", "", regex=True) if sub_c else None),
        "orig_sentiment": pdf[lab_c].astype(str).str.lower() if lab_c else None,
        "orig_score": pdf[sc_c].astype(float) if sc_c else None})
    return out.dropna(subset=["created_ts"])


def log(mode, n, secs):
    row = pd.DataFrame([dict(mode=mode, records=n, seconds=round(secs, 2), records_per_sec=round(n / secs, 1))])
    config.RESULTS_DIR.mkdir(exist_ok=True)
    row.to_csv(CSV, mode="a", header=not CSV.exists(), index=False)
    print(f"{mode}: {n:,} records in {secs:.1f}s = {n/secs:,.0f} records/s")


def produce(n):
    from kafka import KafkaProducer
    prod = KafkaProducer(bootstrap_servers=[config.KAFKA_BOOTSTRAP], linger_ms=20, batch_size=262144,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    files = [(f, "twitter") for f in sorted(config.RAW_DIR.glob("tweets_chunk_*.parquet"))[:20]] + \
            [(f, "reddit") for f in sorted(config.RAW_DIR.glob("reddit_chunk_*.parquet"))[:20]]
    random.Random(7).shuffle(files)
    sent, t0 = 0, time.time()
    for f, src in files:
        if sent >= n:
            break
        df = to_unified(pq.read_table(f).to_pandas(), src).head(n - sent)
        df["created_ts"] = df["created_ts"].astype(str)
        for rec in df.astype(object).where(df.notna(), None).to_dict("records"):
            prod.send(config.KAFKA_TOPIC, rec)
            sent += 1
        prod.flush()
        print(f"  produced {sent:,}/{n:,}")
    log("kafka_produce", sent, time.time() - t0)


def consume(idle_timeout_ms=20000, batch=5000):
    from kafka import KafkaConsumer
    cons = KafkaConsumer(config.KAFKA_TOPIC, bootstrap_servers=[config.KAFKA_BOOTSTRAP], auto_offset_reset="earliest",
                         group_id=f"mongo-sink-{int(time.time())}", consumer_timeout_ms=idle_timeout_ms,
                         value_deserializer=lambda b: json.loads(b.decode("utf-8")), max_poll_records=batch)
    col = pl.mongo_client()[config.MONGO_DB][config.COL_STREAM]
    col.drop(); col.create_index("sample_key")
    buf, total, t0, last = [], 0, time.time(), time.time()
    required = {"post_id", "source", "text", "created_ts"}
    skipped = 0
    for msg in cons:
        r = msg.value
        if not required.issubset(r):      # e.g. raw-schema messages left on the topic by the proof-of-concept producer
            skipped += 1
            continue
        r["created_ts"] = pd.Timestamp(r["created_ts"]).to_pydatetime()
        r["sample_key"] = random.random()
        buf.append(r); last = time.time()
        if len(buf) >= batch:
            col.insert_many(buf, ordered=False); total += len(buf); buf = []
    if buf:
        col.insert_many(buf, ordered=False); total += len(buf)
    if skipped:
        print(f"skipped {skipped:,} messages that do not follow the unified schema")
    log("kafka_consume_to_mongo", total, max(last - t0, 1e-3))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["produce", "consume"])
    ap.add_argument("--n", type=int, default=200000)
    a = ap.parse_args()
    produce(a.n) if a.mode == "produce" else consume()
