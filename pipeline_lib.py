"""Shared pipeline logic: schema normalisation, MongoDB I/O, cleaning,
sentiment scoring, aggregation and trend detection."""
import time

import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T

import config

# ---------------------------------------------------------------- schema ----
POSTS_SCHEMA = T.StructType([
    T.StructField("post_id", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("created_ts", T.TimestampType()),
    T.StructField("community", T.StringType()),
    T.StructField("orig_sentiment", T.StringType()),
    T.StructField("orig_score", T.DoubleType()),
    T.StructField("sample_key", T.DoubleType()),
])

TEXT_CANDS = ["text", "content", "body", "full_text", "tweet", "comment"]
TIME_CANDS = ["created_at", "created_utc", "date", "timestamp", "time", "datetime", "created"]
ID_CANDS = ["id", "tweet_id", "comment_id", "id_str"]
LABEL_CANDS = ["sentiment_label", "label", "sentiment"]
SCORE_CANDS = ["sentiment_score"]
SUBREDDIT_CANDS = ["subreddit", "subreddit_name_prefixed"]


def pick(cols, cands):
    low = {c.lower(): c for c in cols}
    for c in cands:
        if c in low:
            return low[c]
    return None


# ------------------------------------------------------ normalisation (Spark) ----
def _to_timestamp(df, col_name):
    dtype = dict(df.dtypes)[col_name]
    c = F.col(col_name)
    if dtype in ("timestamp", "date", "timestamp_ntz"):
        return c.cast("timestamp")
    if dtype in ("bigint", "int", "double", "float", "smallint", "tinyint") or dtype.startswith("decimal"):
        v = c.cast("double")   # epoch in s / ms / us / ns (ns arrives via nanosAsLong)
        secs = (F.when(v > 1e17, v / 1e9).when(v > 1e14, v / 1e6)
                 .when(v > 1e11, v / 1e3).otherwise(v))
        return F.timestamp_seconds(secs)
    s = c.cast("string")
    return F.coalesce(
        F.to_timestamp(s),
        F.to_timestamp(F.regexp_replace(s, r"^[A-Za-z]{3}\s+", ""), "MMM dd HH:mm:ss Z yyyy"),
        F.timestamp_seconds(s.cast("double")),
    )


def normalize(df, source):
    """Map a raw tweet or reddit DataFrame onto the unified POSTS_SCHEMA."""
    cols = df.columns
    text_c, time_c = pick(cols, TEXT_CANDS), pick(cols, TIME_CANDS)
    if text_c is None or time_c is None:
        raise ValueError(
            f"[{source}] cannot find text/time columns. Columns present: {cols}. "
            f"Add the right names to TEXT_CANDS / TIME_CANDS in pipeline_lib.py")
    id_c = pick(cols, ID_CANDS)
    post_id = (F.concat(F.lit(source + "-"), F.col(id_c).cast("string")) if id_c
               else F.concat(F.lit(source + "-"), F.monotonically_increasing_id().cast("string")))
    label_c = pick(cols, LABEL_CANDS) if source == "twitter" else None
    score_c = pick(cols, SCORE_CANDS) if source == "twitter" else None
    sub_c = pick(cols, SUBREDDIT_CANDS) if source == "reddit" else None
    out = df.select(
        post_id.alias("post_id"),
        F.lit(source).alias("source"),
        F.col(text_c).cast("string").alias("text"),
        _to_timestamp(df, time_c).alias("created_ts"),
        (F.regexp_replace(F.col(sub_c).cast("string"), "^r/", "") if sub_c else F.lit(None).cast("string")).alias("community"),
        (F.lower(F.col(label_c).cast("string")) if label_c else F.lit(None).cast("string")).alias("orig_sentiment"),
        (F.col(score_c).cast("double") if score_c else F.lit(None).cast("double")).alias("orig_score"),
    )
    return out.filter(F.col("text").isNotNull() & (F.length("text") > 0) & F.col("created_ts").isNotNull())


def load_all_sources(spark):
    tw = list(config.RAW_DIR.glob("tweets_chunk_*.parquet"))
    rd = list(config.RAW_DIR.glob("reddit_chunk_*.parquet"))
    if not tw or not rd:
        raise FileNotFoundError(f"Need tweets_chunk_*.parquet and reddit_chunk_*.parquet in {config.RAW_DIR}")
    tdf = spark.read.parquet(str(config.RAW_DIR / "tweets_chunk_*.parquet"))
    rdf = spark.read.parquet(str(config.RAW_DIR / "reddit_chunk_*.parquet"))
    print("Tweet columns :", tdf.columns)
    print("Reddit columns:", rdf.columns)
    u = normalize(tdf, "twitter").unionByName(normalize(rdf, "reddit"))
    return u.withColumn("sample_key", F.rand(seed=42))


# ------------------------------------------------------------ Mongo I/O ----
def mongo_client():
    from pymongo import MongoClient
    return MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)


def write_posts(df, collection=config.COL_RAW):
    if config.OFFLINE:
        df.write.mode("append").parquet(str(config.OFFLINE_STORE / collection))
        return
    (df.write.format("mongodb").mode("append")
       .option("connection.uri", config.MONGO_URI)
       .option("database", config.MONGO_DB)
       .option("collection", collection)
       .option("maxBatchSize", "5000")
       .option("ordered", "false")
       .save())


def read_posts(spark, fraction=1.0, collection=config.COL_RAW):
    if config.OFFLINE:
        df = spark.read.parquet(str(config.OFFLINE_STORE / collection))
    else:
        df = (spark.read.format("mongodb").schema(POSTS_SCHEMA)
              .option("connection.uri", config.MONGO_URI)
              .option("database", config.MONGO_DB)
              .option("collection", collection).load())
    return df.filter(F.col("sample_key") < fraction)


def count_stored(collection=config.COL_RAW, fraction=None):
    """Document count straight from MongoDB (also used as a query-latency probe)."""
    if config.OFFLINE:
        import pyarrow.dataset as ds
        if not (config.OFFLINE_STORE / collection).exists():
            return 0
        t = ds.dataset(str(config.OFFLINE_STORE / collection)).to_table(columns=["sample_key"]).to_pandas()
        return int((t["sample_key"] < (fraction or 2)).sum())
    col = mongo_client()[config.MONGO_DB][collection]
    return col.count_documents({} if fraction is None else {"sample_key": {"$lt": fraction}})


# ------------------------------------------------------------ cleaning ----
URL_RE = r"https?://\S+|www\.\S+"
STOPWORDS = """a about above after again all also am an and any are as at be because been before being below
between both but by can could did do does doing down during each few for from further get got had has have having
he her here hers him his how i if in into is it its just like me more most my no nor not now of off on once only or
other our out over own same she should so some such than that the their them then there these they this those
through to too under until up us very was we were what when where which while who whom why will with would you your
yours amp rt via im dont thats cant didnt isnt youre ive one new people really going want know think make time
today https http com www co""".split()


def clean(df):
    df = df.withColumn("text_v", F.substring(F.regexp_replace("text", URL_RE, ""), 1, 1000))
    df = df.filter(~F.lower(F.trim("text_v")).isin("[deleted]", "[removed]", ""))
    tc = F.regexp_replace(F.lower(F.regexp_replace("text_v", r"@\w+", " ")), r"[^a-z\s]", " ")
    df = df.withColumn("text_clean", F.trim(F.regexp_replace(tc, r"\s+", " ")))
    df = df.filter(F.length("text_clean") >= 3)
    return df.dropDuplicates(["source", "text_clean"])


def _vader_row(text):
    global _ANALYZER
    if not text:
        return 0.0
    if _ANALYZER is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _ANALYZER = SentimentIntensityAnalyzer()
    return float(_ANALYZER.polarity_scores(text)["compound"])


_ANALYZER = None
# A plain Python UDF is used on purpose: it avoids the Arrow/JVM-flag
# incompatibilities that pandas UDFs hit on some Java 17/21 installs.
vader_compound = F.udf(_vader_row, T.DoubleType())


def score(df):
    lab = (F.when(F.col("compound") >= 0.05, "positive")
            .when(F.col("compound") <= -0.05, "negative").otherwise("neutral"))
    return (df.withColumn("compound", vader_compound("text_v"))
              .withColumn("sentiment", lab)
              .withColumn("day", F.to_date("created_ts"))
              .withColumn("month", F.date_trunc("month", "created_ts").cast("date")))


# ------------------------------------------------------------ analytics ----
def norm_label(c):
    """Map dataset labels to our three classes. DLT-Tweets uses bullish / bearish / neutral."""
    return (F.when(c.startswith("pos") | c.startswith("bull"), "positive")
             .when(c.startswith("neg") | c.startswith("bear"), "negative")
             .when(c.startswith("neu"), "neutral"))


def aggregate(scored):
    """Run every aggregation on the persisted scored DataFrame; returns pandas frames."""
    out = {}
    t = time.time()
    out["sentiment_by_source"] = scored.groupBy("source", "sentiment").count().toPandas()
    out["daily_sentiment"] = (scored.groupBy("day", "source").agg(
        F.count("*").alias("n"), F.round(F.avg("compound"), 4).alias("avg_compound"),
        F.sum((F.col("sentiment") == "positive").cast("int")).alias("n_pos"),
        F.sum((F.col("sentiment") == "negative").cast("int")).alias("n_neg"),
    ).toPandas().sort_values(["source", "day"]))
    out["monthly_totals"] = scored.groupBy("month", "source").count().withColumnRenamed("count", "n").toPandas()
    out["compound_hist"] = (scored.withColumn("bin", F.floor((F.col("compound") + 1) * 10) / 10 - 1)
                            .groupBy("source", "bin").count().toPandas().sort_values(["source", "bin"]))
    out["community_sentiment"] = (scored.filter(F.col("community").isNotNull())
        .groupBy("community").agg(F.count("*").alias("n"), F.round(F.avg("compound"), 4).alias("avg_compound"))
        .toPandas().sort_values("n", ascending=False))

    # keyword trends: distinct non-stopword tokens per post, counted per month
    stop = F.array(*[F.lit(w) for w in STOPWORDS])
    toks = F.array_except(F.split("text_clean", " "), stop)
    kw = (scored.select("month", "source", F.explode(toks).alias("token"))
                .filter(F.col("token").rlike("^[a-z]{4,}$"))
                .groupBy("month", "source", "token").count().filter("count >= 10"))
    out["keyword_monthly"] = kw.toPandas().rename(columns={"count": "n"})

    tags = F.regexp_extract_all(F.lower("text_v"), F.lit(r"#(\w+)"), F.lit(1))
    ht = (scored.select("month", "source", F.explode(F.array_distinct(tags)).alias("hashtag"))
                .filter(F.length("hashtag") >= 3).groupBy("month", "source", "hashtag").count().filter("count >= 5"))
    out["hashtag_monthly"] = ht.toPandas().rename(columns={"count": "n"})

    # validation against the dataset's own model-generated labels (tweets only)
    v = (scored.filter(F.col("orig_sentiment").isNotNull())
               .withColumn("orig", norm_label(F.col("orig_sentiment"))).filter(F.col("orig").isNotNull()))
    out["validation_confusion"] = v.groupBy("orig", "sentiment").count().toPandas()
    corr = v.filter(F.col("orig_score").isNotNull()).stat.corr("compound", "orig_score") if \
        v.filter(F.col("orig_score").isNotNull()).limit(1).count() else float("nan")
    out["validation_corr"] = pd.DataFrame({"metric": ["pearson_compound_vs_orig_score"], "value": [corr]})
    out["_agg_seconds"] = time.time() - t
    return out


def detect_trends(kw_monthly, monthly_totals, top_n=40, min_total=200, min_months=4, min_peak_count=30,
                  template_min_terms=8, template_tol=0.15):
    """Burst detection, computed separately for each source (Twitter and Reddit cover different years
    and volumes, so a combined monthly total would make a term look bursty just because one source
    dominates that month).

    A term trends in the month where its share of that source's posts (per 10,000) is furthest above
    its own average (z-score). To keep one-month spam and bot hashtags out of the ranking a term must
    also appear in at least `min_months` months, reach `min_peak_count` posts in its peak month and
    peak at >= 1.5x its normal level. Bursts made of repeated template posts are removed (see below). The top terms are taken per source so both are represented."""
    if kw_monthly.empty:
        return pd.DataFrame()
    sources = sorted(kw_monthly["source"].unique())
    parts = []
    for src in sources:
        tot = monthly_totals[monthly_totals["source"] == src].groupby("month")["n"].sum().sort_index()
        tot = tot[tot >= 0.3 * tot.median()]          # ignore partial edge months, they distort shares
        k = kw_monthly[kw_monthly["source"] == src].groupby(["token", "month"])["n"].sum().reset_index()
        piv = k.pivot(index="token", columns="month", values="n").reindex(columns=tot.index).fillna(0)
        piv = piv[(piv.sum(axis=1) >= min_total) & ((piv > 0).sum(axis=1) >= min_months)]
        if piv.empty:
            continue
        share = piv.div(tot, axis=1) * 10000
        mu, sd = share.mean(axis=1), share.std(axis=1).replace(0, float("nan"))
        peak_val, peak_month = share.max(axis=1), share.idxmax(axis=1)
        z = ((peak_val - mu) / sd).fillna(0)
        res = pd.DataFrame({"token": share.index, "source": src,
                            "total_mentions": piv.sum(axis=1).astype(int).values,
                            "months_present": (piv > 0).sum(axis=1).values,
                            "peak_month": peak_month.values,
                            "peak_count": piv.max(axis=1).astype(int).values,
                            "peak_per_10k": peak_val.round(2).values,
                            "avg_per_10k": mu.round(2).values,
                            "lift": (peak_val / mu).round(2).values,
                            "burst_z": z.round(2).values})
        res = res[(res["lift"] >= 1.5) & (res["peak_count"] >= min_peak_count)]
        # Template filter: many unrelated terms peaking in the same month with almost the same, large count
        # means one repeated post (bot or moderator boilerplate differing only in a username), not a topic.
        # "Large" = at least 1% of that month's posts, so ordinary low-count noise is never flagged.
        pc = res["peak_count"].to_numpy(dtype=float)
        month_total = tot.reindex(res["peak_month"]).to_numpy(dtype=float)
        same_month = res["peak_month"].to_numpy()[:, None] == res["peak_month"].to_numpy()[None, :]
        near = np.abs(pc[:, None] - pc[None, :]) <= template_tol * pc[:, None]
        neighbours = (same_month & near).sum(axis=1) - 1
        template = (neighbours >= template_min_terms) & (pc >= 0.01 * month_total)
        res = res[~template]
        parts.append(res.sort_values("burst_z", ascending=False).head(max(top_n // len(sources), 1)))
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts).sort_values("burst_z", ascending=False).reset_index(drop=True)


def validation_metrics(conf):
    if conf.empty:
        return pd.DataFrame()
    labels = ["positive", "neutral", "negative"]
    m = conf.pivot(index="orig", columns="sentiment", values="count").reindex(index=labels, columns=labels).fillna(0)
    total, correct = m.values.sum(), sum(m.loc[l, l] for l in labels)
    rows = [("accuracy", correct / total if total else 0.0)]
    f1s = []
    for l in labels:
        tp = m.loc[l, l]; fp = m[l].sum() - tp; fn = m.loc[l].sum() - tp
        p = tp / (tp + fp) if tp + fp else 0.0; r = tp / (tp + fn) if tp + fn else 0.0
        f = 2 * p * r / (p + r) if p + r else 0.0
        f1s.append(f); rows += [(f"precision_{l}", p), (f"recall_{l}", r), (f"f1_{l}", f)]
    rows.append(("macro_f1", sum(f1s) / 3))
    rows += [("n_labelled_posts", float(total)),
             ("classes_present_in_reference_labels", float(int((m.sum(axis=1) > 0).sum())))]
    return pd.DataFrame(rows, columns=["metric", "value"])
