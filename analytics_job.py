"""Distributed analytics (clean -> dedupe -> VADER -> aggregate -> trends).

Reads from MongoDB, runs on Spark and returns timing metrics. `run_analytics`
is also called by scalability_experiment.py for the 25/50/100% experiment.

    python analytics_job.py --fraction 1.0
"""
import argparse
import time
import pandas as pd
from pyspark import StorageLevel

import config
from spark_session import get_spark
import pipeline_lib as pl


def run_analytics(spark, fraction, write_results=False):
    t_start = time.time()
    q0 = time.time()
    n_raw = pl.count_stored(fraction=fraction)        # indexed MongoDB query
    query_seconds = time.time() - q0

    t0 = time.time()
    scored = (pl.score(pl.clean(pl.read_posts(spark, fraction)))
              .select("source", "sentiment", "day", "month", "compound", "community",
                      "text_v", "text_clean", "orig_sentiment", "orig_score")
              .persist(StorageLevel.MEMORY_AND_DISK))
    n_clean = scored.count()                           # forces read + clean + dedupe + VADER
    t_pipeline = time.time() - t0

    aggs = pl.aggregate(scored)
    scored.unpersist()
    total = time.time() - t_start

    aggs["trend_terms"] = pl.detect_trends(aggs["keyword_monthly"], aggs["monthly_totals"])
    aggs["validation_metrics"] = pl.validation_metrics(aggs["validation_confusion"])
    metrics = dict(dataset_fraction=fraction, rows_in=n_raw, rows_after_cleaning=n_clean,
                   mongo_count_query_seconds=round(query_seconds, 3),
                   load_clean_score_seconds=round(t_pipeline, 2),
                   aggregate_seconds=round(aggs.pop("_agg_seconds"), 2),
                   total_seconds=round(total, 2),
                   rows_per_second=round(n_raw / total, 1))
    if write_results:
        save_results(aggs)
    return metrics


def save_results(aggs):
    config.RESULTS_DIR.mkdir(exist_ok=True)
    db = None if config.OFFLINE else pl.mongo_client()[config.MONGO_DB]
    for name, df in aggs.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        df.to_csv(config.RESULTS_DIR / f"{name}.csv", index=False)
        if db is not None:
            col = db[config.AGG_PREFIX + name]
            col.drop()
            d = df.copy()
            for c in d.columns:
                if d[c].map(lambda v: type(v).__name__ == "date").any():
                    d[c] = pd.to_datetime(d[c])
            recs = d.astype(object).where(d.notna(), None).to_dict("records")
            for i in range(0, len(recs), 20000):
                col.insert_many(recs[i:i + 20000])
    print(f"Saved aggregates to results/*.csv" + ("" if config.OFFLINE else " and MongoDB agg_* collections"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fraction", type=float, default=1.0)
    a = ap.parse_args()
    spark = get_spark("analytics-job")
    m = run_analytics(spark, a.fraction, write_results=True)
    print(pd.Series(m).to_string())
    spark.stop()
