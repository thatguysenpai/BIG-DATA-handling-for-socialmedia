"""Bulk ingestion of all chunks (tweets + reddit) into MongoDB (collection config.COL_RAW).

    python ingest_bulk.py

Data is loaded in three cumulative slices (sample_key 0-0.25, 0.25-0.5, 0.5-1.0),
so the cumulative time after each slice is the ingestion time for 25%, 50% and
100% of the dataset, while the whole run costs a single full load.
Metrics go to results/ingestion_metrics.csv.
"""
import time
import pandas as pd
from pyspark.sql import functions as F

import config
from spark_session import get_spark
import pipeline_lib as pl

SLICES = [(0.0, 0.25), (0.25, 0.50), (0.50, 1.00)]


def prepare_collection():
    if config.OFFLINE:
        import shutil
        shutil.rmtree(config.OFFLINE_STORE / config.COL_RAW, ignore_errors=True)
        return
    col = pl.mongo_client()[config.MONGO_DB][config.COL_RAW]
    col.drop()
    col.create_index("sample_key")
    col.create_index([("source", 1), ("created_ts", 1)])


def main():
    config.RESULTS_DIR.mkdir(exist_ok=True)
    spark = get_spark("ingest-bulk")
    prepare_collection()
    posts = pl.load_all_sources(spark)

    rows, cum_time, cum_rows = [], 0.0, 0
    for lo, hi in SLICES:
        before = pl.count_stored()
        t0 = time.time()
        pl.write_posts(posts.filter((F.col("sample_key") >= lo) & (F.col("sample_key") < hi)))
        dt = time.time() - t0
        n = pl.count_stored() - before
        cum_time += dt; cum_rows += n
        rows.append(dict(dataset_fraction=hi, slice_rows=n, slice_seconds=round(dt, 2),
                         slice_rows_per_sec=round(n / dt, 1), cumulative_rows=cum_rows,
                         cumulative_seconds=round(cum_time, 2),
                         cumulative_rows_per_sec=round(cum_rows / cum_time, 1)))
        print(f"[{int(hi*100):>3}%] +{n:,} rows in {dt:,.1f}s ({n/dt:,.0f} rows/s) | total {cum_rows:,} rows")
    pd.DataFrame(rows).to_csv(config.RESULTS_DIR / "ingestion_metrics.csv", index=False)
    print("\nSaved results/ingestion_metrics.csv")
    spark.stop()


if __name__ == "__main__":
    main()
