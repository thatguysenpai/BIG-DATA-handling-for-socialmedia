"""Recompute trend_terms and validation_metrics from the saved aggregates and store them again, without re-running Spark.

    python tools/refresh_trends.py

Needs results/keyword_monthly.csv, monthly_totals.csv and validation_confusion.csv (written by analytics_job.py).
Writes results/*.csv and, if MongoDB is up, the agg_trend_terms / agg_validation_metrics collections that the dashboard reads first.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pandas as pd
import config
import pipeline_lib as pl
from analytics_job import save_results

R = config.RESULTS_DIR
trends = pl.detect_trends(pd.read_csv(R / "keyword_monthly.csv"), pd.read_csv(R / "monthly_totals.csv"))
metrics = pl.validation_metrics(pd.read_csv(R / "validation_confusion.csv"))
save_results({"trend_terms": trends, "validation_metrics": metrics})
print(trends.groupby("source").size().to_string())
