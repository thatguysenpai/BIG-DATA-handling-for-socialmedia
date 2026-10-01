"""Mandatory scalability experiment at 25%, 50% and 100% of the data.

Runs the full analytics pipeline at each size (after an unrecorded warm-up so JVM
start-up cost does not penalise the first run) and records execution time,
throughput (records/s) and MongoDB query response time. Produces charts.

    python scalability_experiment.py
"""
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import config
from spark_session import get_spark
from analytics_job import run_analytics


def make_charts(df, ing):
    config.FIG_DIR.mkdir(parents=True, exist_ok=True)
    x = [f"{int(f*100)}%" for f in df.dataset_fraction]
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.2))
    ax[0].bar(x, df.total_seconds, color="#1f77b4"); ax[0].set_title("Analytics execution time")
    ax[0].set_ylabel("seconds")
    ax[1].plot(x, df.rows_per_second, marker="o", color="#2ca02c"); ax[1].set_title("Analytics throughput")
    ax[1].set_ylabel("records / second"); ax[1].set_ylim(bottom=0)
    ax[2].bar(x, df.mongo_count_query_seconds, color="#d62728"); ax[2].set_title("MongoDB count query response")
    ax[2].set_ylabel("seconds")
    for a in ax:
        a.set_xlabel("share of dataset"); a.grid(alpha=.3)
    fig.tight_layout(); fig.savefig(config.FIG_DIR / "scalability_analytics.png", dpi=160); plt.close(fig)
    if ing is not None:
        xi = [f"{int(f*100)}%" for f in ing.dataset_fraction]
        fig, ax = plt.subplots(1, 2, figsize=(10, 4.2))
        ax[0].bar(xi, ing.cumulative_seconds, color="#9467bd"); ax[0].set_title("Cumulative ingestion time")
        ax[0].set_ylabel("seconds")
        ax[1].plot(xi, ing.cumulative_rows_per_sec, marker="o", color="#ff7f0e"); ax[1].set_title("Ingestion throughput")
        ax[1].set_ylabel("records / second"); ax[1].set_ylim(bottom=0)
        for a in ax:
            a.set_xlabel("share of dataset"); a.grid(alpha=.3)
        fig.tight_layout(); fig.savefig(config.FIG_DIR / "scalability_ingestion.png", dpi=160); plt.close(fig)


def main():
    spark = get_spark("scalability-experiment")
    print("Warm-up run at 5% (not recorded)...")
    run_analytics(spark, 0.05)
    rows = []
    for f in config.SCALE_FRACTIONS:
        print(f"\n=== Analytics at {int(f*100)}% ===")
        m = run_analytics(spark, f, write_results=(f == 1.0))
        rows.append(m); print(pd.Series(m).to_string())
    df = pd.DataFrame(rows)
    df.to_csv(config.RESULTS_DIR / "scalability_metrics.csv", index=False)
    ing_path = config.RESULTS_DIR / "ingestion_metrics.csv"
    make_charts(df, pd.read_csv(ing_path) if ing_path.exists() else None)
    print("\nSaved results/scalability_metrics.csv and results/figures/scalability_*.png")
    spark.stop()


if __name__ == "__main__":
    main()
