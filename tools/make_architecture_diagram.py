"""Draws docs/architecture.png (technology at every stage) for the report and slides."""
import sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = Path(__file__).resolve().parent.parent / "docs" / "architecture.png"
OUT.parent.mkdir(exist_ok=True)
fig, ax = plt.subplots(figsize=(15, 7.2)); ax.set_xlim(0, 15); ax.set_ylim(0, 7.2); ax.axis("off")

def box(x, y, w, h, title, sub, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.12", fc=color, ec="#333", lw=1.4))
    ax.text(x + w / 2, y + h - 0.32, title, ha="center", va="top", fontsize=10.5, weight="bold")
    ax.text(x + w / 2, y + h - 0.85, sub, ha="center", va="top", fontsize=8.6, linespacing=1.35)

def arrow(x0, y0, x1, y1, label="", rad=0.0):
    ax.add_patch(FancyArrowPatch((x0, y0), (x1, y1), arrowstyle="-|>", mutation_scale=16, lw=1.6,
                                 color="#222", connectionstyle=f"arc3,rad={rad}"))
    if label:
        ax.text((x0 + x1) / 2, (y0 + y1) / 2 + 0.22, label, ha="center", fontsize=8, style="italic")

ax.text(7.5, 6.95, "System architecture: scalable sentiment and trend analytics platform", ha="center", fontsize=14, weight="bold")
box(0.2, 3.3, 2.3, 2.6, "1. Data sources", "HuggingFace datasets\nDLT-Tweets (English)\nREDDIT_comments\n(5 subreddits)\n~1.1 GB Parquet chunks", "#e8f1fb")
box(2.9, 4.3, 2.9, 1.9, "2a. Streaming ingestion", "Apache Kafka 3.7\n(KRaft, 1 broker)\ntopic social_media_raw", "#fdeede")
box(2.9, 1.3, 2.9, 1.9, "2b. Bulk ingestion", "PySpark 3.5\nParquet reader\nschema normalisation", "#fdeede")
box(6.5, 2.6, 2.4, 2.6, "3. Storage", "MongoDB 7\nposts_raw, posts_stream\nagg_* result collections\nindexes: sample_key,\nsource + created_ts", "#e6f4ea")
box(9.5, 2.6, 2.7, 2.6, "4. Distributed processing", "PySpark (local[4])\nclean, deduplicate\nVADER sentiment UDF\nkeyword and hashtag counts\nburst detection", "#f3e8fb")
box(12.7, 2.6, 2.2, 2.6, "5. Visualisation", "Streamlit + Plotly\nfilters, keyword explorer,\nvalidation, scalability,\nlive MongoDB query", "#fff7d6")
arrow(2.5, 4.9, 2.9, 5.2); arrow(2.5, 4.3, 2.9, 2.4)
arrow(5.8, 5.0, 6.5, 4.5); ax.text(6.05, 5.15, "consumer insert_many", fontsize=8, style="italic", ha="center")
arrow(5.8, 2.4, 6.5, 3.2); ax.text(6.05, 2.0, "Mongo Spark connector 10.4", fontsize=8, style="italic", ha="center")
arrow(8.9, 4.3, 9.5, 4.3); arrow(9.5, 3.4, 8.9, 3.4)
ax.text(9.2, 4.6, "read + filter", fontsize=8, style="italic", ha="center"); ax.text(9.2, 3.0, "write agg_*", fontsize=8, style="italic", ha="center")
arrow(12.2, 3.9, 12.7, 3.9); ax.text(12.45, 4.2, "reads agg_*", fontsize=8, style="italic", ha="center")
ax.add_patch(FancyBboxPatch((0.2, 0.15), 14.6, 0.75, boxstyle="round,pad=0.03,rounding_size=0.1", fc="#f5f5f5", ec="#888"))
ax.text(7.5, 0.52, "Deployment: Docker Desktop containers (Kafka, MongoDB, Mongo Express) on WSL2; Spark and Streamlit run on the host in a Python 3 virtual environment; 12 GB RAM host, WSL2 capped at 7 GB.",
        ha="center", va="center", fontsize=8.8)
ax.text(7.5, 6.5, "Why: Kafka decouples producers from storage and absorbs bursts. MongoDB stores schema-flexible JSON posts with different fields per platform. Spark scales the heavy text processing across cores and spills to disk.",
        ha="center", fontsize=8.6, style="italic", wrap=True)
fig.savefig(OUT, dpi=170, bbox_inches="tight"); print("wrote", OUT)
