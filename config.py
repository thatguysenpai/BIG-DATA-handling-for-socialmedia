"""Single source of truth: paths, connection strings, collection names, experiment settings.

Every script imports this module, so a name or setting is changed in exactly one place.
Environment variables override the defaults (useful for a different machine or for
offline testing without Docker):

    MONGO_URI          full MongoDB connection string
    SPARK_TMP          directory for Spark shuffle/spill files
    BIGDATA_OFFLINE=1  skip MongoDB and use Parquet files in results/offline_store (testing only)
"""
import os
from pathlib import Path

# ----------------------------------------------------------------- paths ----
ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "raw"              # tweets_chunk_*.parquet, reddit_chunk_*.parquet
RESULTS_DIR = ROOT / "results"
FIG_DIR = RESULTS_DIR / "figures"
DOCS_DIR = ROOT / "docs"
SPARK_TMP = os.environ.get("SPARK_TMP", "/tmp/spark-tmp")   # WSL-native disk, not /mnt/d

# --------------------------------------------------------------- MongoDB ----
# Credentials match docker-compose.yml (local development only).
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
MONGO_DB = "bigdata_project"
COL_RAW = "posts_raw"         # bulk ingestion (tweets + reddit, unified schema)
COL_STREAM = "posts_stream"   # Kafka consumer output
AGG_PREFIX = "agg_"           # analytics results: agg_daily_sentiment, agg_trend_terms, ...

# ----------------------------------------------------------------- Kafka ----
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "social_media_raw"

# ----------------------------------------------------------------- Spark ----
MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"

# ------------------------------------------------------------ experiments ----
SCALE_FRACTIONS = [0.25, 0.5, 1.0]

# --------------------------------------------------------- offline testing ----
OFFLINE = os.environ.get("BIGDATA_OFFLINE", "0") == "1"
OFFLINE_STORE = RESULTS_DIR / "offline_store"
