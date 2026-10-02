# COEN542 Group 4: Scalable Sentiment and Trend Analytics Platform

Social Media Analytics. Kafka (streaming ingestion) -> MongoDB (storage) -> PySpark (processing) -> Streamlit (dashboard).

Data: English tweets (HuggingFace `ExponentialScience/DLT-Tweets`) and Reddit comments (HuggingFace `HuggingFaceGECLM/REDDIT_comments`, r/askscience): about 1.1 GB of Parquet, 6.14 million posts. Architecture diagram: `docs/architecture.png`.

## Repository layout

| File | Purpose |
|---|---|
| `config.py` | Paths, MongoDB/Kafka settings, collection names, connector version, scale fractions. Every script imports it. |
| `spark_session.py` | The one Spark session factory (local[4], 7 GB driver, 64 shuffle partitions, Mongo connector 10.4.0). |
| `pipeline_lib.py` | Shared logic: schema normalisation, MongoDB I/O, cleaning, VADER scoring, aggregation, trend detection, validation metrics. |
| `acquire_data.py` | Streams both HuggingFace datasets into `data/raw/*.parquet`. |
| `ingest_bulk.py` | Spark bulk load into MongoDB `posts_raw` in three slices (25/50/100%); writes `results/ingestion_metrics.csv`. |
| `ingest_stream.py` | Kafka path: `produce` to topic `social_media_raw`, `consume` into MongoDB `posts_stream`; writes `results/kafka_metrics.csv`. |
| `analytics_job.py` | Clean, de-duplicate, score, aggregate, detect trends; results to `results/*.csv` and MongoDB `agg_*` collections. |
| `scalability_experiment.py` | The mandatory 25/50/100% experiment (calls `analytics_job.py`) plus charts in `results/figures/`. |
| `dashboard.py` | Streamlit dashboard (reads `agg_*` from MongoDB, falls back to `results/*.csv`). |
| `make_report.py` | Builds `report/COEN542_Group4_Report.docx` from the real results. |
| `tools/` | `make_architecture_diagram.py`, `make_synthetic_data.py` (offline tests), `inspect_labels.py`, `refresh_trends.py`. |
| `poc/` | Proof-of-concept scripts from the first stage (see `poc/README.txt`). |
| `docs/` | `architecture.png`, `wslconfig.example`, `HANDOFF_ARCHIVE.md` (archived planning notes). |
| `PROGRESS_LOG.md`, `SETUP.md` | Engineering log and machine setup. |

## Run order (project folder, venv active; setup in `SETUP.md`)

    pip install -r requirements.txt
    docker compose up -d
    python acquire_data.py                       # skip if data/raw already holds the chunks
    python ingest_bulk.py                        # MongoDB posts_raw + results/ingestion_metrics.csv
    python ingest_stream.py produce --n 200000   # Kafka producer
    python ingest_stream.py consume              # Kafka -> MongoDB posts_stream
    python scalability_experiment.py             # warm-up, then 25/50/100%; the final run writes all analytics results
    streamlit run dashboard.py
    python make_report.py                        # report/COEN542_Group4_Report.docx
    python tools/make_architecture_diagram.py    # only if the diagram changes

Memory (12 GB host, WSL2 capped at 7 GB): before `ingest_bulk.py` and `scalability_experiment.py` run
`docker stop kafka mongo-express`; before `ingest_stream.py` run `docker start kafka`. Close other heavy programs during the experiment.

## Dataset

The raw data (about 1.1 GB) is **not stored in Git**. `acquire_data.py` rebuilds it from the two public HuggingFace datasets.
Exact copy used for the reported results: `<ADD DOWNLOAD LINK HERE>`  (put the `data/raw/` folder at the project root).

## Results (from `results/`)

| Size | Records | After cleaning | Analytics time | Records/s | Mongo count query |
|---|---|---|---|---|---|
| 25% | 1,537,287 | 1,208,276 | 158.8 s | 9,679 | 1.4 s |
| 50% | 3,073,348 | 2,381,956 | 296.8 s | 10,356 | 3.2 s |
| 100% | 6,144,625 | 4,689,671 | 671.6 s | 9,149 | 13.1 s |

Bulk ingestion of all 6.14 M documents: 115 s (53,318 docs/s). Kafka path (200,000 messages): producer 6,427 records/s, consumer into MongoDB 20,677 records/s.

## Design notes for the oral examination

- `sample_key`: a fixed-seed random number stored on every document, so `sample_key < 0.25` is a repeatable 25% sample served by an index.
- Ingestion is loaded in 3 slices, so the cumulative time after each slice is the 25/50/100% ingestion time.
- Dedupe runs after cleaning, so retweets and bot repeats do not inflate counts.
- VADER thresholds: compound >= 0.05 positive, <= -0.05 negative, else neutral.
- DLT-Tweets labels are model-generated, not human labels. In the ingested sample the usable labels contain only one class, so they cannot
  measure VADER's accuracy (see `PROGRESS_LOG.md`, Entry 13). The report states this.
- Trend detection runs per source: monthly share per 10,000 posts, burst z-score, lift >= 1.5, a term must appear in >= 4 months and reach
  30 posts in its peak month, partial edge months are ignored, and bursts made of repeated template posts are removed.
- Spark runs in local mode (4 cores): it measures growth on fixed resources, not cluster scale-out. Kafka has one broker: no replication.
- Main bottleneck: the VADER Python UDF (about 84% of the full run is load + clean + score).
# bigdata-project-
