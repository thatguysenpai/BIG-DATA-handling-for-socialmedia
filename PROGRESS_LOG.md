# PROGRESS LOG — COEN542 Big Data Project

Append one entry per work session/phase. Keep entries factual and specific
(exact numbers, exact errors, exact decisions) — this log is the raw
material for the report's Implementation/Experiments sections later, so
vague entries ("worked on Kafka, mostly fine") are much less useful than
specific ones ("Kafka container took 3 attempts; fixed by X").

Log format per entry:
- Date/session
- Phase
- What was attempted
- What actually happened (including errors, verbatim where possible)
- Decisions made and why
- What's next

---

## File name map (scripts were renamed in Entry 14)

Entries 1-13 mention the old script names as they were at the time. Current names:

| Old name | Current name |
|---|---|
| `phase3_get_data.py` | `acquire_data.py` |
| `phase4_kafka_producer.py` | `poc/poc_kafka_producer.py` |
| `phase5_kafka_to_mongo.py` | `poc/poc_kafka_to_mongo.py` |
| `phase6_mongodb_to_spark.py` | `poc/poc_mongodb_to_spark.py` |
| `phase7_full_ingestion.py` | `ingest_bulk.py` |
| `phase7a_kafka_stream.py` | `ingest_stream.py` |
| `phase8_analytics.py` | `analytics_job.py` |
| `phase9_scalability.py` | `scalability_experiment.py` |
| `phase10_dashboard.py` | `dashboard.py` |
| `HANDOFF.md` | `docs/HANDOFF_ARCHIVE.md` |
| `wslconfig` / `wsl-config-reference/.wslconfig` | `docs/wslconfig.example` |

"Phase N" in entry titles is the workflow stage, not a file name.

---

## Entry 1 — Planning phase (chat-based, pre-execution)

**Phase:** 1-3 (understanding project, environment planning, dataset decision)

**What was attempted:** Reviewed original one-page proposal against the
official COEN542 question paper. Worked through dataset choice across
several iterations (see HANDOFF.md "Dataset decision — full evolution"
section for the complete reasoning trail; the file is now `docs/HANDOFF_ARCHIVE.md`).

**What actually happened:** No code run yet at this point — this entry
covers planning/decision-making only. Files created: docker-compose.yml,
requirements.txt, spark_session.py, SETUP.md, .wslconfig,
phase3_get_data.py, HANDOFF.md.

**Decisions made and why:**
- Kafka in KRaft mode (no Zookeeper) — simpler single-broker setup
- MongoDB for schema-flexible storage — tweets and Reddit comments have
  different field structures, relational schema would need constant
  migration
- Target dataset size set at ~3GB combined after explicit reasoning about
  the 12GB RAM constraint vs. dataset-volume being only 5/60 marks (full
  reasoning in HANDOFF.md)
- Sourced both datasets from HuggingFace (ExponentialScience/DLT-Tweets +
  HuggingFaceGECLM/REDDIT_comments) rather than Kaggle, per explicit user
  request for HuggingFace-native, larger sources

**What's next:** Run phase3_get_data.py, record actual achieved sizes,
verify Docker/WSL2 setup actually works on the real machine.

---

## Entry 2 — Environment setup actually completed on real machine

**Phase:** 2 (environment setup) — now verified, not just planned

**What was attempted:** Ran through SETUP.md on the real machine
(Windows 11 + WSL2 Ubuntu, hostname AMK-LASS, 12GB RAM).

**What actually happened:**
- Initially worked from D:\ITACHI\Big-data-project (Windows drive) via
  PowerShell — corrected to move into WSL2 proper per the setup notes.
  `python3 -m venv` fails outright from Windows PowerShell (Python not
  installed there / Store alias issue) — this was the signal to switch
  into WSL.
- Copied project folder from `/mnt/d/ITACHI/Big-data-project` into
  `~/bigdata-project` using `cp -r`. This produced a nested duplicate
  folder (`cp -r source dest` copies the source directory itself into
  dest, not just its contents) — resolved with
  `mv -f Big-data-project/* .` then `rmdir Big-data-project`.
- `.wslconfig` file was present in the repo as `wslconfig` (no leading
  dot, not yet in its final location). Copied to
  `/mnt/c/Users/AMK_LASS/.wslconfig` (the actual Windows username,
  found via `ls /mnt/c/Users`), then `wsl --shutdown` from PowerShell
  and reopened WSL to apply the memory cap.
- `python3 --version` → 3.12.3 (within PySpark 3.5.1's supported range).
- `python3 -m venv venv` initially failed: "ensurepip is not available"
  — fixed with `sudo apt install python3.12-venv -y`, then venv creation
  succeeded on retry.
- `java -version` initially → command not found. Installed with
  `sudo apt install -y openjdk-17-jdk`. Confirmed working:
  `openjdk version "17.0.20.1" 2026-08-18`.
- `pip install -r requirements.txt` — succeeded cleanly, all packages
  installed with no errors (pyspark 3.5.1, pymongo, kafka-python,
  vaderSentiment, streamlit, plotly, pandas, etc. — full list in
  requirements.txt).

**Decisions made and why:** None new — this entry is execution/debugging
of already-agreed setup, not new decisions. Worth noting for the report's
"reproducibility" angle: the `cp -r` nested-folder issue and the
`python3.12-venv` package gap are both genuine reproducibility gotchas
worth mentioning in the report's setup/README section so examiners (or
a marker trying to reproduce the build) don't hit the same friction.

**What's next:** Docker verification (Kafka + MongoDB containers) —
`docker compose up -d` and the two verification commands in SETUP.md
section 4, then Phase 3 (run phase3_get_data.py).

---

## Entry 3 — Docker verified, Phase 2 fully complete

**Phase:** 2 (environment setup) — final confirmation

**What was attempted:** Started Docker containers (Kafka, MongoDB, Mongo
Express) and ran the two service-level verification checks from
SETUP.md section 4.

**What actually happened:**
- `docker compose up -d` initially failed: "The command 'docker' could
  not be found in this WSL 2 distro." Docker Desktop was installed on
  Windows but WSL2 integration for the Ubuntu distro was not enabled.
  Fixed via Docker Desktop → Settings → Resources → WSL Integration →
  enabled the toggle for the Ubuntu distro → Apply & Restart. Confirmed
  with `docker --version` → Docker version 29.6.2.
- `docker compose up -d` then succeeded: pulled mongo:7.0,
  mongo-express:1.0.2, apache/kafka:3.7.0 (image pulls took ~7-10 min
  combined — expect this on first run, it's a one-time cost). All three
  containers (kafka, mongodb, mongo-express) started successfully.
- Kafka verification (`kafka-topics.sh --list`) returned empty output,
  no error — broker is answering correctly (no topics exist yet, as
  expected before any producer has run).
- MongoDB verification (`mongosh ... db.adminCommand('ping')`) returned
  `{ ok: 1 }` — confirmed reachable with the admin credentials from
  docker-compose.yml.

**Decisions made and why:** None new. Worth noting for the report: the
Docker Desktop WSL2 integration toggle being off-by-default is a real
gotcha for anyone reproducing this setup, distinct from the .wslconfig
memory cap (which is a separate, also-necessary step). Both should be
mentioned in the report's reproducibility/setup section.

**What's next:** Phase 3 — run `phase3_get_data.py` to pull and size the
two HuggingFace datasets. The script's Reddit-loading approach has since
been corrected (see Entry 4) — it originally used the wrong API pattern
entirely.

---

## Entry 4 — Corrected Reddit dataset loading pattern (caught before running)

**Phase:** 3 (dataset acquisition) — pre-execution correction

**What was attempted:** Verified the `HuggingFaceGECLM/REDDIT_comments`
usage in `phase3_get_data.py` against real examples before running the
script against 3GB of downloads, since the original config names
(`AskReddit`, `worldnews`) were flagged as unverified placeholders.

**What actually happened:** Found real working code samples (a
HuggingFace Space using this exact dataset) showing the correct API
pattern is `load_dataset("HuggingFaceGECLM/REDDIT_comments",
split=<subreddit_name>, streaming=True)` — the subreddit is passed via
the `split` parameter, NOT as a second positional config argument like
the original script did. Confirmed valid subreddit split names from
real usage and file listings: `gadgets`, `ifyoulikeblank`, `askscience`,
`gaming`, `technology`, `todayilearned`, `programming`, `tifu` (all
lowercase).

**Decisions made and why:** Rewrote `pull_reddit()` in
phase3_get_data.py to use `split=subreddit` with
`REDDIT_SUBREDDITS = ["askscience", "gaming", "technology",
"todayilearned", "programming"]`. Chose these five for thematic breadth
(distinct trend-analysis stories) and confirmed file-listing evidence of
real volume (technology alone has 15+ parquet shards at ~200MB each).

**What's next:** Actually run `phase3_get_data.py` and record real
output — row counts, GB achieved per source, total combined GB, any
runtime errors encountered.

---

## Entry 5 — Missing `datasets` package in requirements.txt (caught at runtime)

**Phase:** 3 (dataset acquisition) — dependency fix

**What was attempted:** Ran `python phase3_get_data.py`.

**What actually happened:** `ModuleNotFoundError: No module named 'datasets'`.
requirements.txt was originally written during the Kaggle-sourcing plan
(pymongo, pyspark, kafka-python, etc.) and was never updated when the
project pivoted to HuggingFace's `datasets` library for data acquisition
— a real gap between a decision made in chat and the file that should
have reflected it.

**Decisions made and why:** Installed directly with `pip install datasets`
(resolved to datasets==5.0.1), then added `datasets==5.0.1` to
requirements.txt so a fresh clone of the repo won't hit the same error.
Worth a line in the report's reproducibility discussion: dependency
files can drift from actual decisions mid-project, and checking
`pip freeze` against requirements.txt periodically is cheap insurance.

**What's next:** Re-run `python phase3_get_data.py` for real this time.

---

## Entry 6 — Phase 3 script run in progress, appeared to stall mid-run

**Phase:** 3 (dataset acquisition) — first real execution, in progress

**What was attempted:** Ran `python phase3_get_data.py` after fixing the
missing `datasets` package (Entry 5). Script began streaming
ExponentialScience/DLT-Tweets successfully.

**What actually happened:** Progressed cleanly through English-filtered
tweets: 2,950,000 rows processed, ~1.10GB accumulated (against a
1.5GB / TWEET_TARGET_BYTES target), printing a progress line every
50,000 rows as designed. Progress appeared to stall at exactly that
point. Diagnosed via Windows Task Manager (opening a second WSL terminal
to run `top` was itself likely a contributing factor, competing for the
7GB WSL memory cap): VmmemWSL showed 8.2% CPU and 65.8 MB/s real disk
I/O — i.e. the process was still doing genuine work, not frozen/dead.
System-wide memory was at 83%. Advised closing the second terminal
(freeing RAM back up) and waiting a few minutes before concluding it's
truly stuck, since low-CPU-with-real-disk-I/O is a normal pattern for
network-streaming workloads, not necessarily a hang.

**Decisions made and why:** No code changes yet — this is an
in-progress diagnostic, not yet resolved. If it turns out to be a
genuine hang (zero disk/CPU activity sustained for several more
minutes), next steps would be: Ctrl+C to stop it, add more frequent
flush/logging (e.g. every 10,000 rows instead of 50,000) to get finer
visibility into where exactly it stalls, and consider whether streaming
from HuggingFace without an HF_TOKEN (a warning was shown: "sending
unauthenticated requests... set HF_TOKEN to enable higher rate limits")
is causing rate-limit-induced slowdowns partway through a shard.

**What's next:** Confirm whether the script actually resumed progress
after closing the second terminal, or whether it needs to be
interrupted and restarted with better diagnostics / an HF_TOKEN set.
**Session handed off to a different assistant (Qwen) at this point** —
see HANDOFF.md for the full context package.

---

## Entry 7 — Relocated project from WSL-native filesystem to D: drive

**Phase:** infrastructure/environment adjustment (spans all phases going forward)

**What was attempted:** User reported concern about disk space (initially
believed 1GB free on C:, later confirmed via `df -h` to be 8.5GB free on
C:, 79GB free on D:). Data folder was already at 1.1GB from the earlier
Phase 3 run. Decided to move the entire project from
`~/bigdata-project` (WSL2 native ext4 filesystem) to
`/mnt/d/ITACHI/bigdata-project` (Windows D: drive, accessed via WSL's
cross-filesystem mount) for simplicity, trading some I/O performance for
more headroom and less operational complexity.

**What actually happened:** The move was completed. Entry 8 records the project at
`/mnt/d/ITACHI/bigdata-project` with `data/raw/` intact (1.1 GB).

**Decisions made and why:** Chose whole-project relocation over the
initially-proposed narrower fix (symlinking only the `data/raw` folder
to D: while keeping code/venv on WSL-native storage) for simplicity,
given how much friction had already been caused by having files split
across multiple locations (D:\ITACHI Windows copy vs ~/bigdata-project
WSL copy vs VS Code pointing at the wrong one, earlier in this session).
To mitigate the known performance risk of Spark's shuffle/spill temp
files crossing the Windows/Linux filesystem boundary, added
`spark.local.dir=/tmp/spark-tmp` to spark_session.py — this keeps
Spark's actual write-heavy temp I/O on WSL's native filesystem even
though the project directory itself now lives on D:. Venv must be
rebuilt fresh at the new location (not copied) since venvs bake in
absolute paths.

**What's next:** Confirm the move completed, venv rebuilt and working,
old ~/bigdata-project copy removed to avoid future confusion about which
copy is authoritative. Then resume Phase 3 (re-run phase3_get_data.py
for the Reddit portion, since the working directory changed and OUTPUT_DIR
is a relative path that will now write to the new location).

---

## Entry 8 — Major sync-up: discovered Phase 4-7 already built and mostly run; project relocated to D:

**Phase:** 4, 5, 6 (confirmed run successfully), 7 (written, not yet run)

**What was attempted:** After relocating the project from
`~/bigdata-project` (WSL native) to `/mnt/d/ITACHI/bigdata-project`
(D: drive, for storage space reasons — see Entry 7), discovered four
previously-unrecorded scripts already existed: `phase4_kafka_producer.py`,
`phase5_kafka_to_mongo.py`, `phase6_mongodb_to_spark.py`,
`phase7_full_ingestion.py`. These represent real work done outside this
session's direct tracking (likely during/after the Qwen handoff).

**What actually happened:**
- Confirmed `data/raw/` now contains 65 tweet chunks (~7.5MB each) + 59
  Reddit chunks (~9-13MB each) = **1.1GB total actual combined size**,
  well under the ~3GB target originally planned, but still clears the
  question paper's explicit "preferably at least 1GB" requirement.
  `phase3_get_data.py` was evidently modified at some point to write
  chunked parquet files (`*_chunk_N.parquet`) rather than single files —
  this differs from the version on record before the Qwen handoff.
- User confirmed phases 4, 5, 6 ran successfully. Phase 4 sent 500
  sample tweet rows to Kafka topic `social_media_raw`. Phase 5 consumed
  from Kafka into MongoDB collection `raw_tweets`. Phase 6 read that
  collection via PySpark and grouped by `sentiment_label` (using
  DLT-Tweets' pre-existing labels, not VADER — consistent with the open
  question flagged earlier). Phase 7 (full ingestion of all chunks +
  scalability sampling at 0.25/0.5/1.0, writing to MongoDB collection
  `full_tweets`, measuring execution time and throughput) has NOT been
  run yet.
- **Decision: proceed with the 1.1GB dataset as-is**, given time already
  spent on setup friction and that dataset volume is only 5/60 marks.

**Inconsistencies found, not yet resolved:**
- Mongo Spark connector version drift: `spark_session.py` uses
  `mongo-spark-connector_2.12:10.3.0`; phase4-7 scripts use `10.4.0`.
  Should standardize on one version.
- Phase4-7 each build their own inline `SparkSession` rather than using
  the shared `spark_session.py` factory (which has the `/tmp` local-dir
  fix and tuned memory settings). Worth consolidating for code-quality
  marks and to avoid the config drift above happening again.
- MongoDB collection naming is inconsistent: `raw_tweets` (phase5) vs
  `full_tweets` (phase7, which actually contains BOTH tweets and Reddit
  data despite the name). Worth renaming for clarity before the report
  (e.g. `raw_posts` / `cleaned_posts` style, matching common practice
  seen in the comparison repo reviewed earlier).

**What's next:** Run `phase7_full_ingestion.py` at fraction=1.0 first as
a full-pipeline sanity check, then re-run at 0.25 and 0.5 (editing the
`main(fraction=...)` call each time, or better — modify the script to
accept a command-line argument and log results to a CSV across all
three runs) to actually produce the mandatory scalability experiment
data. Consider resolving the connector-version and shared-session
inconsistencies before or alongside this.

---

## Entry 9 — Shared-code refactor adopted (config, one Spark factory, one library)

**Phase:** 7-10 foundation

**What was attempted:** Replace the per-script inline Spark sessions and hard-coded settings (the inconsistencies listed in Entry 8)
with shared modules, and rebuild the ingestion and analytics scripts on top of them.

**What actually happened:** The repository now has `config.py` (paths, MongoDB and Kafka settings, collection names, connector version,
scale fractions), `spark_session.py` (one factory: local[4], 7 GB driver, 64 shuffle partitions, `spark.local.dir` on WSL-native disk,
`spark.sql.legacy.parquet.nanosAsLong=true` because pandas writes nanosecond timestamps that Spark 3.5 cannot read as timestamps) and
`pipeline_lib.py` (normalisation, MongoDB I/O, cleaning, VADER scoring, aggregation, trend detection, validation metrics).
`docker-compose.yml` now caps Kafka heap at 768 MB and the MongoDB WiredTiger cache at 1.5 GB.

**Decisions made and why:**
- One connector version everywhere: `mongo-spark-connector_2.12:10.4.0` (resolves the 10.3.0 / 10.4.0 drift from Entry 8).
- One unified document schema for both sources: post_id, source, text, created_ts, community, orig_sentiment, orig_score, sample_key.
- Collections renamed to `posts_raw` (bulk), `posts_stream` (Kafka) and `agg_*` (analytics results), replacing `raw_tweets` / `full_tweets`
  (resolves the naming problem from Entry 8; `full_tweets` actually held both sources).
- `sample_key` (fixed-seed random in [0,1), indexed) gives repeatable 25/50/100% samples through an indexed range query.
- Duplicates are removed after cleaning so retweets and bot repeats do not inflate counts.

**What's next:** Run the bulk ingestion with metrics, then the Kafka path, then the scalability experiment.

---

## Entry 10 — Bulk ingestion into MongoDB (numbers from results/ingestion_metrics.csv)

**Phase:** 7 (ingestion and storage)

**What actually happened:** `ingest_bulk.py` loaded all chunks into `posts_raw` in three slices of `sample_key`:

| Dataset share | Documents (cumulative) | Cumulative time | Throughput (cumulative) |
|---|---|---|---|
| 25% | 1,537,287 | 32.83 s | 46,830 docs/s |
| 50% | 3,073,348 | 66.21 s | 46,417 docs/s |
| 100% | 6,144,625 | 115.24 s | 53,318 docs/s |

The last slice (3,071,277 documents) took 49.03 s, 62,638 docs/s. Four times the data took 3.5 times the time, so fixed per-write
overhead is amortised over larger writes.

**What's next:** Streaming path through Kafka.

---

## Entry 11 — Kafka streaming path (numbers from results/kafka_metrics.csv)

**Phase:** 7a

**What actually happened:** `ingest_stream.py produce --n 200000` sent 200,000 messages in 31.12 s (6,427 records/s).
`ingest_stream.py consume` wrote the same 200,000 into `posts_stream` in 9.67 s (20,677 records/s, measured from first to last message).

**Decisions made and why:** The single Python producer is the limit (about 3x slower than the consumer), which is why the full
6.1 million documents use the Spark bulk path and the Kafka path is demonstrated at 200,000 messages. The 500 raw-schema messages sent by
the proof-of-concept producer share the topic name `social_media_raw`; the consumer now skips any message lacking the unified-schema
fields instead of crashing on it (Entry 14).

**What's next:** Analytics and the scalability experiment.

---

## Entry 12 — Analytics and scalability experiment (numbers from results/scalability_metrics.csv)

**Phase:** 8-9

| Size | Records in | After cleaning | Load+clean+score | Aggregation | Total | Records/s | Mongo count query |
|---|---|---|---|---|---|---|---|
| 25% | 1,537,287 | 1,208,276 | 131.3 s | 26.1 s | 158.8 s | 9,679 | 1.425 s |
| 50% | 3,073,348 | 2,381,956 | 241.6 s | 52.0 s | 296.8 s | 10,356 | 3.208 s |
| 100% | 6,144,625 | 4,689,671 | 566.3 s | 92.3 s | 671.6 s | 9,149 | 13.066 s |

**What it shows:**
- Four times the data took 4.23 times as long, and throughput stayed within about 8% of 9,700 records/s: close to linear scaling on fixed resources.
- Load + clean + score is about 84% of the full run. The VADER Python UDF is the dominant cost, so it is the main bottleneck.
- Cleaning and de-duplication removed 23.7% of the 6.14 million loaded rows at 100%.
- The indexed MongoDB count grew 9.2 times for 4 times the data, which is worse than linear. It is still about 2% of the run, but the
  cause (count over an index range scanning entries; memory pressure at 100%) should be understood before the oral.
- Sentiment shares after cleaning: Twitter 50.9% positive, 27.1% neutral, 22.0% negative (2,684,309 posts); Reddit 54.6% / 18.5% / 26.8%
  (2,005,362 posts).

---

## Entry 13 — Audit of the whole repository against its own results

**Phase:** review before submission

Findings, with the evidence for each:
1. **Sentiment validation is degenerate.** `validation_confusion.csv` has a single reference class: all 488,162 labelled tweets are
   "neutral" (VADER: 260,329 positive, 135,639 neutral, 92,194 negative). Accuracy 27.8% and macro-F1 0.145 therefore say nothing about
   VADER, and the Pearson correlation with the dataset score is 0.052. The report would have presented 27.8% as "agreement".
2. **Trend list was dominated by artefacts.** The top seven terms all had lift 87.0 and burst z 9.22 (hashtag-like spam tokens present in a
   single month), and trends were computed on combined Twitter+Reddit monthly totals although the sources cover different years
   (Reddit 2010-2015, Twitter 2013-2023). Separately, Reddit August 2015 contained about 1,700 near-identical moderator-bot comments
   that made unrelated words ("moderator", "denied", "paleontology") burst together.
3. **No `community_sentiment.csv`** exists in `results/`, so the per-subreddit chart never appears. Most likely the `subreddit` column is
   absent from the per-subreddit HuggingFace splits, leaving `community` empty.
4. **Report text contradicted the code:** report said Spark driver 3 GB and 16 shuffle partitions; `spark_session.py` uses 7 GB and 64.
5. **`config.py` had no content in the packed repository file** although every script imports it, and the proof-of-concept scripts
   `phase5_kafka_to_mongo.py` and `phase6_mongodb_to_spark.py` were not in the repository at all.
6. **Stale documents:** `SETUP.md` (3g/2g Spark, 1.5 GB target, WSL-native location, `wsl-config-reference/` path), `HANDOFF.md`
   (self-described as not updated), and a README written as a "drop" description.

---

## Entry 14 — Fixes applied and files renamed

**Phase:** clean-up

- Scripts renamed to descriptive, importable names (see the File name map at the top); every import, docstring, README command,
  report appendix and dashboard message was updated to match, and no `phaseN` file name remains in any code file.
- `config.py` rebuilt from every `config.X` reference in the code (paths, `mongodb://admin:admin123@localhost:27017/?authSource=admin`,
  database `bigdata_project`, `posts_raw`, `posts_stream`, `agg_`, topic `social_media_raw`, connector 10.4.0, fractions 0.25/0.5/1.0).
- `detect_trends` rewritten: per-source baselines; term must appear in >= 4 months and reach 30 posts in its peak month; repeated-template
  bursts removed; top terms taken per source. `results/trend_terms.csv` regenerated from the saved `keyword_monthly.csv` and
  `monthly_totals.csv` (Twitter now ranks pepe 2023-05, segwit 2017-07, blackrock 2023-06; Reddit ranks pluto 2015-07, rosetta 2014-11).
- `validation_metrics` now also records `n_labelled_posts` and `classes_present_in_reference_labels`; the report and dashboard detect a
  one-class reference and say so instead of presenting accuracy.
- Dashboard keyword normalisation now uses the totals of the selected sources only.
- `ingest_stream.py consume` skips messages that do not follow the unified schema.
- `acquire_data.py` writes to `config.RAW_DIR` (no longer depends on the working directory) and keeps the subreddit name on each Reddit row.
- Report text corrected (Spark 7 GB / 64 partitions, trend method, validation wording, conditional dashboard description, file names).
- `SETUP.md`, `README.md`, `poc/README.txt` rewritten; `HANDOFF.md` archived with a banner; `wslconfig` moved to `docs/wslconfig.example`.
- New tools: `tools/inspect_labels.py`, `tools/refresh_trends.py`.

---

## Entry 15 — Open items

- [ ] Run `python tools/inspect_labels.py` to see the real label values, then decide how to present sentiment quality (or label a small
      sample by hand and report real precision/recall).
- [ ] Run `python tools/refresh_trends.py` so MongoDB `agg_trend_terms` and `agg_validation_metrics` match the regenerated CSVs
      (the dashboard reads MongoDB first).
- [ ] The per-subreddit chart needs the `subreddit` column; the existing chunks do not have it. Either accept and omit it, or re-run
      `acquire_data.py` and the pipeline.
- [ ] Move `phase5_kafka_to_mongo.py` and `phase6_mongodb_to_spark.py` into `poc/` under the new names (`apply_cleanup.sh` does this).
- [ ] Fill in the member names and dashboard screenshots in the report, update the table of contents field, check the page count (15-20).
- [ ] Add the dataset download link to `README.md`.

