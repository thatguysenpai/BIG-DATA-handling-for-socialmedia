# HANDOFF — COEN542 Big Data Analytics Project

Last updated: Phase 3 (dataset acquisition script written, not yet run)

## Course context

- **Course:** COEN542, Big Data Analytics — Department of Computer Engineering, Ahmadu Bello University, Zaria
- **Session:** 2025/2026 Second Semester
- **Deadline:** 28th September 2026
- **Marks:** 60 total (see marking scheme below)
- **Group:** Group 4. Reg numbers: U19CO1059 (Logic), U19CO1064, U19CO1025, U19CO1052, U19CO1015, U19CO1034, U21CO2037, U21CO2031, U19CO2005, U21CO2016
- **Submission mode:** Written report (15-20 pages), code repository, oral presentation (10-15 min) + live demo

## Domain and project choice

**Application area:** Social Media Analytics (#7 of 12 allowed domains). Confirmed no clash with other groups — Group 2 is doing Cybersecurity (CICIDS2017-based), different domain, so no approval conflict expected.

**Project title:** Scalable Sentiment and Trend Analytics Platform for Large-Scale Social Media Data

**Original one-page proposal** (already submitted/drafted, file: `COEN542_Project_Proposal.docx`) described:
- Data Sources → Kafka → MongoDB → PySpark → sentiment scoring/trend detection → Streamlit
- Originally named Sentiment140 + Pushshift Reddit dump as data sources — **this dataset choice has since evolved, see below.**

## Marking scheme (from official question paper, `COEN542_Project_2025_2026.pdf`)

| Criteria | Marks |
|---|---|
| Problem Definition, Objectives, Domain Understanding | 6 |
| Dataset Quality, Volume, Variety and Preparation | 5 |
| Big Data Architecture Design and Justification | 8 |
| Data Ingestion and Storage Implementation | 7 |
| Distributed Data Processing and Integration | 9 |
| Analytics and Quality of Findings | 6 |
| Scalability and Performance Evaluation | 6 |
| Interactive Visualization/Dashboard | 4 |
| Technical Report, Code Quality and Reproducibility | 4 |
| Presentation/Live Demonstration | 5 |
| **Total** | **60** |

Key takeaway driving several decisions below: **dataset volume is only 5/60 marks** — Processing (9), Architecture (8), and Ingestion/Storage (7) carry far more weight. This is why time/risk budget has consistently been protected for those stages rather than spent chasing bigger data.

## Hard requirements from the question paper

- Minimum ~1GB data (can combine datasets)
- Minimum 3 substantive Big Data technologies (Kafka + MongoDB + Spark = 3; Streamlit doesn't count toward this, it satisfies the separate visualization requirement)
- Mandatory scalability experiment: 3 data sizes (25%/50%/100%), at least one performance metric (execution time, throughput, memory, query response, records/sec), presented graphically
- Storage choice must be **justified** based on data characteristics, not just picked
- Dashboard must be interactive — static Matplotlib alone does not satisfy this
- Architecture diagram must show actual technologies at each stage with justification for each
- AI tool use is permitted for debugging/explanation/code improvement, but **students must be able to explain and defend every component during oral exam** — individual members can be marked down separately if they can't explain the group's implementation

## Hardware constraints (drives several config decisions)

- **OS:** Windows with WSL2, Docker Desktop installed and confirmed working
- **RAM: 12GB total** — this is tight for Kafka + MongoDB + Spark running concurrently
- Decisions made because of this:
  - `.wslconfig` caps WSL2 at 7GB so Docker can't starve Windows itself
  - Spark session configured with explicit conservative memory (`spark.driver.memory=3g`, `spark.executor.memory=2g`) rather than defaults
  - Guidance to avoid `.collect()`/`.toPandas()`/large `.show(N)` — these pull full results into the driver JVM heap and are the most likely OOM cause at this data size, not the Spark transform/shuffle stages (which spill to disk instead of crashing)
  - Guidance to stop the Mongo Express container during heavy Spark runs, restart after for inspection

## Architecture (current)

```
Data Sources (HuggingFace datasets)
  → Ingestion Layer (Kafka, single-broker, Docker/KRaft mode — no Zookeeper)
  → Storage Layer (MongoDB — chosen for schema-flexible JSON storage,
    since tweets and Reddit comments have different field structures)
  → Processing Layer (PySpark — clean, dedupe, tokenize, aggregate)
  → Analytics Layer (sentiment scoring + time-windowed keyword/hashtag trend detection)
  → Visualization Layer (Streamlit + Plotly)
```

## Dataset decision — full evolution (important context for report-writing later)

1. **Original proposal:** Sentiment140 (Twitter, 1.6M tweets, ~230MB) + a Pushshift Reddit subreddit dump.
2. **Risk flagged:** Pushshift's API has been heavily restricted since 2023; live scraping was judged too risky for a tight timeline.
3. **First revision:** Sentiment140 (unchanged) + **r/Antiwork Subreddit Dataset** (Kaggle, `pavellexyr/the-antiwork-subreddit-dataset`) — a dataset that was *already* scraped from Pushshift in the past and packaged as a static file, sidestepping the live-access risk. Confirmed to cover the subreddit's full history through Feb 2022; a related academic paper cites ~304K posts / ~12.1M comments in a similar window.
4. **Time-budget discussion:** User asked whether the project is doable in 12 hrs, then 20 hrs. Conclusion: 20 hrs is workable but tight (base phase estimate ~11-18 hrs with no major blockers). Agreed approach: build Sentiment140-only first, add Reddit back in only if time permits after Phase 8.
5. **Size discussion:** User asked whether to use "GBs of data" given high grading expectations. Reasoning applied: dataset volume is only 5/60 marks; going bigger risks eating time budget via OOM/slow-iteration debugging in the heavily-weighted stages instead. Initial target set at ~1.5GB given the 12GB RAM constraint.
6. **User reconsidered** the 1.5GB target directly. Correction applied: Spark is designed to spill to disk rather than crash when data exceeds RAM, as long as `.collect()`-style full materialization is avoided — so the real cost of going bigger is slower debug iteration, not correctness. **Revised target: ~3GB combined**, agreed as the ceiling before real OOM/thrash risk reappears on 12GB hardware.
7. **Final source pivot:** User explicitly wanted "something larger" and specifically asked for HuggingFace-native options (not Kaggle) — both a bigger labeled tweet dataset and a large Reddit dataset. Landed on:
   - **Tweets: `ExponentialScience/DLT-Tweets`** — 22M rows, ~6.8GB full / ~3.6GB compressed. Already has `sentiment_label`/`sentiment_score`/`confidence_level` fields (model-generated labels — NOT the same as Sentiment140's distant-supervision emoticon labels; this distinction should be stated explicitly and honestly in the report rather than implying human ground truth). Multi-language — must filter to English on load.
   - **Reddit: `HuggingFaceGECLM/REDDIT_comments`** — 100M-1B scale, English, split into per-subreddit parquet configs (~150-200MB each), so size is controllable by choosing how many subreddit configs to pull rather than downloading one giant file.
   - **This replaces Sentiment140 and the Kaggle r/Antiwork dataset entirely** — everything now sources from HuggingFace via the `datasets` library, no Kaggle account/API token needed, one consistent loading pattern for both sources.
   - **Caveat not yet resolved:** exact valid subreddit config names for `HuggingFaceGECLM/REDDIT_comments` beyond `gadgets` and `ifyoulikeblank` (confirmed via search) have not been fully verified. The Phase 3 script handles unknown configs by skipping with a warning rather than crashing.

## Files created so far (in project root unless noted)

| File | Purpose |
|---|---|
| `docker-compose.yml` | Kafka (KRaft mode, no Zookeeper) + MongoDB + Mongo Express UI |
| `requirements.txt` | Python deps (kafka-python, pymongo, pyspark, vaderSentiment, streamlit, plotly, pandas, tqdm) |
| `spark_session.py` | Shared Spark session factory — Mongo connector via Maven coords (NOT pip-installable), memory-tuned for 12GB host, notes on avoiding `.collect()` at ~3GB scale |
| `SETUP.md` | Full setup walkthrough: WSL2 filesystem-location gotcha, Python/Java version requirements, Docker verification steps, memory-constraint section |
| `wsl-config-reference/.wslconfig` | Caps WSL2 at 7GB memory, 4 processors, 2GB swap — goes on the **Windows** side at `C:\Users\<user>\.wslconfig`, not inside WSL |
| `phase3_get_data.py` | Pulls ~1.5GB English-filtered tweets from DLT-Tweets + ~1.5GB from REDDIT_comments configs, writes local parquet files, prints actual achieved sizes |

**Not yet done:** actually running `phase3_get_data.py` and recording real output sizes; Phase 4 (Kafka producer/consumer proof) onward.

## Phase plan (unchanged structure, agreed early on)

1. Understand project — ✅ done (proposal + question paper both reviewed)
2. Environment setup — ✅ files written, **not yet verified on the actual machine**
3. Get dataset — 🔶 script written, not yet run
4. Kafka producer→consumer proof — not started
5. Kafka→MongoDB proof — not started
6. MongoDB→PySpark proof — not started
7. Sentiment analysis (VADER, or reuse DLT-Tweets' pre-existing labels?) — not started, **open question**: since DLT-Tweets already has sentiment labels, decide whether VADER is applied fresh (for methodology marks / to have something to evaluate) or the existing labels are used as-is. Recommend applying VADER fresh to both datasets and treating DLT-Tweets' existing labels as a comparison/validation point — gives a genuine "evaluation metrics" story for the report.
8. Trend/keyword analysis — not started
9. Streamlit dashboard — not started
10. Scalability experiment (25/50/100%) — not started
11. Report writing — not started (only after system works, per plan)

## Open questions / things to verify before proceeding

- [ ] Confirm actual valid subreddit configs on `HuggingFaceGECLM/REDDIT_comments` (check the dataset's Files tab)
- [ ] Run `phase3_get_data.py`, record actual GB achieved — may need to raise/lower `TWEET_TARGET_BYTES`/`REDDIT_TARGET_BYTES` to land at 3GB combined
- [ ] Decide the VADER-vs-existing-labels question above
- [ ] Verify Docker/WSL2 setup actually works end-to-end on the real machine (SETUP.md steps have not been confirmed run yet — this handoff describes what was *planned*, not what was *tested*)
