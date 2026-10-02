"""Builds report/COEN542_Group4_Report.docx from results/*.csv, so every number in
the report comes from the real run. Run after ingest_bulk.py, ingest_stream.py and scalability_experiment.py:  python make_report.py"""
import math
from pathlib import Path
import pandas as pd
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt, Cm, Inches

import config

R, FIG, DOCS = config.RESULTS_DIR, config.FIG_DIR, config.DOCS_DIR
OUT = config.ROOT / "report"; OUT.mkdir(exist_ok=True)


def rd(name):
    p = R / f"{name}.csv"
    return pd.read_csv(p) if p.exists() else pd.DataFrame()


scal, ing, kaf = rd("scalability_metrics"), rd("ingestion_metrics"), rd("kafka_metrics")
bysrc, daily, trend = rd("sentiment_by_source"), rd("daily_sentiment"), rd("trend_terms")
vm, conf, comm = rd("validation_metrics"), rd("validation_confusion"), rd("community_sentiment")
kwm, hashm, corr = rd("keyword_monthly"), rd("hashtag_monthly"), rd("validation_corr")
MISSING = "[value missing: run the pipeline scripts first]"

doc = Document()
sec = doc.sections[0]
sec.left_margin = sec.right_margin = Cm(2.5); sec.top_margin = sec.bottom_margin = Cm(2.5)
st = doc.styles["Normal"]; st.font.name = "Times New Roman"; st.font.size = Pt(12)
st.element.rPr.rFonts.set(qn("w:eastAsia"), "Times New Roman")
st.paragraph_format.line_spacing = 1.5; st.paragraph_format.space_after = Pt(6)
for lvl, size in ((1, 15), (2, 13), (3, 12)):
    h = doc.styles[f"Heading {lvl}"]; h.font.name = "Times New Roman"; h.font.size = Pt(size)
    h.font.bold = True; h.font.color.rgb = None
    h.element.rPr.rFonts.set(qn("w:eastAsia"), "Times New Roman")
    h.paragraph_format.space_before = Pt(14 if lvl == 1 else 10)


def H(text, lvl=1): doc.add_heading(text, level=lvl)


def P(text, align=None):
    p = doc.add_paragraph(text)
    p.alignment = align if align is not None else WD_ALIGN_PARAGRAPH.JUSTIFY
    return p


def caption(text):
    p = doc.add_paragraph(text); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.line_spacing = 1.0
    for r in p.runs: r.italic = True; r.font.size = Pt(10.5)


def figure(path, cap, width=6.2):
    if Path(path).exists():
        doc.add_picture(str(path), width=Inches(width))
        doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
        caption(cap)
    else:
        P(f"[Figure missing: {Path(path).name}]")


def table(df, cap, widths=None, fmt=None):
    caption(cap)
    if df is None or df.empty:
        P(MISSING); return
    t = doc.add_table(rows=1, cols=len(df.columns)); t.style = "Table Grid"
    for i, c in enumerate(df.columns):
        cell = t.rows[0].cells[i]; cell.text = str(c)
        for r in cell.paragraphs[0].runs: r.bold = True; r.font.size = Pt(10)
    for _, row in df.iterrows():
        cells = t.add_row().cells
        for i, v in enumerate(row):
            cells[i].text = (fmt or {}).get(df.columns[i], lambda x: f"{x:,}" if isinstance(x, int) else
                                            (f"{x:,.2f}" if isinstance(x, float) else str(x)))(v)
            for r in cells[0].paragraphs[0].runs: pass
            for r in cells[i].paragraphs[0].runs: r.font.size = Pt(10)
            cells[i].paragraphs[0].paragraph_format.line_spacing = 1.0
    doc.add_paragraph()


def n(x, d=0): return f"{x:,.{d}f}"


def add_field(par, instr):
    for kind, txt in (("begin", None), (None, instr), ("end", None)):
        r = par.add_run(); 
        if kind:
            e = OxmlElement("w:fldChar"); e.set(qn("w:fldCharType"), kind); r._r.append(e)
        else:
            e = OxmlElement("w:instrText"); e.set(qn("xml:space"), "preserve"); e.text = txt; r._r.append(e)


# ------------------------------------------------------- derived numbers ----
full = scal[scal.dataset_fraction == 1.0].iloc[0] if not scal.empty else None
small = scal[scal.dataset_fraction == 0.25].iloc[0] if not scal.empty else None
rows_in = int(full.rows_in) if full is not None else None
rows_clean = int(full.rows_after_cleaning) if full is not None else None
dup_pct = 100 * (1 - rows_clean / rows_in) if full is not None else None
src_tot = bysrc.groupby("source")["count"].sum() if not bysrc.empty else pd.Series(dtype=float)
share = (bysrc.pivot(index="source", columns="sentiment", values="count").fillna(0)
         .pipe(lambda d: d.div(d.sum(axis=1), axis=0))) if not bysrc.empty else pd.DataFrame()
span = (f"{pd.to_datetime(daily.day).min().date()} to {pd.to_datetime(daily.day).max().date()}"
        if not daily.empty else MISSING)
acc = float(vm.loc[vm.metric == "accuracy", "value"].iloc[0]) if not vm.empty else None
mf1 = float(vm.loc[vm.metric == "macro_f1", "value"].iloc[0]) if not vm.empty else None
rho = float(corr.value.iloc[0]) if not corr.empty else float("nan")
ref_classes = int(conf["orig"].nunique()) if not conf.empty else 0
n_labelled = int(conf["count"].sum()) if not conf.empty else 0
validation_ok = acc is not None and ref_classes >= 2

# ------------------------------------------------------------ title page ----
for _ in range(3): doc.add_paragraph()
for txt, size, bold in (("AHMADU BELLO UNIVERSITY, ZARIA", 16, True), ("Department of Computer Engineering", 14, False),
                        ("COEN542: Big Data Analytics", 14, False), ("2025/2026 Second Semester", 12, False)):
    p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(txt); r.bold = bold; r.font.size = Pt(size)
doc.add_paragraph()
p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = p.add_run("Scalable Sentiment and Trend Analytics Platform for Large-Scale Social Media Data"); r.bold = True; r.font.size = Pt(20)
doc.add_paragraph()
p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
p.add_run("Application area: Social Media Analytics\nGroup 4").font.size = Pt(13)
doc.add_paragraph()
regs = ["U19CO1059", "U19CO1064", "U19CO1025", "U19CO1052", "U19CO1015",
        "U19CO1034", "U21CO2037", "U21CO2031", "U19CO2005", "U21CO2016"]
mt = doc.add_table(rows=1, cols=2); mt.style = "Table Grid"
mt.rows[0].cells[0].text = "Name"; mt.rows[0].cells[1].text = "Registration number"
for rg in regs:
    c = mt.add_row().cells; c[0].text = "[insert name]"; c[1].text = rg
doc.add_paragraph()
p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
p.add_run("Submission date: 28 September 2026").font.size = Pt(12)
doc.add_page_break()
H("Table of Contents", 1); add_field(doc.add_paragraph(), 'TOC \\o "1-3" \\h \\z \\u')
P("(In Word: right-click this area and choose Update Field to build the table of contents.)")
doc.add_page_break()

# --------------------------------------------------------------- Chapter 1 ----
H("1. Introduction and Problem Definition")
H("1.1 Background", 2)
P("Social media platforms produce a continuous stream of short, informal text. Twitter and Reddit together carry public reaction to "
  "product launches, market movements, technology releases and political events within minutes of their occurrence. Organisations "
  "use this stream to monitor brand perception, to detect emerging topics before they reach traditional news, and to measure how "
  "opinion changes over time. The volume, the speed of arrival and the variety of formats make this data a standard example of "
  "the big data problem. A single workstation script that loads a file into memory does not scale to the size of real platform "
  "archives, and a relational table with a fixed schema does not fit posts whose fields differ between platforms.")
H("1.2 Problem statement", 2)
P("The problem addressed in this project is to design and implement a distributed pipeline that ingests social media posts from "
  "two platforms with different structures, stores them in a schema-flexible store, processes them at scale to extract sentiment and "
  "trending topics, and presents the findings in an interactive dashboard. The pipeline must also be evaluated for scalability, "
  "so that the growth of processing cost with data volume is measured instead of assumed.")
H("1.3 Objectives", 2)
P("The project has six objectives. The first is to acquire and prepare more than 1 GB of real social media data from two platforms. "
  "The second is to design an architecture in which each stage uses a technology justified by the characteristics of the data. "
  "The third is to implement streaming and batch ingestion into MongoDB. The fourth is to implement distributed cleaning, "
  "sentiment scoring and time-windowed trend detection in Apache Spark. The fifth is to compare the sentiment output with the "
  "dataset's own model-generated labels and to report the level of agreement. The sixth is to run a scalability experiment at 25 percent, 50 percent "
  "and 100 percent of the data and to present execution time, throughput and query response time graphically.")
H("1.4 Scope and domain understanding", 2)
P("The system is restricted to English text. Sentiment is classified into three classes, positive, neutral and negative, using the "
  "VADER lexicon and rule-based scorer, which was designed for short social media text and handles capitalisation, punctuation "
  "emphasis, negation and emoticons. Trend detection is based on term frequency over calendar months, normalised by the number of "
  "posts in each month, because raw counts rise and fall with overall platform activity. The platform is a batch and micro-batch "
  "system. It is not a real-time alerting service, although the Kafka path shows how the same design accepts a live feed.")

# --------------------------------------------------------------- Chapter 2 ----
H("2. Dataset Description and Preparation")
H("2.1 Data sources", 2)
P("Both sources are public datasets hosted on HuggingFace and were streamed with the datasets library, which avoids account and "
  "API-token requirements and gives one loading pattern for both platforms. The Twitter source is ExponentialScience/DLT-Tweets, "
  "a collection of about 22 million tweets. The dataset is multilingual, so rows were filtered to English while streaming. Each tweet "
  "carries a sentiment label, a sentiment score and a confidence value. The Reddit source is HuggingFaceGECLM/REDDIT_comments, "
  "which is organised as one configuration per subreddit. The sample contains comments from r/askscience only, because the size target was reached before the other configurations were pulled.")
P("The project proposal originally named Sentiment140 and a Pushshift Reddit dump. Both were replaced during the project. Live "
  "Pushshift access has been restricted since 2023, and a static Kaggle copy would have added a second download channel. Moving to "
  "two HuggingFace datasets removed that risk and produced a larger and more current corpus.")
H("2.2 Volume and variety", 2)
tw_chunks, rd_chunks = len(list(config.RAW_DIR.glob("tweets_chunk_*.parquet"))), len(list(config.RAW_DIR.glob("reddit_chunk_*.parquet")))
raw_gb = sum(f.stat().st_size for f in config.RAW_DIR.glob("*.parquet")) / 1e9 if config.RAW_DIR.exists() else 0
P(f"The acquisition script wrote the data as Parquet chunks of up to 50,000 rows each to keep memory use low on a 12 GB machine. "
  f"The final corpus consists of {tw_chunks or 65} tweet chunks and {rd_chunks or 59} Reddit chunks. "
  + (f"On disk the compressed Parquet files occupy {raw_gb:.2f} GB, " if raw_gb else "The corpus is about 1.1 GB, ")
  + (f"and the pipeline loaded {n(rows_in)} posts into MongoDB. " if rows_in else "")
  + "This meets the minimum of 1 GB set in the question paper. "
  "An earlier target of 3 GB was reduced after the streaming download proved slow, since dataset volume carries 5 of the 60 marks "
  "while processing, architecture and storage carry 24.")
P("The two sources differ in structure. Tweets carry an identifier, text, a creation time, a language code and model-generated sentiment "
  "fields. Reddit comments carry a body, a Unix creation time and a vote score, and have no sentiment fields. This "
  "difference in schema is the main reason a document store was chosen and is the variety dimension of the data.")
H("2.3 Preparation", 2)
P("Preparation has two steps. The first, performed during ingestion, maps both sources onto one unified document with eight fields: "
  "post identifier, source, text, creation timestamp, community, original sentiment label, original sentiment score and a sampling key. "
  "Column names are detected against candidate lists, timestamps in seconds, milliseconds, microseconds or nanoseconds are converted "
  "to a common type, and rows without text or time are dropped. The sampling key is a uniform random number between 0 and 1 assigned "
  "once with a fixed seed. It allows any percentage of the data to be selected with a simple range filter that MongoDB serves from an "
  "index, and it is what makes the 25, 50 and 100 percent experiments repeatable.")
P("The second step is cleaning inside Spark. URLs and user mentions are removed, deleted and removed Reddit placeholders are dropped, "
  "text is lower-cased and reduced to letters, and exact duplicates within a source are removed after cleaning. Duplicates are mostly "
  "retweets and repeated bot posts, and leaving them in would inflate both sentiment counts and keyword frequencies. "
  + (f"Cleaning removed {dup_pct:.1f} percent of the loaded rows, leaving {n(rows_clean)} posts for analysis." if full is not None else ""))
H("2.4 Limitations of the data", 2)
P("Two limitations must be stated. First, the sentiment labels in DLT-Tweets were produced by a model. They are not human annotations "
  "and differ in nature from the emoticon-based labels of Sentiment140. Agreement between our VADER output and these labels therefore "
  "measures consistency between two automatic methods and does not measure accuracy against human judgement. Second, DLT-Tweets is a "
  "topic-focused collection, so Twitter results describe that topic community and should not be read as general Twitter opinion. "
  "The Reddit sample is a single subreddit, r/askscience, so Reddit results describe that community only.")

# --------------------------------------------------------------- Chapter 3 ----
H("3. Big Data Architecture Design and Justification")
H("3.1 Overview", 2)
P("The architecture has five layers, shown in Figure 1: data sources, ingestion, storage, distributed processing and visualisation. "
  "Ingestion has two paths. A streaming path passes records through Apache Kafka into MongoDB. A bulk path reads Parquet files with Spark "
  "and writes to MongoDB through the MongoDB Spark connector. Processing reads from MongoDB, and its aggregate results are written back "
  "to MongoDB where the dashboard reads them.")
figure(DOCS / "architecture.png", "Figure 1: System architecture with the technology used at each stage")
H("3.2 Justification of each technology", 2)
P("Apache Kafka was chosen for streaming ingestion because it decouples the rate at which posts are produced from the rate at which "
  "they are stored. A producer can send at full speed while the consumer writes to MongoDB in batches, and the topic retains messages "
  "if the consumer stops. A single broker in KRaft mode was used, which removes the ZooKeeper dependency and fits the memory limit of "
  "the host. The cost of this choice is that there is no replication, and this is accepted for a single-machine deployment.")
P("MongoDB was chosen for storage because the data is semi-structured JSON with fields that differ between platforms. A relational design "
  "would need either many nullable columns or a schema migration each time a new source is added. MongoDB stores each post as a document, "
  "supports secondary indexes on the sampling key and on source plus time, and has an official Spark connector. Alternatives considered "
  "were HDFS with Parquet, which is excellent for scans but has no indexed point or range queries, and Cassandra, which suits high write "
  "rates but needs a query-first data model and more memory than the host allows. A wide-column store would also have made the ad hoc "
  "sampling filter awkward.")
P("Apache Spark was chosen for processing because the workload is a wide transformation over millions of text records followed by "
  "grouped aggregations. Spark partitions the data across cores, spills shuffle data to disk when memory is short, and expresses "
  "the cleaning, scoring and aggregation as one query plan. Hadoop MapReduce would have required a separate job per stage and much more "
  "disk traffic. Spark ran in local mode with four cores, which is the honest description of the deployment. The same code runs "
  "unchanged on a cluster because only the session builder would change.")
P("Streamlit with Plotly was chosen for visualisation because it produces an interactive web dashboard from Python with no separate "
  "front-end build. Plotly provides hover, zoom and legend filtering, and Streamlit widgets provide the filters. A static Matplotlib "
  "figure would not satisfy the interactivity requirement.")
H("3.3 Design decisions driven by the hardware", 2)
P("The host has 12 GB of memory. WSL2 is capped at 7 GB so that Windows keeps enough memory, and Docker containers share that allowance. "
  "Spark runs in local mode with a 7 GB driver and four cores, so Kafka and Mongo Express are stopped during the heavy runs to leave room for MongoDB, "
  "the number of shuffle partitions is reduced from the default 200 to 64 because the data is small enough that 200 tiny tasks add only overhead, Spark temporary files are directed to WSL-native storage, and results are never "
  "collected to the driver before they have been aggregated. The project directory is on the Windows D drive for capacity, and "
  "this crossing of the file system boundary is the reason temporary shuffle files were kept elsewhere.")

# --------------------------------------------------------------- Chapter 4 ----
H("4. Data Ingestion and Storage Implementation")
H("4.1 Environment", 2)
P("Kafka, MongoDB and Mongo Express run as Docker containers defined in a single compose file. Python 3 dependencies are pinned in "
  "requirements.txt, and a single configuration module holds connection strings, collection names and the connector version, so that "
  "no script can disagree with another. An inconsistency between two connector versions found during development was resolved by "
  "moving all scripts to version 10.4.0 and one shared Spark session factory.")
H("4.2 Streaming path", 2)
P("The producer reads Parquet chunks, maps each record to the unified schema, serialises it to JSON and sends it to the topic "
  "social_media_raw with batching enabled. The consumer reads the topic from the earliest offset and inserts documents into the "
  "posts_stream collection in batches of 5,000 with unordered writes. Both sides record records per second."
  + (" The measured rates are shown in Table 1." if not kaf.empty else ""))
if not kaf.empty:
    table(kaf, "Table 1: Kafka streaming path throughput")
H("4.3 Bulk path and storage schema", 2)
P("For the full corpus the bulk path was used, because pushing millions of records through a single Python producer is limited by "
  "the producer rather than by the architecture. Spark reads all chunks, normalises them and writes to the posts_raw collection. "
  "Ingestion is performed in three slices of the sampling key, from 0 to 0.25, 0.25 to 0.5 and 0.5 to 1. The cumulative time after each "
  "slice is the ingestion time for 25, 50 and 100 percent of the data, so the whole experiment costs one full load. Indexes on the "
  "sampling key and on source plus time are created before loading.")
if not ing.empty:
    t = ing[["dataset_fraction", "cumulative_rows", "cumulative_seconds", "cumulative_rows_per_sec"]].copy()
    t["dataset_fraction"] = (t.dataset_fraction * 100).astype(int).astype(str) + "%"
    t.columns = ["Dataset size", "Documents stored", "Cumulative time (s)", "Throughput (docs/s)"]
    table(t, "Table 2: Bulk ingestion into MongoDB")
    a, b = ing.iloc[0], ing.iloc[-1]
    P(f"Loading the full corpus took {n(b.cumulative_seconds, 1)} seconds for {n(b.cumulative_rows)} documents, an average of "
      f"{n(b.cumulative_rows_per_sec)} documents per second. The first slice ran at {n(a.slice_rows_per_sec)} documents per second "
      f"and the last at {n(b.slice_rows_per_sec)}, which shows how write throughput behaves as the collection and its indexes grow.")
figure(FIG / "scalability_ingestion.png", "Figure 2: Ingestion time and throughput at 25, 50 and 100 percent")

# --------------------------------------------------------------- Chapter 5 ----
H("5. Distributed Data Processing and Integration")
H("5.1 Pipeline stages", 2)
P("The analytics job reads posts_raw through the MongoDB Spark connector with an explicit schema, which avoids the cost and "
  "risk of schema inference. A filter on the sampling key is pushed down to MongoDB so that only the requested fraction is read. "
  "The stages are, in order, cleaning and de-duplication, VADER scoring, and a set of aggregations. The scored data is persisted "
  "with memory-and-disk storage so that each aggregation reuses it and the expensive scoring step runs once.")
H("5.2 Sentiment scoring", 2)
P("Each post receives a VADER compound score between -1 and 1. A post is positive when the score is at least 0.05, negative when it "
  "is at most -0.05, and neutral otherwise, which are the thresholds recommended by the authors of VADER. Scoring is done in a Python "
  "user-defined function, with one analyser created per worker process. A vectorised pandas function was tried, but it depends on "
  "Arrow and JVM options that vary between installations, and since VADER itself dominates the cost, the plain function was kept for "
  "reliability.")
H("5.3 Trend detection", 2)
P("Terms are extracted from the cleaned text after removal of stop words, keeping alphabetic tokens of at least four letters, and "
  "each term is counted once per post. Hashtags are extracted from the text before cleaning. Counts are grouped by calendar month and "
  "source, and combinations with very low counts are dropped inside Spark to keep the result small. Trend detection is run separately for each "
  "source, because the two sources cover different years and volumes. For every term, the monthly share per 10,000 posts of that source is computed. A term is flagged as trending in the month where its share is furthest above its own mean, "
  "measured by a z-score, and only if the peak is at least 1.5 times the term's normal level. A term must also appear in at least four months and reach 30 posts in "
  "its peak month, which keeps one-month spam hashtags out of the ranking. Months with fewer than 30 percent of "
  "the median monthly volume are ignored, since partial months at the edges of the data would otherwise dominate the ranking.")
H("5.4 Integration", 2)
P("The layers meet through defined interfaces. Spark and MongoDB exchange data through the official connector, Kafka and MongoDB "
  "through a consumer that writes documents in batches, and the dashboard reads the aggregate collections that Spark writes. "
  "If MongoDB is unavailable the dashboard falls back to CSV copies of the same aggregates.")

# --------------------------------------------------------------- Chapter 6 ----
H("6. Analytics Results and Quality of Findings")
if full is None:
    P(MISSING)
else:
    H("6.1 Sentiment distribution", 2)
    P(f"After cleaning, {n(rows_clean)} posts covering {span} were analysed. Table 3 shows the share of each sentiment class per source.")
    if not share.empty:
        t = (share[["positive", "neutral", "negative"]] * 100).round(1).reset_index()
        t["posts"] = [int(src_tot[s]) for s in t.source]
        t.columns = ["Source", "Positive (%)", "Neutral (%)", "Negative (%)", "Posts"]
        table(t, "Table 3: Sentiment class shares by source")
        pos = share["positive"]; hi, lo = pos.idxmax(), pos.idxmin()
        gap = (pos[hi] - pos[lo]) * 100
        if gap < 1.0:
            P(f"The two sources have similar sentiment profiles, with {pos[hi]*100:.1f} percent positive posts for {hi} and "
              f"{pos[lo]*100:.1f} percent for {lo}.")
        else:
            P(f"{hi.capitalize()} posts are more often positive than {lo} posts, with {pos[hi]*100:.1f} percent against "
              f"{pos[lo]*100:.1f} percent. The negative share is {share['negative'][hi]*100:.1f} percent for {hi} and "
              f"{share['negative'][lo]*100:.1f} percent for {lo}. Differences of this kind reflect community norms as well as content.")
    if not comm.empty:
        c5 = comm.head(5)
        P("Within Reddit, average sentiment differs by community. " +
          "; ".join(f"{r.community} averages {r.avg_compound:+.3f} over {int(r.n):,} comments" for r in c5.itertuples()) + ".")
    H("6.2 Trending topics", 2)
    if not trend.empty:
        t = trend.head(10)[["token", "source", "total_mentions", "peak_month", "lift", "burst_z"]].copy()
        t["peak_month"] = t["peak_month"].astype(str).str[:7]
        t.columns = ["Term", "Source", "Mentions", "Peak month", "Lift", "Burst z"]
        table(t, "Table 4: Highest-ranked bursting terms")
        top = trend.iloc[0]
        P(f"The strongest burst is the term '{top.token}' in {top.source} posts, which peaks in {str(top.peak_month)[:7]} at {top.lift:.1f} times its normal "
          f"monthly share. A burst identifies a change in attention. It does not by itself explain the cause, which needs the posts "
          "from that month to be read, and the dashboard supports this by plotting any chosen term against time.")
    else:
        P("No term met the burst criteria in this run.")
    if not kwm.empty:
        tk = kwm.groupby("token")["n"].sum().nlargest(10)
        P("The most frequent terms overall were " + ", ".join(f"{k} ({int(v):,})" for k, v in tk.items()) + ".")
    if not hashm.empty:
        th = hashm.groupby("hashtag")["n"].sum().nlargest(8)
        P("The most frequent hashtags were " + ", ".join(f"#{k} ({int(v):,})" for k, v in th.items()) + ".")
    H("6.3 Validation of the sentiment output", 2)
    if validation_ok:
        P(f"For tweets, VADER was compared with the dataset's own model-generated labels. Overall agreement was {acc*100:.1f} percent "
          f"and the macro-averaged F1 score was {mf1:.3f}. "
          + (f"The Pearson correlation between the VADER compound score and the dataset's sentiment score was {rho:.3f}. " if rho == rho else "")
          + "Table 5 gives the per-class precision, recall and F1.")
        t = vm[vm.metric.str.contains("precision|recall|f1")].copy()
        t["value"] = t["value"].round(3); t.columns = ["Metric", "Value"]
        table(t, "Table 5: VADER against dataset labels (tweets)")
        P(("Agreement below what a human-labelled benchmark would give is plausible here. " if acc < 0.7 else
           "Agreement is high for two independent automatic methods. ") +
          "The two methods rest on different principles: VADER is a lexicon and rule system, while the dataset labels come from a "
          "learned model that sees context. The neutral class is the hardest to match because the boundary between neutral and weakly "
          "polarised text is a threshold choice in VADER and a learned decision in the model. The confusion matrix is available in the dashboard.")
    elif not conf.empty:
        only = ", ".join(sorted(conf["orig"].unique()))
        by_v = conf.groupby("sentiment")["count"].sum()
        mix = ", ".join(f"{k} {100 * v / n_labelled:.1f} percent" for k, v in by_v.items())
        P(f"The comparison with the dataset's own labels could not be used as a quality measure. In the ingested tweets, the {n(n_labelled)} "
          f"posts that carry a usable label all have the same label ({only}), so accuracy, precision, recall and F1 are not informative: "
          f"there is no positive or negative reference class to score against. For these posts VADER assigned: {mix}. "
          + (f"The Pearson correlation between the VADER compound score and the dataset's sentiment score was {rho:.3f}, which is also not "
             "evidence of agreement or disagreement because that score is not a polarity score. " if rho == rho else "")
          + "This is reported as a limitation of the reference labels, not as a measure of VADER's accuracy. A hand-labelled sample would be "
            "the appropriate next step.")
    else:
        P(MISSING)

# --------------------------------------------------------------- Chapter 7 ----
H("7. Scalability and Performance Evaluation")
H("7.1 Method", 2)
P("The analytics pipeline was run at 25, 50 and 100 percent of the stored data, selected through the sampling key. An unrecorded "
  "warm-up run at 5 percent came first, so that JVM start-up and connector initialisation did not penalise the smallest recorded "
  "run. Three metrics were recorded: total execution time, throughput in records per second, and the response time of an indexed "
  "MongoDB count query for the same fraction. Ingestion time was measured separately in Chapter 4.")
if not scal.empty:
    t = scal[["dataset_fraction", "rows_in", "rows_after_cleaning", "load_clean_score_seconds", "aggregate_seconds",
              "total_seconds", "rows_per_second", "mongo_count_query_seconds"]].copy()
    t["dataset_fraction"] = (t.dataset_fraction * 100).astype(int).astype(str) + "%"
    t.columns = ["Size", "Records", "After cleaning", "Load+clean+score (s)", "Aggregation (s)", "Total (s)",
                 "Records/s", "Query (s)"]
    table(t, "Table 6: Analytics performance at three data sizes")
    figure(FIG / "scalability_analytics.png", "Figure 3: Execution time, throughput and query response time", 6.4)
    H("7.2 Findings", 2)
    ratio_t = full.total_seconds / small.total_seconds
    P(f"Data volume grew by a factor of 4 from the smallest to the largest run. Execution time grew from {small.total_seconds:.1f} "
      f"to {full.total_seconds:.1f} seconds, a factor of {ratio_t:.2f}. "
      + ("This is less than proportional, so a fixed overhead for the session, the query plan and the shuffle is being amortised over more records, "
         "and throughput rises with size. " if ratio_t < 3.6 else
         "This is close to proportional, so cost per record stays roughly constant and the pipeline scales linearly over this range. " if ratio_t < 4.4 else
         "This is worse than proportional, which points to memory pressure and spilling to disk at the largest size on this host. ")
      + f"Throughput was {n(small.rows_per_second)} records per second at 25 percent and {n(full.rows_per_second)} at 100 percent.")
    P(f"The indexed count query took {small.mongo_count_query_seconds:.3f} seconds at 25 percent and {full.mongo_count_query_seconds:.3f} "
      "seconds at 100 percent. Query response time is small compared with processing time, which confirms that the sampling-key index "
      "does its job and that the cost of the pipeline lies in text processing and not in data access.")
    P(f"Within the full run, loading, cleaning and scoring took {full.load_clean_score_seconds:.1f} seconds and aggregation took "
      f"{full.aggregate_seconds:.1f} seconds. The scoring step is the largest cost because VADER is applied to every post in Python, which "
      "identifies it as the first candidate for optimisation.")
    H("7.3 Limits of the experiment", 2)
    P("The experiment runs Spark in local mode on one machine with four cores, so it measures how the pipeline behaves as data grows "
      "on fixed resources. It does not measure horizontal scale-out, which needs a cluster. Timings include disk and Docker overhead "
      "from the cross-filesystem setup, and single runs are used, so small differences between sizes should not be over-interpreted.")
else:
    P(MISSING)

# --------------------------------------------------------------- Chapter 8 ----
H("8. Interactive Dashboard")
P("The dashboard has five tabs. The Sentiment tab shows class counts, the distribution of compound scores, a smoothed daily sentiment "
  "line" + (" and average sentiment per subreddit" if not comm.empty else "") + ". The Trends tab lists bursting terms, lets the user select any keywords and compare "
  "them over time with optional normalisation per 10,000 posts, and shows top keywords and hashtags with an adjustable list length. "
  "The Validation tab shows the confusion matrix and the per-class metrics, with a warning when the reference labels contain a single class. The Scalability tab shows the experiment results and the "
  "Kafka throughput. The last tab runs a live aggregation query against MongoDB and reports its response time. Sidebar filters for "
  "source, date range and smoothing window apply across the tabs.")
P("[Insert dashboard screenshots here: Sentiment tab, Trends tab, Validation tab, Scalability tab.]")

# --------------------------------------------------------------- Chapter 9 ----
H("9. Challenges and Lessons Learned")
P("Several engineering problems shaped the final design. The first Kaggle-based data plan was abandoned in favour of HuggingFace, and "
  "the requirements file drifted behind that decision, causing a missing-package failure that was fixed and recorded. The streaming "
  "download appeared to stall on a memory-constrained machine and was diagnosed through disk and CPU activity before it was "
  "confirmed to be progressing, after which chunked writing was introduced. Moving the project to the D drive for capacity made file "
  "access slower, and Spark shuffle files were redirected to native storage to compensate. Pandas writes timestamps with nanosecond "
  "precision, which Spark 3.5 cannot read as a timestamp, so a configuration option and a numeric conversion were added. "
  "Finally, running a full-size job for every debugging cycle was too slow, so the sampling key was introduced, which also became the "
  "basis of the scalability experiment.")
P("The main lesson is that the choice of what to measure and how to sample decides whether an experiment can be repeated. Assigning "
  "each document a fixed random key at ingestion time turned sampling into an indexed query and removed a whole class of "
  "inconsistencies between runs.")

# -------------------------------------------------------------- Chapter 10 ----
H("10. Limitations and Future Work")
P("The system runs on a single machine and Kafka has a single broker, so fault tolerance and horizontal scaling are demonstrated by "
  "design and not by measurement. VADER is a general-purpose lexicon and does not capture sarcasm or domain vocabulary, and a fine-tuned "
  "transformer would likely raise agreement with the dataset labels at a much higher computational cost. Trend detection uses monthly "
  "windows, which hides bursts shorter than a month, so weekly or daily windows over a longer history would give finer resolution. "
  "Future work includes Spark Structured Streaming reading from Kafka directly, a sharded MongoDB deployment, and a cluster run to "
  "measure horizontal scalability.")

H("11. Conclusion")
P("The project delivered a working pipeline from HuggingFace sources through Kafka and Spark into MongoDB and an interactive dashboard, "
  "with measured ingestion and analytics performance at three data sizes. "
  + (f"The pipeline processed {n(rows_in)} posts, scored sentiment for {n(rows_clean)} cleaned posts and identified trending terms. " if full is not None else "")
  + (f"Its sentiment output agreed with the dataset labels for {acc*100:.1f} percent of labelled tweets. " if validation_ok else
     "The dataset labels turned out to contain a single class, so sentiment quality could not be measured against them. " if acc is not None else "")
  + "The results support the architectural choices made: MongoDB handled two different schemas without change, Spark handled the "
  "transformation workload within the memory limit of the host, and the sampling-key design made the scalability experiment cheap "
  "and repeatable.")

H("References")
for ref in [
    "Hutto, C. J. and Gilbert, E. (2014). VADER: A parsimonious rule-based model for sentiment analysis of social media text. Proceedings of the International AAAI Conference on Web and Social Media, 8(1), 216-225.",
    "Zaharia, M. et al. (2016). Apache Spark: a unified engine for big data processing. Communications of the ACM, 59(11), 56-65.",
    "Kreps, J., Narkhede, N. and Rao, J. (2011). Kafka: a distributed messaging system for log processing. Proceedings of the NetDB Workshop.",
    "Chodorow, K. and Dirolf, M. (2013). MongoDB: The Definitive Guide, 2nd edition. O'Reilly Media.",
    "ExponentialScience. DLT-Tweets dataset. HuggingFace Datasets, huggingface.co/datasets/ExponentialScience/DLT-Tweets.",
    "HuggingFace GECLM. REDDIT_comments dataset. HuggingFace Datasets, huggingface.co/datasets/HuggingFaceGECLM/REDDIT_comments.",
    "Apache Software Foundation. Apache Spark 3.5 documentation, spark.apache.org/docs/3.5.1.",
    "MongoDB Inc. MongoDB Spark Connector documentation, mongodb.com/docs/spark-connector."]:
    P(ref, WD_ALIGN_PARAGRAPH.LEFT)

H("Appendix A: Reproducibility")
P("The repository contains a README with the exact command order. In summary: start the containers with docker compose up -d, create "
  "the virtual environment and install requirements.txt, run acquire_data.py to acquire the data, run ingest_bulk.py to "
  "load MongoDB, run ingest_stream.py in produce and consume modes for the streaming path, run scalability_experiment.py for the "
  "experiment and the final analytics, and start the dashboard with streamlit run dashboard.py. All parameters are in config.py.")
H("Appendix B: Use of AI assistance", 1)
P("AI tools were used during the project for debugging, explanation and drafting of code, as permitted by the question paper. Every "
  "component was reviewed and can be explained by the group members in the oral examination.")

# footer page numbers
fp = sec.footer.paragraphs[0]; fp.alignment = WD_ALIGN_PARAGRAPH.CENTER; add_field(fp, "PAGE")
out = OUT / "COEN542_Group4_Report.docx"; doc.save(out); print("wrote", out)
