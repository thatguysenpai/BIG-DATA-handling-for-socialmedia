"""Interactive dashboard.   streamlit run dashboard.py

Reads the aggregates written by phase 8 from MongoDB (agg_* collections) and
falls back to results/*.csv if MongoDB is not reachable.
"""
import time
import pandas as pd
import plotly.express as px
import streamlit as st

import config

st.set_page_config(page_title="Social Media Analytics", layout="wide")
COLORS = {"positive": "#2ca02c", "neutral": "#7f7f7f", "negative": "#d62728"}


@st.cache_data(show_spinner=False)
def load(name):
    try:
        from pymongo import MongoClient
        db = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=1500)[config.MONGO_DB]
        docs = list(db[config.AGG_PREFIX + name].find({}, {"_id": 0}))
        if docs:
            return pd.DataFrame(docs), "MongoDB"
    except Exception:
        pass
    p = config.RESULTS_DIR / f"{name}.csv"
    return (pd.read_csv(p, keep_default_na=False, na_values=[""]), "CSV") if p.exists() else (pd.DataFrame(), "missing")


daily, origin = load("daily_sentiment")
if daily.empty:
    st.error("No results found. Run analytics_job.py (or scalability_experiment.py) first."); st.stop()
daily["day"] = pd.to_datetime(daily["day"])
bysrc, _ = load("sentiment_by_source"); kwm, _ = load("keyword_monthly"); hashm, _ = load("hashtag_monthly")
trend, _ = load("trend_terms"); conf, _ = load("validation_confusion"); vm, _ = load("validation_metrics")
hist, _ = load("compound_hist"); comm, _ = load("community_sentiment"); mtot, _ = load("monthly_totals")
scal, _ = load("scalability_metrics"); ing, _ = load("ingestion_metrics"); kaf, _ = load("kafka_metrics")

st.title("Scalable Sentiment and Trend Analytics Platform")
st.caption(f"COEN542 Group 4 | Pipeline: Kafka, MongoDB, PySpark, Streamlit | data source: {origin}")

st.sidebar.header("Filters")
sources = st.sidebar.multiselect("Source", sorted(daily["source"].unique()), default=sorted(daily["source"].unique()))
lo, hi = daily["day"].min().date(), daily["day"].max().date()
d0, d1 = st.sidebar.slider("Date range", lo, hi, (lo, hi))
smooth = st.sidebar.slider("Smoothing window (days)", 1, 30, 7)
f = daily[daily.source.isin(sources) & (daily.day.dt.date >= d0) & (daily.day.dt.date <= d1)]

total = int(f["n"].sum())
wavg = (f["avg_compound"] * f["n"]).sum() / max(total, 1)
c = st.columns(4)
c[0].metric("Posts analysed (after cleaning)", f"{total:,}")
c[1].metric("Average sentiment (VADER compound)", f"{wavg:+.3f}")
c[2].metric("Share positive", f"{f['n_pos'].sum() / max(total, 1):.1%}")
c[3].metric("Share negative", f"{f['n_neg'].sum() / max(total, 1):.1%}")

t1, t2, t3, t4, t5 = st.tabs(["Sentiment", "Trends", "Validation", "Scalability", "Live MongoDB query"])

with t1:
    a, b = st.columns(2)
    s = bysrc[bysrc.source.isin(sources)]
    a.plotly_chart(px.bar(s, x="source", y="count", color="sentiment", barmode="group", color_discrete_map=COLORS,
                          title="Sentiment class counts by source"), use_container_width=True)
    h = hist[hist.source.isin(sources)]
    b.plotly_chart(px.line(h, x="bin", y="count", color="source", title="Distribution of compound score"),
                   use_container_width=True)
    g = f.sort_values("day").copy()
    g["smoothed"] = g.groupby("source")["avg_compound"].transform(lambda x: x.rolling(smooth, min_periods=1).mean())
    st.plotly_chart(px.line(g, x="day", y="smoothed", color="source",
                            title=f"Daily average sentiment ({smooth}-day rolling mean)"), use_container_width=True)
    if not comm.empty:
        st.plotly_chart(px.bar(comm.head(15), x="community", y="avg_compound", hover_data=["n"],
                               title="Average sentiment by subreddit"), use_container_width=True)

with t2:
    st.subheader("Trending terms (burst detection)")
    st.caption("Burst z-score: how far a term's peak monthly share of posts sits above its own average. "
               "Lift is peak share divided by average share.")
    st.dataframe(trend, use_container_width=True, height=280)
    kwm["month"] = pd.to_datetime(kwm["month"])
    base = (mtot[mtot.source.isin(sources)].assign(month=lambda d: pd.to_datetime(d["month"]))
            .groupby("month")["n"].sum())
    terms = st.multiselect("Compare keywords over time", sorted(kwm["token"].unique()),
                           default=list(trend["token"].head(3)) if not trend.empty else [])
    per10k = st.checkbox("Normalise per 10,000 posts", value=True)
    if terms:
        k = kwm[kwm.token.isin(terms) & kwm.source.isin(sources)].groupby(["month", "token"])["n"].sum().reset_index()
        if per10k:
            k["n"] = k.apply(lambda r: r["n"] / base.get(r["month"], float("nan")) * 10000, axis=1)
        st.plotly_chart(px.line(k, x="month", y="n", color="token", markers=True,
                                title="Keyword frequency by month"), use_container_width=True)
    topn = st.slider("Top N", 5, 30, 15)
    a, b = st.columns(2)
    kt = kwm[kwm.source.isin(sources)].groupby("token")["n"].sum().nlargest(topn).reset_index()
    a.plotly_chart(px.bar(kt, x="n", y="token", orientation="h", title="Top keywords").update_yaxes(autorange="reversed"),
                   use_container_width=True)
    ht = hashm[hashm.source.isin(sources)].groupby("hashtag")["n"].sum().nlargest(topn).reset_index()
    b.plotly_chart(px.bar(ht, x="n", y="hashtag", orientation="h", title="Top hashtags").update_yaxes(autorange="reversed"),
                   use_container_width=True)

with t3:
    st.caption("VADER (rule-based, applied by us) compared with the dataset's own model-generated labels. "
               "These labels are not human ground truth, so agreement measures consistency between two methods.")
    if not conf.empty:
        if conf["orig"].nunique() < 2:
            st.warning("The dataset labels in the ingested data contain only one class (" + ", ".join(sorted(conf["orig"].unique()))
                       + "), so accuracy and F1 against them are not meaningful. The matrix shows how VADER classified those posts.")
        m = conf.pivot(index="orig", columns="sentiment", values="count").fillna(0)
        st.plotly_chart(px.imshow(m, text_auto=True, aspect="auto", color_continuous_scale="Blues",
                                  labels=dict(x="VADER", y="Dataset label", color="Posts"),
                                  title="Confusion matrix"), use_container_width=True)
        st.dataframe(vm, use_container_width=True)

with t4:
    if not scal.empty:
        scal["size"] = (scal.dataset_fraction * 100).astype(int).astype(str) + "%"
        a, b, c3 = st.columns(3)
        a.plotly_chart(px.bar(scal, x="size", y="total_seconds", title="Analytics time (s)"), use_container_width=True)
        b.plotly_chart(px.line(scal, x="size", y="rows_per_second", markers=True, title="Throughput (records/s)"),
                       use_container_width=True)
        c3.plotly_chart(px.bar(scal, x="size", y="mongo_count_query_seconds", title="MongoDB query response (s)"),
                        use_container_width=True)
        st.dataframe(scal, use_container_width=True)
    if not ing.empty:
        ing["size"] = (ing.dataset_fraction * 100).astype(int).astype(str) + "%"
        st.plotly_chart(px.bar(ing, x="size", y="cumulative_seconds", title="Cumulative ingestion time (s)"),
                        use_container_width=True)
    if not kaf.empty:
        st.subheader("Kafka streaming path"); st.dataframe(kaf, use_container_width=True)

with t5:
    st.write("Runs a live aggregation against the raw collection in MongoDB and times it.")
    frac = st.select_slider("Dataset fraction", options=[0.25, 0.5, 1.0], value=1.0)
    if st.button("Run query"):
        try:
            from pymongo import MongoClient
            col = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000)[config.MONGO_DB][config.COL_RAW]
            t = time.time()
            res = list(col.aggregate([{"$match": {"sample_key": {"$lt": frac}}},
                                      {"$group": {"_id": "$source", "posts": {"$sum": 1}}}]))
            st.success(f"Query answered in {time.time() - t:.2f} s")
            st.dataframe(pd.DataFrame(res).rename(columns={"_id": "source"}))
        except Exception as e:
            st.error(f"MongoDB not reachable: {e}")
