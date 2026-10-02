# COEN542 Group 4: User Guide and Testing Guide

Project: Scalable Sentiment and Trend Analytics Platform for Large Scale Social Media Data

## 1. Purpose

This guide shows how to install the platform, run the pipeline from data acquisition to the dashboard, and check that each stage works. Section 6 contains numbered tests. Each test gives a command and the result a correct system produces, so a reader who has never seen the project can verify it.

The pipeline has six layers. Data comes from two HuggingFace datasets, English tweets from DLT-Tweets and Reddit comments from r/askscience. Kafka carries the streaming path and Spark carries the bulk path into MongoDB. Spark then cleans the text, scores sentiment with VADER and detects trending terms. A Streamlit dashboard shows the results. The full corpus has 6,144,625 posts and about 1.1 GB of Parquet files.

## 2. System Requirements

Operating system: Windows 10 or 11 with WSL2 and an Ubuntu distribution. All commands in this guide run inside the Ubuntu terminal, never in PowerShell or CMD. The only exception is the WSL memory step in Section 3.5, which uses PowerShell and says so.

Memory: 12 GB on the host. WSL2 is limited to 7 GB.

Disk: at least 5 GB free for the Parquet data, MongoDB storage and Spark temporary files. The Docker disk image grows on the system drive as MongoDB fills, so keep several gigabytes free there as well.

Python: version 3.10, 3.11 or 3.12. PySpark 3.5.1 does not run on Python 3.13.

Java: OpenJDK 17.

Docker Desktop with the WSL2 backend and integration enabled for the Ubuntu distribution.

Internet access is needed for the first Spark run, because Spark downloads the MongoDB connector, and for data acquisition.

## 3. Installation

### 3.1 Open Ubuntu and get the project

```
wsl
cd ~
git clone <repository-url> bigdata-project
cd bigdata-project
```

If the project is copied from another disk, check that the copy did not create a nested folder with the same name. The project may also stay on a Windows drive, for example /mnt/d/ITACHI/bigdata-project. Reads and writes are faster inside the Ubuntu home folder.

### 3.2 Create the Python environment

```
python3 --version
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create the environment at its final location. A copied environment does not work. If the version is 3.13, install Python 3.12 with sudo apt install python3.12 python3.12-venv and create the environment with python3.12.

### 3.3 Install Java

```
sudo apt update
sudo apt install -y openjdk-17-jdk
java -version
```

The output must show version 17.

### 3.4 Start the services

```
docker compose up -d
docker compose ps
```

Three containers must be running, namely kafka, mongodb and mongo-express. Mongo Express is a web page for inspecting MongoDB at http://localhost:8081 in the Windows browser. Kafka needs about 30 seconds after it starts before it accepts connections.

The MongoDB user in the compose file is admin with password admin123. These values are for a local test machine only and must be changed for any shared deployment.

### 3.5 Limit WSL memory

Copy docs/wslconfig.example to C:\Users\YOUR_WINDOWS_NAME\.wslconfig. The file sets memory to 7GB, processors to 4 and swap to 2GB. Then open PowerShell, which is the only place to run this command, and enter:

```
wsl --shutdown
```

Open the Ubuntu terminal again, go to the project folder, activate the environment with source venv/bin/activate and run docker compose up -d.

### 3.6 Check that the project is ready

Run Tests 1 to 3 in Section 6. Do not continue until all three pass.

## 4. Running the Pipeline

Run the steps in order from the project folder with the environment active. Paste one command at a time and wait for it to finish. A multi line paste can start a Spark job after an earlier command has failed.

### 4.1 Acquire the data

```
python acquire_data.py
```

This streams English tweets and Reddit comments from HuggingFace and writes Parquet chunks of 50,000 rows to data/raw. Skip this step if data/raw already holds the chunks. The result is 65 tweet chunks and 59 Reddit chunks, about 1.1 GB. The raw data is not stored in Git.

### 4.2 Load all posts into MongoDB

Stop the containers that are not needed, so Spark has memory.

```
docker stop kafka mongo-express
python ingest_bulk.py
```

The script prints one line per slice and ends by saving results/ingestion_metrics.csv. The last line before it must show a total of 6,144,625 rows. The whole load takes about two minutes.

### 4.3 Run the streaming path

```
docker start kafka
```

Wait about 30 seconds, then run the producer and the consumer.

```
python ingest_stream.py produce --n 200000
python ingest_stream.py consume
```

The producer sends 200,000 messages to the topic social_media_raw. The consumer reads the topic and writes to the collection posts_stream, then stops after 20 seconds without new messages. Both print their rate and append it to results/kafka_metrics.csv.

### 4.4 Run the analytics and the scalability experiment

Stop Kafka and Mongo Express again.

```
docker stop kafka mongo-express
python -u scalability_experiment.py 2>&1 | grep --line-buffered -v WARN | tee results/scalability_run.log
```

The script runs a warm up at 5 percent that is not recorded, then the full analytics job at 25, 50 and 100 percent. It prints a block of numbers after each size. The 100 percent run also writes every result table to the results folder and to the agg_ collections in MongoDB. Allow 15 to 35 minutes. The options -u and --line-buffered make progress visible while the job runs. Without them the screen stays empty until the end.

To run the analytics job alone at one size, use python analytics_job.py --fraction 1.0.

### 4.5 Open the dashboard

```
docker start mongo-express
streamlit run dashboard.py
```

Open the address that Streamlit prints, normally http://localhost:8501.

### 4.6 Rebuild the report and the diagram

```
python make_report.py
python tools/make_architecture_diagram.py
```

The first command writes report/COEN542_Group4_Report.docx from the result files. The second writes docs/architecture.png.

## 5. Using the Dashboard

The sidebar has three filters. Source selects Twitter, Reddit or both. Date range limits the days shown. Smoothing window sets the number of days in the rolling mean, from 1 to 30. Four figures sit above the tabs. They show the number of posts analysed after cleaning, the average VADER compound score, and the shares of positive and negative posts.

The Sentiment tab shows class counts by source, the distribution of compound scores and the daily average sentiment. The Trends tab shows the table of trending terms with their burst z score and lift. Choose keywords in the comparison box to plot their monthly frequency, and tick the box to normalise per 10,000 posts. The same tab lists the top keywords and hashtags, with a slider for the list length. The Validation tab shows the confusion matrix of VADER against the dataset labels and the agreement metrics. The Scalability tab shows analytics time, throughput and query time at each size, the cumulative ingestion time and the Kafka table. The Live MongoDB query tab runs an aggregation on the raw collection for the fraction you choose, and shows how long it takes.

There is no chart of sentiment by subreddit, because the Reddit sample comes from one community.

## 6. Testing Guide

Each test states a command and the expected result. Run the tests from the project folder. Tests 1 to 3 check the infrastructure, Tests 4 to 8 check the stored data, Tests 9 to 12 check ingestion and analytics, Tests 13 to 17 check the dashboard, and Test 18 checks the scalability experiment.

### 6.1 Infrastructure tests

**Test 1. Containers are running.**

```
docker compose ps
```

Expected result: kafka, mongodb and mongo-express are listed as running. If Kafka or Mongo Express were stopped on purpose, start them with docker start kafka mongo-express.

**Test 2. MongoDB answers.**

```
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.adminCommand('ping')"
```

Expected result: the output contains ok: 1.

**Test 3. Kafka answers.**

```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected result: the command returns without an error. Before the first producer run the list is empty. After Section 4.3 it contains social_media_raw.

### 6.2 Stored data tests

**Test 4. Parquet files exist.**

```
ls data/raw | wc -l
du -sh data/raw
```

Expected result: 124 files and about 1.1 GB.

**Test 5. Total document count.**

```
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval 'db.getSiblingDB("bigdata_project").posts_raw.countDocuments({})'
```

Expected result: 6144625.

**Test 6. Documents per source.**

```
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval 'printjson(db.getSiblingDB("bigdata_project").posts_raw.aggregate([{$group:{_id:"$source",n:{$sum:1}}}]).toArray())'
```

Expected result: twitter 3215960 and reddit 2928665. The two numbers add to the total in Test 5.

**Test 7. Sampling key gives repeatable fractions.**

```
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval 'var c=db.getSiblingDB("bigdata_project").posts_raw; printjson([c.countDocuments({sample_key:{$lt:0.25}}), c.countDocuments({sample_key:{$lt:0.5}})])'
```

Expected result: 1537287 and 3073348. These are the same on every run, because the key was assigned once with a fixed seed.

**Test 8. Indexes exist.**

```
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval 'printjson(db.getSiblingDB("bigdata_project").posts_raw.getIndexes().map(function(i){return i.name}))'
```

Expected result: a list that contains an index on sample_key and one on source and created_ts, besides the default _id index.

### 6.3 Ingestion and analytics tests

**Test 9. Streaming path.**

```
docker start kafka
python ingest_stream.py produce --n 200000
python ingest_stream.py consume
```

Expected result: the producer prints a rate of several thousand posts per second and the consumer prints a higher rate. Each prints 200,000 records. The collection posts_stream then holds 200,000 documents. Each consume run drops and refills posts_stream, but the topic keeps its messages, so a second produce run without clearing the topic leads to 400,000 documents.

**Test 10. Sentiment scoring logic.**

```
python - <<'EOF'
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
a = SentimentIntensityAnalyzer()
for t in ["I love this, it is great", "This is terrible and I hate it", "The meeting is at noon"]:
    print(round(a.polarity_scores(t)["compound"], 3), t)
EOF
```

Expected result: the first score is above 0.05, so the post is positive. The second is below minus 0.05, so it is negative. The third is close to 0, so it is neutral. These are the thresholds the pipeline uses.

**Test 11. Analytics smoke run on a small sample (optional).**

This test overwrites the result files and the agg_ collections with values for a 5 percent sample. Back up first and finish with a full run.

```
cp -r results results_backup
python analytics_job.py --fraction 0.05
```

Expected result: the printed table shows rows_in of about 307,000, which is 5 percent of 6,144,625, a smaller rows_after_cleaning, and a total time of a few minutes. Afterwards run python analytics_job.py --fraction 1.0 to restore the full results in the files and in MongoDB.

**Test 12. Result files exist and have the right content.**

After the full run in Section 4.4, run:

```
ls results/*.csv
cat results/validation_metrics.csv
grep -E "segwit|pepe|pluto|rosetta" results/trend_terms.csv
```

Expected result: the folder contains ingestion_metrics.csv, kafka_metrics.csv, scalability_metrics.csv, sentiment_by_source.csv, daily_sentiment.csv, monthly_totals.csv, compound_hist.csv, hashtag_monthly.csv, trend_terms.csv, validation_confusion.csv, validation_corr.csv and validation_metrics.csv. There is no community_sentiment.csv, which is correct for a one community sample. In validation_metrics.csv n_labelled_posts is 2684309 and classes_present_in_reference_labels is 3. Accuracy is about 0.467 and macro F1 about 0.421. The grep returns rows for segwit and pepe on Twitter and for pluto and rosetta on Reddit.

### 6.4 Dashboard tests

**Test 13. Dashboard starts and shows the totals.**

Start the dashboard with streamlit run dashboard.py and open the address. With both sources selected and the full date range, the figure Posts analysed shows 4,689,671.

**Test 14. All five tabs load.**

Click Sentiment, Trends, Validation, Scalability and Live MongoDB query in turn. Each tab must display its charts or tables without an error message. The Validation tab shows a three by three matrix and no single class warning.

**Test 15. Filters change the data.**

Untick Reddit in the sidebar. The figure Posts analysed must drop to 2,684,309, which is the Twitter total. Move the smoothing slider and check that the daily sentiment line changes shape.

**Test 16. Live query.**

Open the Live MongoDB query tab, choose 0.25 and press Run query. A response time in seconds appears. Choose 1.0 and run again. The time for the full collection is usually longer, and it varies between runs.

**Test 17. Fallback to files.**

```
docker stop mongodb
```

Reload the dashboard page. The caption at the top names the data source, and the charts still show data from the CSV files in results. If the caption still names MongoDB, restart the dashboard to clear its cache. Then restore the database.

```
docker start mongodb
```

### 6.5 Scalability test

**Test 18. Scalability experiment.**

```
python -u scalability_experiment.py 2>&1 | grep --line-buffered -v WARN | tee results/scalability_run.log
```

Expected result: three blocks, one for each size. The row counts are the same on every run. At 25 percent rows_in is 1537287 and rows_after_cleaning is 1208276. At 50 percent they are 3073348 and 2381956. At 100 percent they are 6144625 and 4689671. The times vary between runs. The script ends with the line Saved results/scalability_metrics.csv and results/figures/scalability_*.png, and the two files scalability_analytics.png and scalability_ingestion.png exist in results/figures. A run is valid for the report when the row counts match and no other heavy program was running.

## 7. Stopping and Cleaning Up

To stop the services and keep the data:

```
docker compose stop
```

To remove the containers and keep the MongoDB data volume:

```
docker compose down
```

Do not add the option -v to docker compose down unless the stored data should be deleted, because it removes the MongoDB volume and the posts must be loaded again with ingest_bulk.py.

To free Spark temporary files after an interrupted run:

```
pgrep -af java
pkill -9 -f java
rm -rf /tmp/spark-tmp/*
```

The first command lists Spark processes that survived Ctrl+C. Docker containers do not appear in this list, so the second command ends Spark only.

## 8. Troubleshooting

**The terminal shows nothing during a run.** The output is probably held back by a pipe. Use python -u with grep --line-buffered as in Section 4.4, and check progress from a second terminal with top or at http://localhost:4040 while a Spark job runs.

**Commands fail with strange errors.** Check where the command was entered. The commands wsl, diskpart and Remove-Item run in Windows PowerShell. The commands docker, python and df run in Ubuntu.

**Spark fails with an out of memory error.** Stop Kafka and Mongo Express with docker stop kafka mongo-express, close other programs, and confirm that .wslconfig sets memory to 7GB. Do not call collect or toPandas on a large table.

**Connection refused for MongoDB or Kafka.** Run docker compose ps and start the missing container. Wait about 30 seconds after starting Kafka.

**The file /var/run/docker.sock is missing in Ubuntu.** Open Docker Desktop, go to Settings, Resources, WSL Integration, switch Ubuntu off and on, then apply and restart.

**Maven download fails with Host not found.** Set a DNS server for WSL with sudo sh -c 'echo "nameserver 8.8.8.8" > /etc/resolv.conf' and run the script again.

**Spark cannot read Parquet timestamps.** Pandas writes nanosecond timestamps. The setting spark.sql.legacy.parquet.nanosAsLong in spark_session.py handles this, so check that the setting is present.

**Python version error from PySpark.** PySpark 3.5.1 does not support Python 3.13. Install Python 3.12 and create the environment again.

**The unzip command is not found.** Use python -c "import zipfile; zipfile.ZipFile('file.zip').extractall('.')".

**The system drive fills up.** The Docker disk image docker_data.vhdx grows with MongoDB data and does not shrink when data is deleted. Move the image to another drive in Docker Desktop under Settings, Resources, Advanced, Disk image location, or compact it after quitting Docker and running wsl --shutdown in PowerShell. Stop the heavy run if free space on the system drive falls below about 4.5 GB.

**Timings differ strongly between runs.** The host environment affects timings on a laptop. Close other programs, keep the machine plugged in and keep Kafka and Mongo Express stopped. Report a run only when the row counts match the values in Test 18.

## 9. Known Limitations

(i) Spark runs in local mode on four cores. The experiment measures growth on fixed resources and not scale out across machines.

(ii) Kafka has one broker and MongoDB has one node, so there is no replication.

(iii) The Reddit sample covers one community, r/askscience. The download reached its size target inside the first of the five requested subreddits.

(iv) The tweet labels are bullish, bearish and neutral, produced by a model. They describe market direction, so agreement with VADER is a consistency check and not an accuracy measurement.

(v) VADER scoring in a Python function is the main computational cost of the analytics job.

(vi) Timings come from single runs on a laptop and carry noise from the host environment.
