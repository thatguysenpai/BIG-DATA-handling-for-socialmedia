# Setup — Environment

## 0. Location matters (WSL2 + Docker Desktop)

Run everything from inside WSL2 (never PowerShell). The project folder may live on the Windows D: drive
(`/mnt/d/ITACHI/bigdata-project`, as in this project, chosen for disk space) or in the WSL home directory
(`~/bigdata-project`, faster I/O). On D:, Spark shuffle files are kept on WSL-native disk through `SPARK_TMP`
(`/tmp/spark-tmp`, set in `config.py`), and the virtual environment must be created at the final location, not copied.

Check you're in WSL2, not PowerShell/CMD, before running anything below:

    wsl
    cd ~
    git clone <repo-url> bigdata-project && cd bigdata-project   # or copy the files here
    # when copying a folder with cp -r, check that it did not create a nested duplicate folder

## 1. Python environment

Requires Python 3.10, 3.11, or 3.12 (PySpark 3.5.1 does not support 3.13
yet — check with `python3 --version` first; if you're on 3.13, install
3.12 via `sudo apt install python3.12 python3.12-venv`).

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

## 2. Java (required for PySpark, easy to forget)

Spark runs on the JVM. PySpark 3.5.x needs Java 8, 11, or 17.

    sudo apt update
    sudo apt install -y openjdk-17-jdk
    java -version   # confirm it shows 17.x

## 3. Docker services

Start Kafka + MongoDB + Mongo Express (a web UI for inspecting Mongo data,
useful for sanity-checking without writing queries):

    docker compose up -d
    docker compose ps    # all three should show "running"/"healthy"

Mongo Express UI: http://localhost:8081 (only reachable from Windows
browser because Docker Desktop forwards WSL2 ports automatically — no
extra config needed).

## 4. Verify each service independently before writing any pipeline code

Kafka:

    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Should return empty (no error) — confirms the broker is answering.

MongoDB:

    docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.adminCommand('ping')"

Should print `{ ok: 1 }`.

## 5. First-run gotcha to expect

The first time you run any Spark script that references
`spark.jars.packages`, Spark will download the Mongo connector jar (and
its transitive dependencies) from Maven Central — this can take 1-3
minutes and needs internet access. It's cached after that. Don't panic if
it looks stuck; check for download progress in the console output.

## 6. Memory constraints (12GB host — read this before running Spark)

Your machine has 12GB total RAM, which is tight once WSL2 + Docker + Kafka
+ MongoDB + Spark are all running together. Three things to do:

1. **Copy `docs/wslconfig.example` to `C:\Users\<you>\.wslconfig`**
   (on the Windows side, not inside WSL), then from PowerShell:

       wsl --shutdown

   and reopen your WSL terminal before continuing. This caps WSL2 at 7GB
   so Docker can't starve Windows itself.

2. **`spark_session.py` sets the Spark memory** (local mode, 4 cores, 7 GB driver, 64 shuffle partitions). The 7 GB driver
   matches the WSL allowance, so Kafka and Mongo Express must be stopped during the heavy runs (see step 3). The reported timings were
   measured with these settings; change them only if you re-run the whole experiment.

3. **Stop Mongo Express while running heavy Spark jobs**, restart it after
   for inspecting results:

       docker stop kafka mongo-express
       # ... run ingest_bulk.py / scalability_experiment.py ...
       docker start kafka mongo-express

Dataset size: about 1.1 GB of Parquet (6.14 million posts). Going bigger was rejected because dataset volume carries only 5 of the
60 marks and a larger corpus slows every debugging cycle on a 12 GB machine.

---

Once all three containers are running and the two verification commands succeed, continue with the run order in `README.md`
(start with `python acquire_data.py`).
