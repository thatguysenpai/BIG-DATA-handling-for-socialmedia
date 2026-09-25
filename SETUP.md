# Phase 2 Setup — Environment

## 0. Location matters (WSL2 + Docker Desktop)

Work entirely inside the WSL2 filesystem, e.g. `~/bigdata-project`, NOT
`/mnt/c/Users/...`. Docker Desktop's WSL2 backend is dramatically slower
and occasionally flaky when bind-mounting across the Windows/Linux
boundary. Clone/copy this project into your WSL home directory first.

Check you're in WSL2, not PowerShell/CMD, before running anything below:

    wsl
    cd ~
    mkdir bigdata-project && cd bigdata-project
    # copy the files here

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

## 6. Memory constraints (12GB host — read this before Phase 6/Spark)

Your machine has 12GB total RAM, which is tight once WSL2 + Docker + Kafka
+ MongoDB + Spark are all running together. Three things to do:

1. **Copy `wsl-config-reference/.wslconfig` to `C:\Users\<you>\.wslconfig`**
   (on the Windows side, not inside WSL), then from PowerShell:

       wsl --shutdown

   and reopen your WSL terminal before continuing. This caps WSL2 at 7GB
   so Docker can't starve Windows itself.

2. **`spark_session.py` already sets conservative driver/executor memory**
   (3g/2g) — don't remove these configs even if Spark warns they're low;
   raising them risks OOM-killing your Spark job when Kafka+Mongo
   containers are also running.

3. **Stop Mongo Express while running heavy Spark jobs**, restart it after
   for inspecting results:

       docker compose stop mongo-express
       # ... run your Spark job ...
       docker compose start mongo-express

Target combined dataset size: **~1.5GB**, not higher — see the dataset
discussion in chat for why going bigger risks eating your time budget on
OOM debugging rather than actual pipeline work.

---

Once `docker compose ps` shows both containers healthy and the two
verification commands above both succeed, we're ready for Phase 3
(dataset download) and Phase 4 (Kafka producer/consumer proof).
