#!/usr/bin/env bash
# Run from the PROJECT ROOT after unzipping the cleanup bundle over the project folder.
# It removes the old-named files and moves the proof-of-concept scripts. Safe to run twice.
set -u

drop() {                       # remove an old file (git-aware)
  [ -e "$1" ] || { echo "already gone: $1"; return; }
  if git ls-files --error-unmatch "$1" >/dev/null 2>&1; then git rm -q -f "$1"; else rm -f "$1"; fi
  echo "removed: $1"
}
move() {                       # move old -> new (git-aware)
  [ -e "$1" ] || { echo "not found (skipped): $1"; return; }
  mkdir -p "$(dirname "$2")"
  if git ls-files --error-unmatch "$1" >/dev/null 2>&1; then git mv -f "$1" "$2"; else mv -f "$1" "$2"; fi
  echo "moved: $1 -> $2"
}

# replaced by the new-named files in the bundle
drop phase3_get_data.py
drop phase7_full_ingestion.py
drop phase7a_kafka_stream.py
drop phase8_analytics.py
drop phase9_scalability.py
drop phase10_dashboard.py
drop phase4_kafka_producer.py      # new copy is poc/poc_kafka_producer.py
drop HANDOFF.md                    # archived as docs/HANDOFF_ARCHIVE.md
drop wslconfig                     # now docs/wslconfig.example

# proof-of-concept scripts that were not in the packed repository: move them if they exist locally
move phase5_kafka_to_mongo.py   poc/poc_kafka_to_mongo.py
move phase6_mongodb_to_spark.py poc/poc_mongodb_to_spark.py

echo
echo "Leftover references to old names (only PROGRESS_LOG.md history and the poc scripts may appear):"
grep -rn --include=*.py --include=*.md --include=*.txt --include=*.yml -E "phase(3|4|5|6|7|7a|8|9|10)_" . \
  --exclude-dir=venv --exclude-dir=.git --exclude-dir=data || echo "none"
