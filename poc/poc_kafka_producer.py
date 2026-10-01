"""Proof of concept: send 500 sample tweet rows to Kafka (topic social_media_raw).

Run from the project root:  python poc/poc_kafka_producer.py
Messages keep the raw dataset schema (not the unified schema); ingest_stream.py skips them.
"""
import json
import pandas as pd
import numpy as np
from pathlib import Path
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'social_media_raw'

def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Find all tweet chunk files and sort them to get the first one
    chunk_files = sorted(list(Path(__file__).resolve().parent.parent / "data" / "raw".glob("tweets_chunk_*.parquet")))
    if not chunk_files:
        print("Error: No tweet chunk files found in data/raw/")
        return
    
    first_chunk = chunk_files[0]
    print(f"Reading sample from {first_chunk.name}...")
    
    # Read just the first 500 rows for a quick, safe proof-of-concept
    df = pd.read_parquet(first_chunk).head(500)
    
    print(f"Sending {len(df)} records to Kafka topic '{TOPIC_NAME}'...")
    count = 0
    for index, row in df.iterrows():
        record = {}
        for k, v in row.items():
            # 1. Handle lists, tuples, sets, and NumPy arrays first
            if isinstance(v, (list, tuple, set, np.ndarray)):
                try:
                    record[k] = [str(item) for item in v]
                except TypeError:
                    record[k] = str(v)
            else:
                # 2. Now v is guaranteed to be a scalar, so pd.isna() is safe
                try:
                    is_na = pd.isna(v)
                except ValueError:
                    is_na = False # Fallback if pd.isna still acts up
                
                if is_na:
                    record[k] = None
                elif hasattr(v, 'isoformat'): # Handles pandas Timestamps safely
                    record[k] = v.isoformat()
                else:
                    record[k] = str(v)
                
        producer.send(TOPIC_NAME, value=record)
        count += 1
        
    producer.flush()
    print(f"✅ Successfully sent {count} records to Kafka topic '{TOPIC_NAME}'.")

if __name__ == "__main__":
    main()
