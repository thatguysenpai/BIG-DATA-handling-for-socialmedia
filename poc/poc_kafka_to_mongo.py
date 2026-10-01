import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'social_media_raw'

# MongoDB Configuration
MONGO_URI = "mongodb://admin:admin123@localhost:27017/"
DB_NAME = "bigdata_project"
COLLECTION_NAME = "raw_tweets"

def main():
    print(f"Connecting to MongoDB at {MONGO_URI}...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Clear existing data for a clean proof-of-concept run
    collection.delete_many({})
    print("Cleared existing documents in collection (if any).")

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000 # Stop if no new messages for 5 seconds
    )
    
    print(f"Listening to topic '{TOPIC_NAME}'...")
    messages_to_insert = []
    count = 0
    
    for message in consumer:
        messages_to_insert.append(message.value)
        count += 1
        
        # Insert in batches of 100 for efficiency
        if len(messages_to_insert) >= 100:
            collection.insert_many(messages_to_insert)
            print(f"  -> Inserted batch of {len(messages_to_insert)} documents.")
            messages_to_insert = []
            
    # Insert any remaining messages
    if messages_to_insert:
        collection.insert_many(messages_to_insert)
        print(f"  -> Inserted final batch of {len(messages_to_insert)} documents.")
        
    consumer.close()
    
    # Verify final count
    final_count = collection.count_documents({})
    print(f"\n Successfully ingested {final_count} records from Kafka into MongoDB collection '{COLLECTION_NAME}'.")

if __name__ == "__main__":
    main()