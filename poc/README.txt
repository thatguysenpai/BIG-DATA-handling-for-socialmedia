Proof-of-concept stage (run before the full pipeline was built):

  poc_kafka_producer.py     500 sample tweets -> Kafka topic social_media_raw     (python poc/poc_kafka_producer.py)
  poc_kafka_to_mongo.py     Kafka consumer -> MongoDB collection raw_tweets
  poc_mongodb_to_spark.py   PySpark reads raw_tweets and groups by sentiment_label

They keep the raw dataset schema and use their own inline settings. The production pipeline is in the project root
(ingest_bulk.py, ingest_stream.py, analytics_job.py) and takes all settings from config.py.
