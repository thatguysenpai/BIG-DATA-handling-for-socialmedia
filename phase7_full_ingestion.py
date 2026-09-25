import time
from pyspark.sql import SparkSession

def main(fraction=1.0):
    print(f"Initializing Spark session for {int(fraction*100)}% data ingestion...")
    
    # Initialize Spark with conservative memory settings and the MongoDB connector
    spark = SparkSession.builder \
        .appName("Full Dataset Ingestion to MongoDB") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin123@localhost:27017/bigdata_project.full_tweets?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .getOrCreate()

    print("Reading all Parquet files from data/raw/...")
    # Read all tweet and reddit chunks
    df = spark.read.parquet("data/raw/tweets_chunk_*.parquet", "data/raw/reddit_chunk_*.parquet")
    
    # Apply the fraction for the scalability experiment (e.g., 0.25, 0.5, 1.0)
    if fraction < 1.0:
        print(f"Sampling dataset to {int(fraction*100)}%...")
        df = df.sample(withReplacement=False, fraction=fraction, seed=42)
    
    total_rows_before = df.count()
    print(f"Total rows to process: {total_rows_before:,}")

    print("Writing data to MongoDB 'bigdata_project.full_tweets'...")
    start_time = time.time()
    
    # Write to MongoDB
    df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("collection", "full_tweets") \
        .save()
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Verify final count in MongoDB
    verify_df = spark.read.format("mongodb").option("collection", "full_tweets").load()
    final_count = verify_df.count()
    
    throughput = final_count / execution_time if execution_time > 0 else 0
    
    print("\n" + "="*50)
    print(f" Ingestion Complete ({int(fraction*100)}% Scale)")
    print(f"   - Rows Processed: {final_count:,}")
    print(f"   - Execution Time: {execution_time:.2f} seconds")
    print(f"   - Throughput:     {throughput:,.0f} records/second")
    print("="*50)
    
    spark.stop()

if __name__ == "__main__":
    # Run at 100% by default. Change to 0.25 or 0.5 for the scalability experiment.
    main(fraction=1.0)