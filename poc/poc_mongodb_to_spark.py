from pyspark.sql import SparkSession

def main():
    print("Initializing Spark session with MongoDB connector...")
    
    # Initialize Spark with conservative memory settings and the MongoDB connector
    spark = SparkSession.builder \
        .appName("MongoDB to PySpark Proof") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.mongodb.read.connection.uri", "mongodb://admin:admin123@localhost:27017/bigdata_project.raw_tweets?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .getOrCreate()

    print("Reading data from MongoDB 'bigdata_project.raw_tweets'...")
    df = spark.read.format("mongodb").load()
    
    # Action 1: Count total documents (triggers the actual read from MongoDB)
    total_count = df.count()
    print(f"\n✅ Total documents read from MongoDB: {total_count}")
    
    # Action 2: Perform a simple distributed aggregation
    print("\nPerforming a simple distributed aggregation: Count by sentiment_label...")
    df.groupBy("sentiment_label").count().orderBy("count", ascending=False).show()
    
    print(" Phase 6 Proof Successful: PySpark successfully read and processed data from MongoDB.")
    
    spark.stop()

if __name__ == "__main__":
    main()