"""
Shared Spark session factory.

IMPORTANT: the MongoDB Spark connector is NOT a pip package. It's pulled in
at runtime via Maven coordinates, matched to your Spark/Scala version.
pyspark==3.5.1 ships with Scala 2.12, so we use the connector build for
Spark 3.5 / Scala 2.12. If you upgrade pyspark, you MUST match this string
to the new Spark version or every Mongo read/write will fail with a
ClassNotFoundException.
"""

from pyspark.sql import SparkSession

MONGO_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
MONGO_URI = "mongodb://admin:admin123@localhost:27017/bigdata_project.posts?authSource=admin"


def get_spark_session(app_name: str = "BigDataProject") -> SparkSession:
    """
    Memory settings tuned for a 12GB host machine (WSL2 capped to 7GB via
    .wslconfig — see wsl-config-reference/.wslconfig). Driver + executor
    together stay under that ceiling with room for Kafka/Mongo containers
    running alongside. If you run on a machine with more RAM, these are
    conservative floors, not a hard requirement — feel free to raise them.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", MONGO_CONNECTOR_PACKAGE)
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")  # default 200 is overkill and slow at this data size
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
