"""Single Spark session factory used by every Spark phase."""
from pyspark.sql import SparkSession
import config


def get_spark(app_name: str, use_mongo: bool = True, driver_mem: str = "7g", cores: int = 4):
    b = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_mem)
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.local.dir", config.SPARK_TMP)          # keep shuffle/spill on WSL-native disk
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")   # pandas writes ns timestamps; Spark 3.5 cannot read them natively
        .config("spark.ui.showConsoleProgress", "false")  # Arrow on Java 17/21
    )
    if use_mongo and not config.OFFLINE:
        b = b.config("spark.jars.packages", config.MONGO_SPARK_PACKAGE)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
