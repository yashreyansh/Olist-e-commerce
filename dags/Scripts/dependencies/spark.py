from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable 
from gamma.Scripts.dependencies.logging import Log4J

def start_spark(app_name="DataPipeline"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        # Delta Lake integration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
        # Ensure driver and executors can see jars downloaded by --packages
        .config("spark.driver.extraClassPath", "/opt/spark/custom-jars/*")
    .config("spark.executor.extraClassPath", "/opt/spark/custom-jars/*")
        .getOrCreate()
    )
    spark_logger = Log4J(spark)
    spark_logger.info("Spark session started successfully")
    return spark, spark_logger