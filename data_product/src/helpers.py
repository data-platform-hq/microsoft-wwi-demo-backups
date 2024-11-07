import os
import sys
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_spark_session_init(py4j_log_level: str = None, spark_log_level: str = None) -> SparkSession:
    """Create spark session and apply configurations"""
    scala_version = "2.12"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    if py4j_log_level:
        logger = logging.getLogger('py4j')
        logger.setLevel(py4j_log_level)

    spark_session = configure_spark_with_delta_pip(
        SparkSession.builder
        .master('local[*]')
        .appName('IaCDA')
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ["com.microsoft.sqlserver:mssql-jdbc:8.4.0.jre11",
         f"com.microsoft.azure:spark-mssql-connector_{scala_version}_3.0:1.0.0-alpha"]
    ).getOrCreate()

    if spark_log_level:
        logger = logging.getLogger('py4j')
        logger.setLevel(spark_log_level)
        spark_session.sparkContext.setLogLevel(spark_log_level)

    return spark_session
