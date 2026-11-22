"""
SparkSession factory configured for local Windows execution.
"""

import os
import sys
from pyspark.sql import SparkSession
from src.utils.logger import get_logger

logger = get_logger(__name__)


def create_spark_session(app_name: str = "RetailPulse-ETL") -> SparkSession:
    """
    Initialize and return a local SparkSession tuned for 16 GB RAM machines.

    Args:
        app_name: Display name for the Spark application.

    Returns:
        SparkSession: Active local SparkSession.
    """
    # Ensure Spark workers use the exact same .venv Python interpreter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    logger.info("Initializing local SparkSession: %s", app_name)

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready (Spark v%s).", spark.version)
    return spark