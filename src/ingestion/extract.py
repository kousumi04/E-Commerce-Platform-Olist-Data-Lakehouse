"""
PySpark CSV extraction module for the Olist E-Commerce dataset.
"""

from pathlib import Path
from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from src.ingestion.schemas import DATASET_FILENAMES, OLIST_SCHEMAS
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract_csv(
    spark: SparkSession,
    table_name: str,
    data_dir: Path,
) -> DataFrame:
    """
    Read a single Olist CSV file into a Spark DataFrame using its explicit schema.

    Args:
        spark: Active SparkSession.
        table_name: Logical table key (e.g., 'orders', 'reviews').
        data_dir: Directory containing the raw CSV files.

    Returns:
        DataFrame: Loaded Spark DataFrame.

    Raises:
        KeyError: If table_name is not defined in DATASET_FILENAMES.
        FileNotFoundError: If the expected CSV file does not exist.
    """
    if table_name not in DATASET_FILENAMES:
        raise KeyError(f"Unknown table name: '{table_name}'")

    file_path = data_dir / DATASET_FILENAMES[table_name]
    if not file_path.is_file():
        raise FileNotFoundError(f"Required raw CSV not found: {file_path}")

    schema = OLIST_SCHEMAS[table_name]
    logger.info("Extracting '%s' from %s", table_name, file_path)

    try:
        df = (
            spark.read.option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(schema)
            .csv(str(file_path))
        )
        row_count = df.count()
        logger.info(
            "Loaded '%s': %s rows, %d columns.",
            table_name,
            f"{row_count:,}",
            len(df.columns),
        )
        return df
    except Exception as exc:
        logger.exception("Failed to extract '%s' from %s: %s", table_name, file_path, exc)
        raise


def extract_all_raw_tables(
    spark: SparkSession,
    data_dir: Path,
) -> Dict[str, DataFrame]:
    """
    Extract all 9 raw Olist CSV datasets into a dictionary of Spark DataFrames.

    Args:
        spark: Active SparkSession.
        data_dir: Directory containing the raw CSV files.

    Returns:
        Dict[str, DataFrame]: Mapping of logical table name to Spark DataFrame.
    """
    logger.info("Starting batch extraction of all 9 Olist tables from %s", data_dir)
    tables: Dict[str, DataFrame] = {}

    for table_name in DATASET_FILENAMES:
        tables[table_name] = extract_csv(spark, table_name, data_dir)

    logger.info("Successfully extracted all %d raw tables.", len(tables))
    return tables