"""
Parquet I/O utilities with Windows-safe fallback when HADOOP_HOME is unset.
"""

import os
from pathlib import Path
from pyspark.sql import DataFrame
from src.utils.logger import get_logger

logger = get_logger(__name__)


def write_parquet(df: DataFrame, output_dir: Path, table_name: str) -> Path:
    """
    Write a Spark DataFrame to Snappy-compressed Parquet.

    Uses native Spark Parquet writer when HADOOP_HOME is configured.
    On Windows without winutils.exe/HADOOP_HOME, falls back cleanly to
    PyArrow so local execution succeeds without Hadoop native binaries.

    Args:
        df: Spark DataFrame to persist.
        output_dir: Parent directory for the table folder.
        table_name: Logical table name (used as subfolder/file name).

    Returns:
        Path: Directory where the Parquet file(s) were written.
    """
    target_dir = output_dir / table_name
    target_dir.mkdir(parents=True, exist_ok=True)

    use_native_spark = os.name != "nt" or bool(os.environ.get("HADOOP_HOME"))

    if use_native_spark:
        logger.info("Writing '%s' via native Spark Parquet to %s", table_name, target_dir)
        df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
            str(target_dir)
        )
    else:
        logger.info(
            "HADOOP_HOME unset on Windows; writing '%s' via PyArrow Snappy Parquet to %s",
            table_name,
            target_dir,
        )
        # Remove stale parquet files for idempotent overwrite
        for old_file in target_dir.glob("*.parquet"):
            old_file.unlink()

        pdf = df.toPandas()
        parquet_file = target_dir / f"{table_name}.snappy.parquet"
        pdf.to_parquet(parquet_file, engine="pyarrow", compression="snappy", index=False)

    return target_dir