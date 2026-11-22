"""
Verification script for Phase 4: PySpark Ingestion.
Loads all 9 Olist CSVs with explicit schemas and validates non-empty counts.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Ensure project root is on sys.path when executed directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.extract import extract_all_raw_tables
from src.utils.logger import get_logger
from src.utils.spark_utils import create_spark_session

logger = get_logger(__name__)


def main() -> None:
    """Run ingestion check across all 9 raw Olist CSV files."""
    load_dotenv()
    dataset_path = Path(os.getenv("DATASET_PATH", "data/raw"))

    spark = create_spark_session("RetailPulse-Ingestion-Verify")
    try:
        raw_tables = extract_all_raw_tables(spark, dataset_path)

        print("\n=== PHASE 4 PYSPARK INGESTION SUMMARY ===")
        for name, df in raw_tables.items():
            count = df.count()
            assert count > 0, f"Table '{name}' loaded with 0 rows!"
            print(f"{name:22s} | Rows: {count:>9,} | Columns: {len(df.columns):>2}")

        print("\nSample schema check (orders):")
        raw_tables["orders"].printSchema()
        logger.info("Phase 4 ingestion verification PASSED.")
    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()