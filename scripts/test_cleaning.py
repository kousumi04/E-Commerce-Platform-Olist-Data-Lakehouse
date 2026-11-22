"""
Verification script for Phase 5: Data Cleaning, Validation & Processed Parquet Write.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.extract import extract_all_raw_tables
from src.transformations.cleaning import clean_all_tables
from src.transformations.validation import validate_cleaned_tables
from src.utils.io_utils import write_parquet
from src.utils.logger import get_logger
from src.utils.spark_utils import create_spark_session

logger = get_logger(__name__)


def main() -> None:
    """Execute Extract -> Clean -> Validate -> Write Processed Parquet."""
    load_dotenv()
    raw_dir = Path(os.getenv("DATASET_PATH", "data/raw"))
    processed_dir = Path(os.getenv("PROCESSED_PATH", "data/processed")) / "cleaned_data"

    spark = create_spark_session("RetailPulse-Cleaning-Verify")
    try:
        raw_tables = extract_all_raw_tables(spark, raw_dir)
        cleaned_tables = clean_all_tables(raw_tables)
        dq_report = validate_cleaned_tables(cleaned_tables)

        print("\n=== PHASE 5: RAW VS CLEANED ROW COUNTS ===")
        print(f"{'Table':22s} | {'Raw Rows':>10s} | {'Cleaned Rows':>12s} | {'PK Dupes':>8s}")
        print("-" * 62)
        for name in raw_tables:
            raw_cnt = raw_tables[name].count()
            clean_cnt = dq_report["tables"][name]["rows"]
            pk_dupes = dq_report["tables"][name]["pk_duplicates"]
            print(f"{name:22s} | {raw_cnt:>10,} | {clean_cnt:>12,} | {pk_dupes:>8}")

        print("\n=== REFERENTIAL INTEGRITY (ORPHAN COUNT) ===")
        for rel, orphans in dq_report["referential_integrity"].items():
            print(f"  {rel:48s} : {orphans} orphans")

        # Persist cleaned Parquet files to data/processed/cleaned_data/
        for name, df in cleaned_tables.items():
            write_parquet(df, processed_dir, name)

        logger.info("Phase 5 cleaning, validation, and Parquet export PASSED.")
    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()