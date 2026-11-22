"""
End-to-End Local PySpark ETL Runner (Extract -> Clean -> Validate -> Model -> Curated Parquet).
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.extract import extract_all_raw_tables
from src.modeling.dimension_tables import build_all_dimensions
from src.modeling.fact_tables import build_all_facts
from src.transformations.cleaning import clean_all_tables
from src.transformations.validation import validate_cleaned_tables
from src.utils.io_utils import write_parquet
from src.utils.logger import get_logger
from src.utils.spark_utils import create_spark_session

logger = get_logger(__name__)


def main() -> None:
    """Run full local ETL pipeline and reconcile curated Star Schema totals."""
    load_dotenv()
    raw_dir = Path(os.getenv("DATASET_PATH", "data/raw"))
    processed_dir = Path(os.getenv("PROCESSED_PATH", "data/processed")) / "cleaned_data"
    curated_dir = Path(os.getenv("CURATED_PATH", "data/curated"))

    spark = create_spark_session("RetailPulse-Full-ETL")
    try:
        # 1. Extract
        raw_tables = extract_all_raw_tables(spark, raw_dir)

        # 2. Clean & Validate
        cleaned_tables = clean_all_tables(raw_tables)
        validate_cleaned_tables(cleaned_tables)

        # 3. Write Processed (Silver) Parquet
        for name, df in cleaned_tables.items():
            write_parquet(df, processed_dir, name)

        # 4. Build Star Schema (Gold)
        dimensions = build_all_dimensions(spark, cleaned_tables)
        facts = build_all_facts(cleaned_tables)
        curated_tables = {**dimensions, **facts}

        # 5. Write Curated (Gold) Parquet & Print Summary
        print("\n=== PHASE 6: CURATED STAR SCHEMA SUMMARY ===")
        print(f"{'Table':20s} | {'Rows':>10s} | {'Columns':>7s}")
        print("-" * 44)
        for name, df in curated_tables.items():
            row_count = df.count()
            print(f"{name:20s} | {row_count:>10,} | {len(df.columns):>7}")
            write_parquet(df, curated_dir, name)

        # 6. Financial Reconciliation Check (Raw vs Curated)
        raw_gmv = raw_tables["order_items"].agg(F.round(F.sum("price"), 2)).collect()[0][0]
        curated_gmv = facts["fact_order_items"].agg(F.round(F.sum("price"), 2)).collect()[0][0]
        raw_pay = raw_tables["payments"].agg(F.round(F.sum("payment_value"), 2)).collect()[0][0]
        curated_pay = facts["fact_payments"].agg(F.round(F.sum("payment_value"), 2)).collect()[0][0]

        print("\n=== FINANCIAL RECONCILIATION (BRL) ===")
        print(f"  Item GMV (price)  -> Raw: {raw_gmv:,.2f} | Curated: {curated_gmv:,.2f}")
        print(f"  Payments Total    -> Raw: {raw_pay:,.2f} | Curated: {curated_pay:,.2f}")
        assert abs(raw_gmv - curated_gmv) < 0.01, "GMV mismatch between Raw and Curated!"
        assert abs(raw_pay - curated_pay) < 0.01, "Payments mismatch between Raw and Curated!"

        logger.info("Phase 6 Star Schema modeling and reconciliation PASSED.")
    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()