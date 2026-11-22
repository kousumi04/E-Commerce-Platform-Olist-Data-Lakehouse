"""
Verify all three Medallion layers (raw, processed, curated) in Amazon S3.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.s3_utils import get_s3_client, list_objects

logger = get_logger(__name__)

EXPECTED_CURATED_TABLES = [
    "dim_date",
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "dim_geolocation",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_reviews",
]


def main() -> None:
    """Audit object counts, sizes, and expected curated tables in S3."""
    load_dotenv()
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        raise ValueError("S3_BUCKET_NAME is not set in .env")

    s3_client = get_s3_client()
    layers = ["raw/", "processed/cleaned_data/", "curated/"]

    print(f"\n=== S3 DATA LAKE AUDIT: s3://{bucket} ===")
    print(f"{'Layer Prefix':26s} | {'Files':>6s} | {'Total Size (MB)':>15s}")
    print("-" * 54)

    curated_keys = []
    for prefix in layers:
        objs = list_objects(s3_client, bucket, prefix)
        total_mb = sum(o["Size"] for o in objs) / (1024 * 1024)
        print(f"{prefix:26s} | {len(objs):>6} | {total_mb:>15.2f}")
        if prefix == "curated/":
            curated_keys = [o["Key"] for o in objs]

    # Verify all 9 Star Schema tables are present in curated/
    missing = [
        tbl for tbl in EXPECTED_CURATED_TABLES
        if not any(f"curated/{tbl}/" in key for key in curated_keys)
    ]
    if missing:
        raise RuntimeError(f"Missing curated tables in S3: {missing}")

    print("\nAll 9 Curated Star Schema tables verified in S3!")
    logger.info("Phase 7 S3 verification PASSED.")


if __name__ == "__main__":
    main()