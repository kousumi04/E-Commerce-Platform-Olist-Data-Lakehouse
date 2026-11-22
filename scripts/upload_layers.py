"""
Upload Processed (Silver) and Curated (Gold) Parquet layers to Amazon S3.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.s3_utils import get_s3_client, upload_directory

logger = get_logger(__name__)


def main() -> None:
    """Upload local Processed and Curated Parquet datasets to S3."""
    load_dotenv()

    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        raise ValueError("S3_BUCKET_NAME is not set in .env")

    processed_local = Path(os.getenv("PROCESSED_PATH", "data/processed")) / "cleaned_data"
    curated_local = Path(os.getenv("CURATED_PATH", "data/curated"))

    processed_prefix = os.getenv("S3_PROCESSED_PREFIX", "processed/").strip("/") + "/cleaned_data"
    curated_prefix = os.getenv("S3_CURATED_PREFIX", "curated/").strip("/")

    s3_client = get_s3_client()

    logger.info("=== Uploading Processed (Silver) Layer ===")
    processed_keys = upload_directory(
        s3_client=s3_client,
        local_dir=processed_local,
        bucket=bucket,
        s3_prefix=processed_prefix,
        file_extension=".parquet",
    )

    logger.info("=== Uploading Curated (Gold) Star Schema Layer ===")
    curated_keys = upload_directory(
        s3_client=s3_client,
        local_dir=curated_local,
        bucket=bucket,
        s3_prefix=curated_prefix,
        file_extension=".parquet",
    )

    print("\n=== PHASE 7 S3 UPLOAD SUMMARY ===")
    print(f"  Bucket          : {bucket}")
    print(f"  Processed files : {len(processed_keys)} Parquet files uploaded")
    print(f"  Curated files   : {len(curated_keys)} Parquet files uploaded")
    logger.info("Phase 7 S3 upload completed successfully.")


if __name__ == "__main__":
    main()