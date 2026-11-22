"""
Reusable Amazon S3 utilities powered by Boto3 and AWS CLI named profiles.
Never hardcodes AWS credentials.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def get_s3_client(
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
):
    """
    Create a Boto3 S3 client using the configured AWS CLI profile.

    Args:
        profile_name: AWS CLI profile name (defaults to AWS_PROFILE env var).
        region_name: AWS region (defaults to AWS_REGION env var).

    Returns:
        boto3.client: Configured S3 client.
    """
    profile = profile_name or os.getenv("AWS_PROFILE")
    region = region_name or os.getenv("AWS_REGION", "ap-south-1")

    try:
        session = (
            boto3.Session(profile_name=profile, region_name=region)
            if profile
            else boto3.Session(region_name=region)
        )
        client = session.client("s3")
        logger.info("Initialized S3 client (profile=%s, region=%s).", profile or "default", region)
        return client
    except NoCredentialsError as exc:
        logger.error("AWS credentials not found. Run 'aws configure --profile retailpulse-dev'.")
        raise exc


def object_exists(s3_client, bucket: str, s3_key: str) -> bool:
    """Return True if an object exists in S3, False otherwise."""
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        logger.error("Error checking existence of s3://%s/%s: %s", bucket, s3_key, exc)
        raise


def upload_file(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
) -> bool:
    """
    Upload a single local file to S3 and verify its existence after upload.

    Args:
        s3_client: Active Boto3 S3 client.
        local_path: Path to local file.
        bucket: Target S3 bucket name.
        s3_key: Target S3 object key (forward slashes).

    Returns:
        bool: True if uploaded and verified.
    """
    if not local_path.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    try:
        logger.info("Uploading %s -> s3://%s/%s", local_path, bucket, s3_key)
        s3_client.upload_file(str(local_path), bucket, s3_key)

        if not object_exists(s3_client, bucket, s3_key):
            raise RuntimeError(f"Upload verification failed for s3://{bucket}/{s3_key}")
        return True
    except (ClientError, NoCredentialsError) as exc:
        logger.error("Failed to upload %s to s3://%s/%s: %s", local_path, bucket, s3_key, exc)
        raise


def upload_directory(
    s3_client,
    local_dir: Path,
    bucket: str,
    s3_prefix: str,
    file_extension: str = ".parquet",
) -> List[str]:
    """
    Recursively upload all files matching file_extension from local_dir to S3.

    Args:
        s3_client: Active Boto3 S3 client.
        local_dir: Local root directory to scan.
        bucket: Target S3 bucket name.
        s3_prefix: Target S3 prefix (e.g., 'curated/' or 'processed/cleaned_data/').
        file_extension: Only upload files ending with this extension.

    Returns:
        List[str]: List of uploaded S3 keys.
    """
    if not local_dir.is_dir():
        raise FileNotFoundError(f"Local directory not found: {local_dir}")

    normalized_prefix = s3_prefix.strip("/")
    uploaded_keys: List[str] = []

    files = sorted(local_dir.rglob(f"*{file_extension}"))
    if not files:
        logger.warning("No '%s' files found in %s.", file_extension, local_dir)
        return uploaded_keys

    for file_path in files:
        relative_posix = file_path.relative_to(local_dir).as_posix()
        s3_key = f"{normalized_prefix}/{relative_posix}" if normalized_prefix else relative_posix
        upload_file(s3_client, file_path, bucket, s3_key)
        uploaded_keys.append(s3_key)

    logger.info(
        "Uploaded %d '%s' files from %s to s3://%s/%s/",
        len(uploaded_keys),
        file_extension,
        local_dir,
        bucket,
        normalized_prefix,
    )
    return uploaded_keys


def list_objects(
    s3_client,
    bucket: str,
    prefix: str = "",
) -> List[Dict]:
    """
    List all objects in an S3 bucket under a given prefix using pagination.

    Returns:
        List[Dict]: List of dicts containing 'Key' and 'Size' (bytes).
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    objects: List[Dict] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # Skip zero-byte folder placeholder markers
            if not obj["Key"].endswith("/"):
                objects.append({"Key": obj["Key"], "Size": obj["Size"]})

    return objects