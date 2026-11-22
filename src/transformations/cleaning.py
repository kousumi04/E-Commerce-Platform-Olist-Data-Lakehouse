"""
Modular PySpark cleaning transformations for all 9 Olist datasets.
"""

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _pad_zip(col_name: str):
    """Standardize Brazilian 5-digit CEP zip code prefix with leading zeros."""
    return F.lpad(F.trim(F.col(col_name)), 5, "0")


def clean_orders(df: DataFrame) -> DataFrame:
    """Clean orders: drop null keys, deduplicate, and standardize status."""
    return (
        df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .dropDuplicates(["order_id"])
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    """Clean order_items: drop null keys, enforce positive price & non-negative freight."""
    return (
        df.filter(
            F.col("order_id").isNotNull()
            & F.col("order_item_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("seller_id").isNotNull()
            & (F.col("price") > 0)
            & (F.col("freight_value") >= 0)
        )
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("seller_id", F.trim(F.col("seller_id")))
        .dropDuplicates(["order_id", "order_item_id"])
    )


def clean_payments(df: DataFrame) -> DataFrame:
    """Clean payments: drop null keys, standardize payment_type, keep non-negative values."""
    return (
        df.filter(
            F.col("order_id").isNotNull()
            & F.col("payment_sequential").isNotNull()
            & (F.col("payment_value") >= 0)
        )
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
        .withColumn(
            "payment_installments",
            F.when(F.col("payment_installments") < 1, F.lit(1)).otherwise(
                F.col("payment_installments")
            ),
        )
        .dropDuplicates(["order_id", "payment_sequential"])
    )


def clean_reviews(df: DataFrame) -> DataFrame:
    """Clean reviews: composite key deduplication (review_id, order_id) and valid 1-5 scores."""
    return (
        df.filter(
            F.col("review_id").isNotNull()
            & F.col("order_id").isNotNull()
            & F.col("review_score").between(1, 5)
        )
        .withColumn("review_id", F.trim(F.col("review_id")))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("review_comment_title", F.trim(F.col("review_comment_title")))
        .withColumn("review_comment_message", F.trim(F.col("review_comment_message")))
        .dropDuplicates(["review_id", "order_id"])
    )


def clean_customers(df: DataFrame) -> DataFrame:
    """Clean customers: standardize city/state/zip and deduplicate on customer_id."""
    return (
        df.filter(F.col("customer_id").isNotNull() & F.col("customer_unique_id").isNotNull())
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("customer_unique_id", F.trim(F.col("customer_unique_id")))
        .withColumn("customer_zip_code_prefix", _pad_zip("customer_zip_code_prefix"))
        .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .dropDuplicates(["customer_id"])
    )


def clean_products(df: DataFrame) -> DataFrame:
    """Clean products: fix source header typos ('lenght' -> 'length') and fill null categories."""
    return (
        df.filter(F.col("product_id").isNotNull())
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .withColumn(
            "product_category_name",
            F.coalesce(F.lower(F.trim(F.col("product_category_name"))), F.lit("unknown")),
        )
        .dropDuplicates(["product_id"])
    )


def clean_sellers(df: DataFrame) -> DataFrame:
    """Clean sellers: standardize city/state/zip and deduplicate on seller_id."""
    return (
        df.filter(F.col("seller_id").isNotNull())
        .withColumn("seller_id", F.trim(F.col("seller_id")))
        .withColumn("seller_zip_code_prefix", _pad_zip("seller_zip_code_prefix"))
        .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .dropDuplicates(["seller_id"])
    )


def clean_geolocation(df: DataFrame) -> DataFrame:
    """
    Clean geolocation: filter to valid Brazilian coordinate bounds and aggregate
    to 1 unique row per 5-digit zip_code_prefix to prevent many-to-many join explosion.
    """
    valid_geo = (
        df.filter(
            F.col("geolocation_zip_code_prefix").isNotNull()
            & F.col("geolocation_lat").between(-35.0, 6.0)
            & F.col("geolocation_lng").between(-75.0, -30.0)
        )
        .withColumn("geolocation_zip_code_prefix", _pad_zip("geolocation_zip_code_prefix"))
        .withColumn("geolocation_city", F.lower(F.trim(F.col("geolocation_city"))))
        .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    )

    return valid_geo.groupBy("geolocation_zip_code_prefix").agg(
        F.round(F.avg("geolocation_lat"), 6).alias("geolocation_lat"),
        F.round(F.avg("geolocation_lng"), 6).alias("geolocation_lng"),
        F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
        F.first("geolocation_state", ignorenulls=True).alias("geolocation_state"),
    )


def clean_category_translation(df: DataFrame) -> DataFrame:
    """Clean category translation lookup table."""
    return (
        df.filter(F.col("product_category_name").isNotNull())
        .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
        .withColumn(
            "product_category_name_english",
            F.lower(F.trim(F.col("product_category_name_english"))),
        )
        .dropDuplicates(["product_category_name"])
    )


def clean_all_tables(raw_tables: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """Run all 9 cleaning functions and return the cleaned DataFrame dictionary."""
    cleaners = {
        "orders": clean_orders,
        "order_items": clean_order_items,
        "payments": clean_payments,
        "reviews": clean_reviews,
        "customers": clean_customers,
        "products": clean_products,
        "sellers": clean_sellers,
        "geolocation": clean_geolocation,
        "category_translation": clean_category_translation,
    }

    cleaned: Dict[str, DataFrame] = {}
    for name, fn in cleaners.items():
        logger.info("Cleaning table '%s'...", name)
        cleaned[name] = fn(raw_tables[name])

    logger.info("Completed cleaning for all %d tables.", len(cleaned))
    return cleaned