"""
Dimensional table builders for the RetailPulse Star Schema (5 Dimensions).
"""

from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_dim_date(
    spark: SparkSession,
    start_date: str = "2016-01-01",
    end_date: str = "2019-12-31",
) -> DataFrame:
    """
    Build a calendar dimension covering all Olist order and delivery dates.
    Grain: 1 row per calendar day. PK: date_key (YYYYMMDD integer).
    """
    logger.info("Building dim_date (%s to %s)...", start_date, end_date)
    dates_df = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), "
        f"interval 1 day)) AS full_date"
    )

    return dates_df.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin([1, 7]).alias("is_weekend"),
    )


def build_dim_customers(customers_df: DataFrame) -> DataFrame:
    """
    Build customer dimension.
    Grain: 1 row per order-level customer_id (retains customer_unique_id for repeat analysis).
    """
    logger.info("Building dim_customers...")
    return customers_df.select(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
    )


def build_dim_products(
    products_df: DataFrame, translation_df: DataFrame
) -> DataFrame:
    """
    Build product dimension enriched with English category translations.
    Grain: 1 row per product_id.
    """
    logger.info("Building dim_products...")
    return (
        products_df.join(
            translation_df,
            on="product_category_name",
            how="left",
        )
        .select(
            F.col("product_id"),
            F.col("product_category_name").alias("category_portuguese"),
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name"),
                F.lit("unknown"),
            ).alias("category_english"),
            F.col("product_name_length"),
            F.col("product_description_length"),
            F.col("product_photos_qty"),
            F.col("product_weight_g"),
            F.col("product_length_cm"),
            F.col("product_height_cm"),
            F.col("product_width_cm"),
        )
    )


def build_dim_sellers(sellers_df: DataFrame) -> DataFrame:
    """
    Build seller dimension.
    Grain: 1 row per seller_id.
    """
    logger.info("Building dim_sellers...")
    return sellers_df.select(
        F.col("seller_id"),
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
    )


def build_dim_geolocation(geolocation_df: DataFrame) -> DataFrame:
    """
    Build deduplicated geolocation dimension.
    Grain: 1 row per 5-digit zip_code_prefix.
    """
    logger.info("Building dim_geolocation...")
    return geolocation_df.select(
        F.col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
        F.col("geolocation_lat").alias("latitude"),
        F.col("geolocation_lng").alias("longitude"),
        F.col("geolocation_city").alias("city"),
        F.col("geolocation_state").alias("state"),
    )


def build_all_dimensions(
    spark: SparkSession, cleaned_tables: Dict[str, DataFrame]
) -> Dict[str, DataFrame]:
    """Build and return all 5 Star Schema dimension tables."""
    return {
        "dim_date": build_dim_date(spark),
        "dim_customers": build_dim_customers(cleaned_tables["customers"]),
        "dim_products": build_dim_products(
            cleaned_tables["products"], cleaned_tables["category_translation"]
        ),
        "dim_sellers": build_dim_sellers(cleaned_tables["sellers"]),
        "dim_geolocation": build_dim_geolocation(cleaned_tables["geolocation"]),
    }