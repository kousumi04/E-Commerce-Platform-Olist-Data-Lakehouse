"""
Fact table builders for the RetailPulse Star Schema (4 Facts).
"""

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _to_date_key(ts_col: str):
    """Convert a timestamp column into a YYYYMMDD integer surrogate key."""
    return F.date_format(F.col(ts_col), "yyyyMMdd").cast("int")


def build_fact_orders(orders_df: DataFrame) -> DataFrame:
    """
    Build order-level fact table with logistics and delivery KPIs.
    Grain: 1 row per order_id.
    Delivery duration & late flags are computed ONLY for delivered orders
    with a valid actual delivery timestamp.
    """
    logger.info("Building fact_orders...")
    valid_delivery = (F.col("order_status") == "delivered") & F.col(
        "order_delivered_customer_date"
    ).isNotNull()

    delivery_delay = F.datediff(
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),
    )

    return orders_df.select(
        F.col("order_id"),
        F.col("customer_id"),
        _to_date_key("order_purchase_timestamp").alias("purchase_date_key"),
        F.col("order_status"),
        F.col("order_purchase_timestamp"),
        F.col("order_approved_at"),
        F.col("order_delivered_carrier_date"),
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),
        (F.col("order_status") == "delivered").alias("is_delivered"),
        F.when(
            valid_delivery,
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp"),
            ),
        ).alias("delivery_days"),
        F.datediff(
            F.col("order_estimated_delivery_date"),
            F.col("order_purchase_timestamp"),
        ).alias("estimated_delivery_days"),
        F.when(valid_delivery, delivery_delay).alias("delivery_delay_days"),
        F.when(valid_delivery, delivery_delay > 0).alias("is_late_delivery"),
    )


def build_fact_order_items(
    order_items_df: DataFrame, orders_df: DataFrame
) -> DataFrame:
    """
    Build line-item sales fact table.
    Grain: 1 row per (order_id, order_item_id).
    """
    logger.info("Building fact_order_items...")
    order_lookup = orders_df.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_status"),
        _to_date_key("order_purchase_timestamp").alias("purchase_date_key"),
    )

    return order_items_df.join(order_lookup, on="order_id", how="inner").select(
        F.col("order_id"),
        F.col("order_item_id"),
        F.col("product_id"),
        F.col("seller_id"),
        F.col("customer_id"),
        F.col("purchase_date_key"),
        F.col("order_status"),
        F.col("shipping_limit_date"),
        F.round(F.col("price"), 2).alias("price"),
        F.round(F.col("freight_value"), 2).alias("freight_value"),
        F.round(F.col("price") + F.col("freight_value"), 2).alias("total_item_value"),
    )


def build_fact_payments(
    payments_df: DataFrame, orders_df: DataFrame
) -> DataFrame:
    """
    Build order payment fact table.
    Grain: 1 row per (order_id, payment_sequential).
    """
    logger.info("Building fact_payments...")
    order_lookup = orders_df.select(
        F.col("order_id"),
        F.col("customer_id"),
        _to_date_key("order_purchase_timestamp").alias("purchase_date_key"),
    )

    return payments_df.join(order_lookup, on="order_id", how="inner").select(
        F.col("order_id"),
        F.col("payment_sequential"),
        F.col("customer_id"),
        F.col("purchase_date_key"),
        F.col("payment_type"),
        F.col("payment_installments"),
        F.round(F.col("payment_value"), 2).alias("payment_value"),
    )


def build_fact_reviews(
    reviews_df: DataFrame, orders_df: DataFrame
) -> DataFrame:
    """
    Build customer review fact table.
    Grain: 1 row per (review_id, order_id).
    """
    logger.info("Building fact_reviews...")
    order_lookup = orders_df.select(
        F.col("order_id"),
        F.col("customer_id"),
    )

    return reviews_df.join(order_lookup, on="order_id", how="inner").select(
        F.col("review_id"),
        F.col("order_id"),
        F.col("customer_id"),
        _to_date_key("review_creation_date").alias("review_date_key"),
        F.col("review_score"),
        (
            F.col("review_comment_message").isNotNull()
            & (F.length(F.col("review_comment_message")) > 0)
        ).alias("has_comment"),
        F.col("review_creation_date"),
        F.col("review_answer_timestamp"),
    )


def build_all_facts(cleaned_tables: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """Build and return all 4 Star Schema fact tables."""
    orders_df = cleaned_tables["orders"]
    return {
        "fact_orders": build_fact_orders(orders_df),
        "fact_order_items": build_fact_order_items(
            cleaned_tables["order_items"], orders_df
        ),
        "fact_payments": build_fact_payments(
            cleaned_tables["payments"], orders_df
        ),
        "fact_reviews": build_fact_reviews(
            cleaned_tables["reviews"], orders_df
        ),
    }