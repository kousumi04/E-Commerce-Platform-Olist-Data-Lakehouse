"""
Explicit PySpark StructType schemas for all 9 raw Olist CSV datasets.
"""

from typing import Dict
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

OLIST_SCHEMAS: Dict[str, StructType] = {
    "orders": StructType([
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_status", StringType(), nullable=True),
        StructField("order_purchase_timestamp", TimestampType(), nullable=True),
        StructField("order_approved_at", TimestampType(), nullable=True),
        StructField("order_delivered_carrier_date", TimestampType(), nullable=True),
        StructField("order_delivered_customer_date", TimestampType(), nullable=True),
        StructField("order_estimated_delivery_date", TimestampType(), nullable=True),
    ]),
    "order_items": StructType([
        StructField("order_id", StringType(), nullable=False),
        StructField("order_item_id", IntegerType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("seller_id", StringType(), nullable=False),
        StructField("shipping_limit_date", TimestampType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("freight_value", DoubleType(), nullable=True),
    ]),
    "payments": StructType([
        StructField("order_id", StringType(), nullable=False),
        StructField("payment_sequential", IntegerType(), nullable=False),
        StructField("payment_type", StringType(), nullable=True),
        StructField("payment_installments", IntegerType(), nullable=True),
        StructField("payment_value", DoubleType(), nullable=True),
    ]),
    "reviews": StructType([
        StructField("review_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("review_score", IntegerType(), nullable=True),
        StructField("review_comment_title", StringType(), nullable=True),
        StructField("review_comment_message", StringType(), nullable=True),
        StructField("review_creation_date", TimestampType(), nullable=True),
        StructField("review_answer_timestamp", TimestampType(), nullable=True),
    ]),
    "customers": StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("customer_unique_id", StringType(), nullable=False),
        StructField("customer_zip_code_prefix", StringType(), nullable=True),
        StructField("customer_city", StringType(), nullable=True),
        StructField("customer_state", StringType(), nullable=True),
    ]),
    "products": StructType([
        StructField("product_id", StringType(), nullable=False),
        StructField("product_category_name", StringType(), nullable=True),
        StructField("product_name_lenght", IntegerType(), nullable=True),
        StructField("product_description_lenght", IntegerType(), nullable=True),
        StructField("product_photos_qty", IntegerType(), nullable=True),
        StructField("product_weight_g", DoubleType(), nullable=True),
        StructField("product_length_cm", DoubleType(), nullable=True),
        StructField("product_height_cm", DoubleType(), nullable=True),
        StructField("product_width_cm", DoubleType(), nullable=True),
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType(), nullable=False),
        StructField("seller_zip_code_prefix", StringType(), nullable=True),
        StructField("seller_city", StringType(), nullable=True),
        StructField("seller_state", StringType(), nullable=True),
    ]),
    "geolocation": StructType([
        StructField("geolocation_zip_code_prefix", StringType(), nullable=True),
        StructField("geolocation_lat", DoubleType(), nullable=True),
        StructField("geolocation_lng", DoubleType(), nullable=True),
        StructField("geolocation_city", StringType(), nullable=True),
        StructField("geolocation_state", StringType(), nullable=True),
    ]),
    "category_translation": StructType([
        StructField("product_category_name", StringType(), nullable=False),
        StructField("product_category_name_english", StringType(), nullable=True),
    ]),
}

DATASET_FILENAMES: Dict[str, str] = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}