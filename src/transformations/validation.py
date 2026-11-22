"""
Data quality and referential integrity validation for cleaned Olist tables.
"""

from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

PRIMARY_KEYS: Dict[str, List[str]] = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "category_translation": ["product_category_name"],
}

# (child_table, child_fk, parent_table, parent_pk)
FOREIGN_KEYS: List[Tuple[str, str, str, str]] = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("reviews", "order_id", "orders", "order_id"),
]


def validate_cleaned_tables(cleaned_tables: Dict[str, DataFrame]) -> Dict[str, Dict]:
    """
    Validate primary key uniqueness, non-empty counts, referential integrity,
    and core business rules across all cleaned tables.

    Raises:
        ValueError: If any critical uniqueness or business rule check fails.
    """
    report: Dict[str, Dict] = {"tables": {}, "referential_integrity": {}, "business_rules": {}}
    errors: List[str] = []

    # 1. Primary key uniqueness & non-empty checks
    for table_name, pk_cols in PRIMARY_KEYS.items():
        df = cleaned_tables[table_name]
        total_rows = df.count()
        distinct_pks = df.select(pk_cols).distinct().count()
        pk_dupes = total_rows - distinct_pks

        report["tables"][table_name] = {
            "rows": total_rows,
            "pk_cols": pk_cols,
            "pk_duplicates": pk_dupes,
        }

        if total_rows == 0:
            errors.append(f"Table '{table_name}' has 0 rows after cleaning.")
        if pk_dupes > 0:
            errors.append(f"Table '{table_name}' has {pk_dupes} duplicate primary keys on {pk_cols}.")

    # 2. Referential integrity checks (left_anti join finds orphan records)
    for child_tbl, child_fk, parent_tbl, parent_pk in FOREIGN_KEYS:
        orphans = (
            cleaned_tables[child_tbl]
            .join(
                cleaned_tables[parent_tbl].select(F.col(parent_pk).alias("_parent_key")),
                cleaned_tables[child_tbl][child_fk] == F.col("_parent_key"),
                how="left_anti",
            )
            .count()
        )
        key_label = f"{child_tbl}.{child_fk} -> {parent_tbl}.{parent_pk}"
        report["referential_integrity"][key_label] = orphans
        if orphans > 0:
            errors.append(f"Referential integrity violation: {orphans} orphans in {key_label}.")

    # 3. Business rule checks
    invalid_prices = cleaned_tables["order_items"].filter(F.col("price") <= 0).count()
    invalid_scores = (
        cleaned_tables["reviews"].filter(~F.col("review_score").between(1, 5)).count()
    )
    report["business_rules"] = {
        "order_items_invalid_price": invalid_prices,
        "reviews_invalid_score": invalid_scores,
    }
    if invalid_prices > 0 or invalid_scores > 0:
        errors.append(
            f"Business rule failure: invalid_prices={invalid_prices}, invalid_scores={invalid_scores}"
        )

    if errors:
        for err in errors:
            logger.error("DQ FAILURE: %s", err)
        raise ValueError("Data quality validation failed: " + " | ".join(errors))

    logger.info("All Data Quality and Referential Integrity checks PASSED.")
    return report