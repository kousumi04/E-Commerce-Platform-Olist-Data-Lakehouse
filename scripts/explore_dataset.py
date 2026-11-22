"""
Olist E-Commerce Dataset Profiler & Data Quality Auditor.

Inspects all 9 raw Olist CSV files in DATASET_PATH, checks primary key
uniqueness, missing values, exact duplicates, and domain-specific anomalies,
and writes a Markdown report to docs/data_quality_report.md.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Expected Olist files and their candidate primary keys
EXPECTED_FILES: Dict[str, Optional[List[str]]] = {
    "olist_orders_dataset.csv": ["order_id"],
    "olist_order_items_dataset.csv": ["order_id", "order_item_id"],
    "olist_order_payments_dataset.csv": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset.csv": ["review_id"],
    "olist_customers_dataset.csv": ["customer_id"],
    "olist_products_dataset.csv": ["product_id"],
    "olist_sellers_dataset.csv": ["seller_id"],
    "olist_geolocation_dataset.csv": None,  # Known many-to-many zip prefix table
    "product_category_name_translation.csv": ["product_category_name"],
}


def validate_files_exist(data_dir: Path) -> None:
    """Verify all 9 expected Olist CSV files exist in data_dir."""
    missing = [f for f in EXPECTED_FILES if not (data_dir / f).is_file()]
    if missing:
        logger.error("Missing required CSV files in %s: %s", data_dir, missing)
        sys.exit(1)
    logger.info("All 9 expected Olist CSV files found in %s.", data_dir)


def profile_dataframe(
    filename: str, df: pd.DataFrame, pk_cols: Optional[List[str]]
) -> Dict:
    """Compute structural, missing-value, and duplicate metrics for one table."""
    row_count = len(df)
    col_count = len(df.columns)
    exact_dupes = int(df.duplicated().sum())

    # Primary key uniqueness check
    pk_dupes = None
    if pk_cols:
        pk_dupes = int(df.duplicated(subset=pk_cols).sum())

    # Missing value breakdown
    null_counts = df.isnull().sum()
    null_cols = {
        col: {
            "count": int(cnt),
            "pct": round((cnt / row_count) * 100, 2) if row_count > 0 else 0.0,
        }
        for col, cnt in null_counts.items()
        if cnt > 0
    }

    return {
        "filename": filename,
        "rows": row_count,
        "columns": col_count,
        "exact_dupes": exact_dupes,
        "pk_cols": pk_cols,
        "pk_dupes": pk_dupes,
        "null_cols": null_cols,
    }


def run_business_anomaly_checks(dfs: Dict[str, pd.DataFrame]) -> List[str]:
    """Run cross-table and domain-specific data quality checks."""
    findings: List[str] = []

    orders = dfs["olist_orders_dataset.csv"]
    items = dfs["olist_order_items_dataset.csv"]
    payments = dfs["olist_order_payments_dataset.csv"]
    reviews = dfs["olist_order_reviews_dataset.csv"]
    customers = dfs["olist_customers_dataset.csv"]
    products = dfs["olist_products_dataset.csv"]
    translations = dfs["product_category_name_translation.csv"]

    # 1. Orders without matching order_items
    orders_without_items = (~orders["order_id"].isin(items["order_id"])).sum()
    findings.append(
        f"**Orders without line items:** {orders_without_items:,} orders "
        f"(mostly canceled/unavailable/created statuses)."
    )

    # 2. Delivered orders missing actual delivery timestamp
    delivered = orders[orders["order_status"] == "delivered"]
    missing_delivery_ts = delivered["order_delivered_customer_date"].isnull().sum()
    findings.append(
        f"**Delivered orders missing delivery date:** {missing_delivery_ts:,} rows "
        f"(must be excluded from delivery-duration calculations)."
    )

    # 3. Non-positive prices or negative freight
    zero_or_neg_price = (items["price"] <= 0).sum()
    neg_freight = (items["freight_value"] < 0).sum()
    findings.append(
        f"**Order item price/freight anomalies:** {zero_or_neg_price:,} rows with price <= 0; "
        f"{neg_freight:,} rows with freight_value < 0."
    )

    # 4. Zero-value payments
    zero_payments = (payments["payment_value"] <= 0).sum()
    findings.append(
        f"**Zero or negative payment rows:** {zero_payments:,} rows in payments."
    )

    # 5. Customer ID vs Unique Customer ID
    total_cust_ids = customers["customer_id"].nunique()
    unique_cust_ids = customers["customer_unique_id"].nunique()
    repeat_ratio = round(((total_cust_ids - unique_cust_ids) / unique_cust_ids) * 100, 2)
    findings.append(
        f"**Customer identity grain:** {total_cust_ids:,} order-level `customer_id`s map to "
        f"{unique_cust_ids:,} `customer_unique_id`s (~{repeat_ratio}% repeat-order ratio)."
    )

    # 6. Duplicate review_ids (one review_id linked to multiple orders)
    dup_review_ids = reviews["review_id"].duplicated().sum()
    findings.append(
        f"**Duplicate `review_id`s:** {dup_review_ids:,} duplicate rows "
        f"(a single review_id can span multiple orders; composite key is `(review_id, order_id)`)."
    )

    # 7. Untranslated product categories
    prod_cats = set(products["product_category_name"].dropna().unique())
    trans_cats = set(translations["product_category_name"].dropna().unique())
    untranslated = sorted(prod_cats - trans_cats)
    findings.append(
        f"**Untranslated Portuguese categories:** {len(untranslated)} categories "
        f"({', '.join(untranslated) if untranslated else 'None'})."
    )

    # 8. One-to-many fan-out demonstration
    multi_item_orders = (items.groupby("order_id").size() > 1).sum()
    multi_pay_orders = (payments.groupby("order_id").size() > 1).sum()
    multi_rev_orders = (reviews.groupby("order_id").size() > 1).sum()
    findings.append(
        f"**1-to-Many fan-out risk:** {multi_item_orders:,} orders have multiple items, "
        f"{multi_pay_orders:,} have multiple payment rows, and {multi_rev_orders:,} have multiple reviews. "
        f"Joining these tables directly will inflate revenue."
    )

    return findings


def write_markdown_report(
    profiles: List[Dict], anomalies: List[str], output_path: Path
) -> None:
    """Write the complete profiling & data quality report to Markdown."""
    lines: List[str] = [
        "# Olist Dataset — Automated Data Quality Report",
        "",
        "## 1. Table Summary & Primary Key Uniqueness",
        "",
        "| File | Rows | Columns | Exact Duplicates | Candidate PK | PK Duplicates |",
        "| :--- | ---: | ---: | ---: | :--- | ---: |",
    ]

    for p in profiles:
        pk_str = ", ".join(p["pk_cols"]) if p["pk_cols"] else "None (zip prefix)"
        pk_dupes_str = str(p["pk_dupes"]) if p["pk_dupes"] is not None else "N/A"
        lines.append(
            f"| `{p['filename']}` | {p['rows']:,} | {p['columns']} | "
            f"{p['exact_dupes']:,} | `{pk_str}` | {pk_dupes_str} |"
        )

    lines.extend(["", "## 2. Missing Values Report", ""])
    for p in profiles:
        if not p["null_cols"]:
            lines.append(f"- **`{p['filename']}`**: No missing values.")
        else:
            lines.append(f"- **`{p['filename']}`**:")
            for col, stats in p["null_cols"].items():
                lines.append(f"  - `{col}`: {stats['count']:,} nulls ({stats['pct']}%)")

    lines.extend(["", "## 3. Domain & Grain Anomaly Findings", ""])
    for finding in anomalies:
        lines.append(f"- {finding}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Data quality report saved to %s", output_path)


def main() -> None:
    """Run full dataset profiling workflow."""
    load_dotenv()
    dataset_path = Path(os.getenv("DATASET_PATH", "data/raw"))
    report_path = Path("docs/data_quality_report.md")

    validate_files_exist(dataset_path)

    dfs: Dict[str, pd.DataFrame] = {}
    profiles: List[Dict] = []

    for filename, pk_cols in EXPECTED_FILES.items():
        file_path = dataset_path / filename
        logger.info("Profiling %s ...", filename)
        try:
            df = pd.read_csv(file_path, low_memory=False)
        except Exception as exc:
            logger.exception("Failed to read %s: %s", file_path, exc)
            sys.exit(1)

        dfs[filename] = df
        profiles.append(profile_dataframe(filename, df, pk_cols))

    anomalies = run_business_anomaly_checks(dfs)
    write_markdown_report(profiles, anomalies, report_path)

    # Print quick console summary
    print("\n=== OLIST DATASET PROFILE SUMMARY ===")
    for p in profiles:
        print(
            f"{p['filename']:42s} | Rows: {p['rows']:>9,} | "
            f"Cols: {p['columns']:>2} | Exact Dupes: {p['exact_dupes']:>7,} | "
            f"PK Dupes: {str(p['pk_dupes']):>5}"
        )
    print("\n=== KEY ANOMALIES ===")
    for item in anomalies:
        print("-", item.replace("**", ""))


if __name__ == "__main__":
    main()