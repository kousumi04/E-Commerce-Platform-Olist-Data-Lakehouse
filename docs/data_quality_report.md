# Olist Dataset — Automated Data Quality Report

## 1. Table Summary & Primary Key Uniqueness

| File | Rows | Columns | Exact Duplicates | Candidate PK | PK Duplicates |
| :--- | ---: | ---: | ---: | :--- | ---: |
| `olist_orders_dataset.csv` | 99,441 | 8 | 0 | `order_id` | 0 |
| `olist_order_items_dataset.csv` | 112,650 | 7 | 0 | `order_id, order_item_id` | 0 |
| `olist_order_payments_dataset.csv` | 103,886 | 5 | 0 | `order_id, payment_sequential` | 0 |
| `olist_order_reviews_dataset.csv` | 99,224 | 7 | 0 | `review_id` | 814 |
| `olist_customers_dataset.csv` | 99,441 | 5 | 0 | `customer_id` | 0 |
| `olist_products_dataset.csv` | 32,951 | 9 | 0 | `product_id` | 0 |
| `olist_sellers_dataset.csv` | 3,095 | 4 | 0 | `seller_id` | 0 |
| `olist_geolocation_dataset.csv` | 1,000,163 | 5 | 261,831 | `None (zip prefix)` | N/A |
| `product_category_name_translation.csv` | 71 | 2 | 0 | `product_category_name` | 0 |

## 2. Missing Values Report

- **`olist_orders_dataset.csv`**:
  - `order_approved_at`: 160 nulls (0.16%)
  - `order_delivered_carrier_date`: 1,783 nulls (1.79%)
  - `order_delivered_customer_date`: 2,965 nulls (2.98%)
- **`olist_order_items_dataset.csv`**: No missing values.
- **`olist_order_payments_dataset.csv`**: No missing values.
- **`olist_order_reviews_dataset.csv`**:
  - `review_comment_title`: 87,656 nulls (88.34%)
  - `review_comment_message`: 58,247 nulls (58.7%)
- **`olist_customers_dataset.csv`**: No missing values.
- **`olist_products_dataset.csv`**:
  - `product_category_name`: 610 nulls (1.85%)
  - `product_name_lenght`: 610 nulls (1.85%)
  - `product_description_lenght`: 610 nulls (1.85%)
  - `product_photos_qty`: 610 nulls (1.85%)
  - `product_weight_g`: 2 nulls (0.01%)
  - `product_length_cm`: 2 nulls (0.01%)
  - `product_height_cm`: 2 nulls (0.01%)
  - `product_width_cm`: 2 nulls (0.01%)
- **`olist_sellers_dataset.csv`**: No missing values.
- **`olist_geolocation_dataset.csv`**: No missing values.
- **`product_category_name_translation.csv`**: No missing values.

## 3. Domain & Grain Anomaly Findings

- **Orders without line items:** 775 orders (mostly canceled/unavailable/created statuses).
- **Delivered orders missing delivery date:** 8 rows (must be excluded from delivery-duration calculations).
- **Order item price/freight anomalies:** 0 rows with price <= 0; 0 rows with freight_value < 0.
- **Zero or negative payment rows:** 9 rows in payments.
- **Customer identity grain:** 99,441 order-level `customer_id`s map to 96,096 `customer_unique_id`s (~3.48% repeat-order ratio).
- **Duplicate `review_id`s:** 814 duplicate rows (a single review_id can span multiple orders; composite key is `(review_id, order_id)`).
- **Untranslated Portuguese categories:** 2 categories (pc_gamer, portateis_cozinha_e_preparadores_de_alimentos).
- **1-to-Many fan-out risk:** 9,803 orders have multiple items, 2,961 have multiple payment rows, and 547 have multiple reviews. Joining these tables directly will inflate revenue.
