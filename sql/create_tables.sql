-- ============================================================================
-- RetailPulse: Manual External Table DDL (Zero Glue Crawler Cost)
-- Database: retailpulse_dw
-- Format  : Apache Parquet (Snappy compressed)
-- ============================================================================

-- 1. DIM_DATE
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.dim_date (
    date_key        INT,
    full_date       DATE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      STRING,
    year_month      STRING,
    week_of_year    INT,
    day_of_month    INT,
    day_of_week     INT,
    day_name        STRING,
    is_weekend      BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/dim_date/';

-- 2. DIM_CUSTOMERS
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.dim_customers (
    customer_id         STRING,
    customer_unique_id  STRING,
    zip_code_prefix     STRING,
    city                STRING,
    state               STRING
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/dim_customers/';

-- 3. DIM_PRODUCTS
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.dim_products (
    product_id                  STRING,
    category_portuguese         STRING,
    category_english            STRING,
    product_name_length         DOUBLE,
    product_description_length  DOUBLE,
    product_photos_qty          DOUBLE,
    product_weight_g            DOUBLE,
    product_length_cm           DOUBLE,
    product_height_cm           DOUBLE,
    product_width_cm            DOUBLE
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/dim_products/';

-- 4. DIM_SELLERS
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.dim_sellers (
    seller_id       STRING,
    zip_code_prefix STRING,
    city            STRING,
    state           STRING
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/dim_sellers/';

-- 5. DIM_GEOLOCATION
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.dim_geolocation (
    zip_code_prefix STRING,
    latitude        DOUBLE,
    longitude       DOUBLE,
    city            STRING,
    state           STRING
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/dim_geolocation/';

-- 6. FACT_ORDERS
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.fact_orders (
    order_id                        STRING,
    customer_id                     STRING,
    purchase_date_key               INT,
    order_status                    STRING,
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    is_delivered                    BOOLEAN,
    delivery_days                   DOUBLE,
    estimated_delivery_days         DOUBLE,
    delivery_delay_days             DOUBLE,
    is_late_delivery                BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/fact_orders/';

-- 7. FACT_ORDER_ITEMS
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.fact_order_items (
    order_id            STRING,
    order_item_id       INT,
    product_id          STRING,
    seller_id           STRING,
    customer_id         STRING,
    purchase_date_key   INT,
    order_status        STRING,
    shipping_limit_date TIMESTAMP,
    price               DOUBLE,
    freight_value       DOUBLE,
    total_item_value    DOUBLE
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/fact_order_items/';

-- 8. FACT_PAYMENTS
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.fact_payments (
    order_id                STRING,
    payment_sequential      INT,
    customer_id             STRING,
    purchase_date_key       INT,
    payment_type            STRING,
    payment_installments    INT,
    payment_value           DOUBLE
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/fact_payments/';

-- 9. FACT_REVIEWS
CREATE EXTERNAL TABLE IF NOT EXISTS retailpulse_dw.fact_reviews (
    review_id               STRING,
    order_id                STRING,
    customer_id             STRING,
    review_date_key         INT,
    review_score            INT,
    has_comment             BOOLEAN,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/fact_reviews/';