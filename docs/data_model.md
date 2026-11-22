# RetailPulse — Dimensional Data Model (Star Schema)

## Star Schema Overview
RetailPulse uses a **4-Fact, 5-Dimension Star Schema** stored in Snappy-compressed Apache Parquet under `curated/`.

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_ORDERS : "purchase_date_key"
    DIM_DATE ||--o{ FACT_ORDER_ITEMS : "purchase_date_key"
    DIM_DATE ||--o{ FACT_PAYMENTS : "purchase_date_key"
    DIM_DATE ||--o{ FACT_REVIEWS : "review_date_key"
    DIM_CUSTOMERS ||--o{ FACT_ORDERS : "customer_id"
    DIM_CUSTOMERS ||--o{ FACT_ORDER_ITEMS : "customer_id"
    DIM_CUSTOMERS ||--o{ FACT_PAYMENTS : "customer_id"
    DIM_CUSTOMERS ||--o{ FACT_REVIEWS : "customer_id"
    DIM_PRODUCTS ||--o{ FACT_ORDER_ITEMS : "product_id"
    DIM_SELLERS ||--o{ FACT_ORDER_ITEMS : "seller_id"
    DIM_GEOLOCATION ||--o{ DIM_CUSTOMERS : "zip_code_prefix"
    DIM_GEOLOCATION ||--o{ DIM_SELLERS : "zip_code_prefix"