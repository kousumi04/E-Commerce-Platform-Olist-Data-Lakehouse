# RetailPulse — Olist Dataset Data Dictionary & ER Diagram

## 1. Entity-Relationship Diagram (Source Schema)

```mermaid
erDiagram
    ORDERS ||--o{ ORDER_ITEMS : "has (1:N)"
    ORDERS ||--o{ ORDER_PAYMENTS : "paid by (1:N)"
    ORDERS ||--o{ ORDER_REVIEWS : "reviewed by (1:N)"
    CUSTOMERS ||--|| ORDERS : "places (1:1 per customer_id)"
    PRODUCTS ||--o{ ORDER_ITEMS : "included in (1:N)"
    SELLERS ||--o{ ORDER_ITEMS : "fulfilled by (1:N)"
    CATEGORY_TRANSLATION ||--o{ PRODUCTS : "translates (1:N)"
    GEOLOCATION }o--o{ CUSTOMERS : "zip_code_prefix (M:N)"
    GEOLOCATION }o--o{ SELLERS : "zip_code_prefix (M:N)"