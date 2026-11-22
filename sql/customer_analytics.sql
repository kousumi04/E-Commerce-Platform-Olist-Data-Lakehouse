-- ============================================================================
-- RetailPulse: Customer & Payment Intelligence
-- ============================================================================

-- 1. Unique Customers & Repeat Purchase Rate (using customer_unique_id)
WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM retailpulse_dw.fact_orders o
    JOIN retailpulse_dw.dim_customers c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
)
SELECT
    COUNT(*)                                                                AS total_unique_customers,
    SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END)                        AS repeat_customers,
    ROUND(100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_purchase_rate_pct,
    ROUND(AVG(CAST(order_count AS DOUBLE)), 3)                              AS avg_orders_per_customer
FROM customer_orders;

-- 2. Top 10 Brazilian States by Revenue & Unique Customers
SELECT
    c.state,
    COUNT(DISTINCT c.customer_unique_id)    AS unique_customers,
    COUNT(DISTINCT i.order_id)              AS total_orders,
    ROUND(SUM(i.price), 2)                  AS total_gmv
FROM retailpulse_dw.fact_order_items i
JOIN retailpulse_dw.dim_customers c
    ON i.customer_id = c.customer_id
GROUP BY c.state
ORDER BY total_gmv DESC
LIMIT 10;

-- 3. Payment Method Distribution & Average Installments
SELECT
    payment_type,
    COUNT(*)                                AS transaction_count,
    ROUND(SUM(payment_value), 2)            AS total_payment_value,
    ROUND(AVG(payment_value), 2)            AS avg_payment_value,
    ROUND(AVG(CAST(payment_installments AS DOUBLE)), 2) AS avg_installments
FROM retailpulse_dw.fact_payments
GROUP BY payment_type
ORDER BY total_payment_value DESC;