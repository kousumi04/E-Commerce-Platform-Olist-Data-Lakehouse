-- ============================================================================
-- RetailPulse: Logistics & Delivery Performance
-- ============================================================================

-- 1. Overall Delivery KPIs (Strictly delivered orders with valid delivery timestamp)
SELECT
    COUNT(*)                                                                    AS delivered_orders,
    SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END)                           AS late_deliveries,
    ROUND(100.0 * SUM(CASE WHEN NOT is_late_delivery THEN 1 ELSE 0 END) / COUNT(*), 2) AS on_time_delivery_rate_pct,
    ROUND(AVG(delivery_days), 2)                                                AS avg_actual_delivery_days,
    ROUND(AVG(estimated_delivery_days), 2)                                      AS avg_estimated_delivery_days,
    ROUND(AVG(delivery_delay_days), 2)                                          AS avg_delay_days
FROM retailpulse_dw.fact_orders
WHERE is_delivered = TRUE
  AND delivery_days IS NOT NULL;

-- 2. Delivery Performance & Freight Cost by Customer State
WITH state_freight AS (
    SELECT
        c.state,
        ROUND(AVG(i.freight_value), 2) AS avg_freight_cost
    FROM retailpulse_dw.fact_order_items i
    JOIN retailpulse_dw.dim_customers c
        ON i.customer_id = c.customer_id
    GROUP BY c.state
),
state_delivery AS (
    SELECT
        c.state,
        COUNT(*) AS delivered_orders,
        ROUND(AVG(o.delivery_days), 2) AS avg_delivery_days,
        ROUND(100.0 * SUM(CASE WHEN o.is_late_delivery THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_delivery_rate_pct
    FROM retailpulse_dw.fact_orders o
    JOIN retailpulse_dw.dim_customers c
        ON o.customer_id = c.customer_id
    WHERE o.is_delivered = TRUE
      AND o.delivery_days IS NOT NULL
    GROUP BY c.state
)
SELECT
    d.state,
    d.delivered_orders,
    d.avg_delivery_days,
    d.late_delivery_rate_pct,
    f.avg_freight_cost
FROM state_delivery d
JOIN state_freight f
    ON d.state = f.state
ORDER BY d.late_delivery_rate_pct DESC;

-- 3. Impact of Late Delivery on Customer Review Scores
SELECT
    CASE
        WHEN o.is_late_delivery THEN 'Late Delivery'
        ELSE 'On-Time Delivery'
    END                                 AS delivery_status,
    COUNT(r.review_id)                  AS review_count,
    ROUND(AVG(r.review_score), 2)       AS avg_review_score
FROM retailpulse_dw.fact_orders o
JOIN retailpulse_dw.fact_reviews r
    ON o.order_id = r.order_id
WHERE o.is_delivered = TRUE
  AND o.is_late_delivery IS NOT NULL
GROUP BY 1;