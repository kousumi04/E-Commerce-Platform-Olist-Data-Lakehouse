-- ============================================================================
-- RetailPulse: Executive Sales Analytics
-- ============================================================================

-- 1. Headline KPIs (Item GMV, Total Orders, AOV, Cancellation Rate)
SELECT
    COUNT(DISTINCT o.order_id)                                              AS total_orders,
    SUM(CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END)           AS delivered_orders,
    ROUND(
        100.0 * SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END)
        / COUNT(DISTINCT o.order_id), 2
    )                                                                       AS cancellation_rate_pct
FROM retailpulse_dw.fact_orders o;

-- 2. Total Item GMV, Freight, and Average Order Value (AOV)
SELECT
    ROUND(SUM(price), 2)                                    AS total_item_gmv,
    ROUND(SUM(freight_value), 2)                            AS total_freight,
    ROUND(SUM(total_item_value), 2)                         AS total_gross_revenue,
    ROUND(SUM(price) / COUNT(DISTINCT order_id), 2)         AS avg_order_value_gmv,
    ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT order_id), 2) AS avg_items_per_order
FROM retailpulse_dw.fact_order_items;

-- 3. Monthly Revenue & Month-over-Month (MoM) Growth %
WITH monthly_sales AS (
    SELECT
        d.year_month,
        COUNT(DISTINCT i.order_id)  AS total_orders,
        ROUND(SUM(i.price), 2)      AS monthly_gmv
    FROM retailpulse_dw.fact_order_items i
    JOIN retailpulse_dw.dim_date d
        ON i.purchase_date_key = d.date_key
    GROUP BY d.year_month
)
SELECT
    year_month,
    total_orders,
    monthly_gmv,
    LAG(monthly_gmv) OVER (ORDER BY year_month) AS prev_month_gmv,
    ROUND(
        100.0 * (monthly_gmv - LAG(monthly_gmv) OVER (ORDER BY year_month))
        / NULLIF(LAG(monthly_gmv) OVER (ORDER BY year_month), 0),
        2
    ) AS mom_growth_pct
FROM monthly_sales
ORDER BY year_month;