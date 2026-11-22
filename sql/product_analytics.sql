-- ============================================================================
-- RetailPulse: Product, Category & Seller Intelligence
-- ============================================================================

-- 1. Top 15 Categories by Revenue, Units Sold & Avg Price
SELECT
    p.category_english,
    COUNT(*)                            AS units_sold,
    COUNT(DISTINCT i.order_id)          AS total_orders,
    ROUND(SUM(i.price), 2)              AS total_revenue,
    ROUND(AVG(i.price), 2)              AS avg_unit_price
FROM retailpulse_dw.fact_order_items i
JOIN retailpulse_dw.dim_products p
    ON i.product_id = p.product_id
GROUP BY p.category_english
ORDER BY total_revenue DESC
LIMIT 15;

-- 2. Category Average Review Score (Aggregated safely at order level first)
WITH order_categories AS (
    SELECT DISTINCT
        i.order_id,
        p.category_english
    FROM retailpulse_dw.fact_order_items i
    JOIN retailpulse_dw.dim_products p
        ON i.product_id = p.product_id
)
SELECT
    oc.category_english,
    COUNT(r.review_id)                  AS review_count,
    ROUND(AVG(r.review_score), 2)       AS avg_review_score
FROM order_categories oc
JOIN retailpulse_dw.fact_reviews r
    ON oc.order_id = r.order_id
GROUP BY oc.category_english
HAVING COUNT(r.review_id) >= 50
ORDER BY avg_review_score DESC;

-- 3. Top 10 Sellers by Revenue
SELECT
    s.seller_id,
    s.city,
    s.state,
    COUNT(DISTINCT i.order_id)          AS orders_fulfilled,
    COUNT(*)                            AS units_sold,
    ROUND(SUM(i.price), 2)              AS total_seller_revenue
FROM retailpulse_dw.fact_order_items i
JOIN retailpulse_dw.dim_sellers s
    ON i.seller_id = s.seller_id
GROUP BY s.seller_id, s.city, s.state
ORDER BY total_seller_revenue DESC
LIMIT 10;