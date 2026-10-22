-- include/sql/mart_category_margin.sql
-- Business question 2: revenue and gross margin by product category.
-- margin = revenue - quantity * cost_price.
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    COALESCE(p.category, 'UNKNOWN')                           AS category,
    ROUND(SUM(o.order_amount), 2)                             AS revenue,
    ROUND(SUM(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id AND p.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY COALESCE(p.category, 'UNKNOWN')
ORDER BY revenue DESC;
