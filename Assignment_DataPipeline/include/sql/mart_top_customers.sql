-- include/sql/mart_top_customers.sql
-- Business question 5: top 10 customers by spend with loyalty tier.
-- Uses the MASKED customer dimension: no raw email/phone exposed.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    o.customer_id,
    c.loyalty_tier,
    c.email       AS masked_email,
    c.phone       AS masked_phone,
    COUNT(*)                        AS delivered_orders,
    ROUND(SUM(o.order_amount), 2)   AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id AND c.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.loyalty_tier, c.email, c.phone
ORDER BY total_spend DESC
LIMIT 10;
