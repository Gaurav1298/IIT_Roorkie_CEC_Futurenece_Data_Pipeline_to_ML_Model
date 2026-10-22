-- include/sql/mart_daily_store_kpis.sql
-- Business question 1: daily net revenue by zone and store (delivered only).
-- Only clean rows (dq_status='PASS') feed the mart.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    s.store_name,
    COUNT(*)                                  AS delivered_orders,
    ROUND(SUM(o.order_amount), 2)             AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id AND s.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, o.store_id, s.store_name
ORDER BY o.order_date, s.zone, o.store_id;
