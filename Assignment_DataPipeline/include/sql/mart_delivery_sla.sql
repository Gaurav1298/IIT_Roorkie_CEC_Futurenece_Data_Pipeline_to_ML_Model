-- include/sql/mart_delivery_sla.sql
-- Business question 3: average delivery time per store and % of delivered
-- orders within 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    s.zone,
    COUNT(*)                                                   AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 2)                          AS avg_delivery_minutes,
    ROUND(100.0 * SUM(CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                               AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id AND s.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY o.store_id, s.store_name, s.zone
ORDER BY pct_within_15_min DESC;
