-- include/sql/mart_payment_reconciliation.sql
-- Business question 4 (part): revenue by payment mode + amounts that do NOT
-- reconcile with the gateway. One row per order/payment mismatch, with reason.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT
    p.payment_id,
    p.order_id,
    o.order_amount,
    p.amount_paid,
    p.payment_mode,
    p.payment_status,
    CASE
        WHEN o.order_id IS NULL THEN 'payment references unknown/quarantined order'
        WHEN p.payment_status = 'SUCCESS' AND ABS(p.amount_paid - o.order_amount) > 0.01
             THEN 'SUCCESS amount != order_amount'
        ELSE NULL
    END                                            AS reason
FROM stg_payments p
LEFT JOIN stg_orders o
       ON p.order_id = o.order_id AND o.dq_status = 'PASS'
WHERE p.dq_status = 'PASS'
  AND (
        o.order_id IS NULL
        OR (p.payment_status = 'SUCCESS' AND ABS(p.amount_paid - o.order_amount) > 0.01)
      )
ORDER BY p.order_id;
