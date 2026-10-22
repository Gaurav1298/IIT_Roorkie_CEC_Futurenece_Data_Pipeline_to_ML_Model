-- include/sql/stg_orders.sql
-- ELT staging layer for orders. SQL-only cleaning (no pandas).
-- Casts types, standardises values, applies the same quality rules as the ETL
-- pipeline, and tags each row with dq_status ('PASS'/'FAIL') + failure_reason.
-- Depends on stg_customers, stg_products, stg_stores existing first.
CREATE OR REPLACE TABLE stg_orders AS
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at) AS rn
    FROM raw_orders
),
typed AS (
    SELECT
        order_id,
        TRY_CAST(order_ts AS TIMESTAMP)                           AS order_ts,
        customer_id,
        store_id,
        product_id,
        TRY_CAST(quantity AS INTEGER)                             AS quantity,
        TRY_CAST(unit_price AS DOUBLE)                            AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE)                          AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE)                          AS order_amount,
        CASE
            WHEN LOWER(TRIM(payment_mode)) = 'upi'  THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) = 'card' THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) IN ('cod','cash on delivery') THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END                                                       AS payment_mode,
        CASE
            WHEN UPPER(TRIM(order_status)) IN ('DELIVERED','CANCELLED','RETURNED')
                 THEN UPPER(TRIM(order_status))
            ELSE TRIM(order_status)
        END                                                       AS order_status,
        channel,
        TRY_CAST(delivery_minutes AS DOUBLE)                      AS delivery_minutes,
        _loaded_at, _source_file, _run_id
    FROM deduped
    WHERE rn = 1
),
checked AS (
    SELECT
        t.*,
        CAST(t.order_ts AS DATE)                                  AS order_date,
        NULLIF(TRIM(CONCAT_WS(';',
            CASE WHEN t.customer_id IS NULL OR t.customer_id = '' THEN 'customer_id_missing' END,
            CASE WHEN t.order_ts IS NULL THEN 'order_ts_missing_or_unparseable' END,
            CASE WHEN t.order_status = 'DELIVERED' AND t.delivery_minutes IS NULL THEN 'delivered_no_delivery_minutes' END,
            CASE WHEN t.order_status NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'status_invalid' END,
            CASE WHEN t.payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'payment_mode_invalid' END,
            CASE WHEN t.discount_pct < 0 OR t.discount_pct > 100 THEN 'discount_out_of_range' END,
            CASE WHEN t.quantity IS NULL OR t.quantity <= 0 THEN 'quantity_non_positive' END,
            CASE WHEN ABS(ROUND(t.quantity * t.unit_price * (1 - t.discount_pct/100.0), 2) - t.order_amount) > 0.01 THEN 'amount_formula_mismatch' END,
            CASE WHEN t.order_amount < 0 THEN 'amount_negative' END,
            CASE WHEN t.delivery_minutes > 120 THEN 'delivery_minutes_impossible' END,
            CASE WHEN c.customer_id IS NULL THEN 'customer_not_in_master' END,
            CASE WHEN p.product_id IS NULL THEN 'product_not_in_master' END,
            CASE WHEN s.store_id IS NULL THEN 'store_not_in_master' END,
            CASE WHEN t.order_ts > NOW() THEN 'future_dated' END
        )), '')                                                   AS failure_reason
    FROM typed t
    LEFT JOIN stg_customers c ON t.customer_id = c.customer_id AND c.dq_status = 'PASS'
    LEFT JOIN stg_products  p ON t.product_id  = p.product_id  AND p.dq_status = 'PASS'
    LEFT JOIN stg_stores    s ON t.store_id    = s.store_id    AND s.dq_status = 'PASS'
)
SELECT
    *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END  AS dq_status
FROM checked;
