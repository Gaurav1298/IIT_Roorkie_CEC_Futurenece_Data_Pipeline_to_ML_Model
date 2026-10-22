-- include/sql/stg_payments.sql
-- ELT staging for payments: cast, dedupe, standardise payment_mode.
CREATE OR REPLACE TABLE stg_payments AS
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at) AS rn
    FROM raw_payments
)
SELECT
    payment_id,
    order_id,
    TRY_CAST(payment_ts AS TIMESTAMP)             AS payment_ts,
    CASE
        WHEN LOWER(TRIM(payment_mode)) = 'upi'  THEN 'UPI'
        WHEN LOWER(TRIM(payment_mode)) = 'card' THEN 'Card'
        WHEN LOWER(TRIM(payment_mode)) IN ('cod','cash on delivery') THEN 'Cash on Delivery'
        WHEN LOWER(TRIM(payment_mode)) = 'wallet' THEN 'Wallet'
        ELSE TRIM(payment_mode)
    END                                            AS payment_mode,
    TRY_CAST(amount_paid AS DOUBLE)                AS amount_paid,
    payment_status,
    gateway_ref,
    NULLIF(TRIM(CONCAT_WS(';',
        CASE WHEN rn > 1 THEN 'duplicate_payment_id' END,
        CASE WHEN TRY_CAST(payment_ts AS TIMESTAMP) IS NULL THEN 'payment_ts_unparseable' END
    )), '')                                        AS failure_reason,
    CASE WHEN rn > 1 THEN 'FAIL' ELSE 'PASS' END   AS dq_status
FROM deduped;
