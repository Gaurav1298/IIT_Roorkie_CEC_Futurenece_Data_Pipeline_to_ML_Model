-- include/sql/stg_stores.sql
-- ELT staging for stores: cast, dedupe, tag dq_status + failure_reason.
-- Null zone / zero capacity are Warning-level -> rows still PASS.
CREATE OR REPLACE TABLE stg_stores AS
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _loaded_at) AS rn
    FROM raw_stores
),
typed AS (
    SELECT
        store_id,
        store_name,
        COALESCE(NULLIF(TRIM(zone), ''), 'UNKNOWN') AS zone,
        area,
        pincode,
        TRY_CAST(open_date AS DATE)                 AS open_date,
        manager_name,
        TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
        (zone IS NULL OR TRIM(zone) = '')           AS _zone_missing,
        _loaded_at, _source_file, _run_id
    FROM deduped
    WHERE rn = 1
)
SELECT
    * EXCLUDE (_zone_missing),
    NULLIF(TRIM(CONCAT_WS(';',
        CASE WHEN _zone_missing THEN 'zone_missing' END,
        CASE WHEN capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0 THEN 'capacity_non_positive' END
    )), '')                                         AS failure_reason,
    'PASS'                                           AS dq_status
FROM typed;
