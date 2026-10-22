-- include/sql/stg_products.sql
-- ELT staging for products: cast, dedupe, tag dq_status + failure_reason.
-- Critical (FAIL): duplicate id, non-positive mrp.
-- Warning (PASS): category_missing (-> 'UNKNOWN'), cost_gt_mrp. This matches the
-- ETL pipeline so an order with an uncategorised product still counts (UNKNOWN).
CREATE OR REPLACE TABLE stg_products AS
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at) AS rn
    FROM raw_products
),
typed AS (
    SELECT
        product_id,
        product_name,
        COALESCE(NULLIF(TRIM(category), ''), 'UNKNOWN') AS category,
        sub_category,
        brand,
        TRY_CAST(mrp AS DOUBLE)        AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        unit,
        is_perishable,
        TRY_CAST(launch_date AS DATE)  AS launch_date,
        (rn > 1)                       AS _is_dup,
        (category IS NULL OR TRIM(category) = '') AS _cat_missing,
        _loaded_at, _source_file, _run_id
    FROM deduped
)
SELECT
    product_id, product_name, category, sub_category, brand, mrp, cost_price,
    unit, is_perishable, launch_date, _loaded_at, _source_file, _run_id,
    NULLIF(TRIM(CONCAT_WS(';',
        CASE WHEN _is_dup THEN 'duplicate_product_id' END,
        CASE WHEN _cat_missing THEN 'category_missing' END,
        CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'mrp_non_positive' END,
        CASE WHEN cost_price > mrp THEN 'cost_gt_mrp' END
    )), '')                            AS failure_reason,
    CASE
        WHEN _is_dup THEN 'FAIL'
        WHEN mrp IS NULL OR mrp <= 0 THEN 'FAIL'
        ELSE 'PASS'
    END                                AS dq_status
FROM typed;
