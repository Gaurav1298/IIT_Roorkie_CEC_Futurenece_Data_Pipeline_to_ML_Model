-- include/sql/stg_customers.sql
-- ELT staging for customers: cast, dedupe, STANDARDISE loyalty_tier casing,
-- MASK PII (email + phone) in pure SQL, tag dq_status + failure_reason.
CREATE OR REPLACE TABLE stg_customers AS
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at) AS rn
    FROM raw_customers
),
typed AS (
    SELECT
        customer_id,
        full_name,
        -- MASK email: keep first 2 chars of local part + domain -> pr***@example.com
        CASE
            WHEN email IS NULL OR email = '' THEN ''
            WHEN POSITION('@' IN email) = 0 THEN '***'
            ELSE SUBSTR(SPLIT_PART(email, '@', 1), 1, 2) || '***@' || SPLIT_PART(email, '@', 2)
        END                                                       AS email,
        -- MASK phone: keep first 2 + last 2 digits -> 98******10
        CASE
            WHEN phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '\D', '', 'g')) < 4 THEN ''
            ELSE SUBSTR(REGEXP_REPLACE(phone, '\D', '', 'g'), 1, 2)
                 || REPEAT('*', LENGTH(REGEXP_REPLACE(phone, '\D', '', 'g')) - 4)
                 || RIGHT(REGEXP_REPLACE(phone, '\D', '', 'g'), 2)
        END                                                       AS phone,
        city,
        area,
        pincode,
        TRY_CAST(signup_date AS DATE)                             AS signup_date,
        UPPER(TRIM(loyalty_tier))                                 AS loyalty_tier,
        is_active,
        REGEXP_REPLACE(COALESCE(phone,''), '\D', '', 'g')         AS _phone_digits,
        _loaded_at, _source_file, _run_id
    FROM deduped
    WHERE rn = 1
),
checked AS (
    SELECT
        * EXCLUDE (_phone_digits),
        NULLIF(TRIM(CONCAT_WS(';',
            CASE WHEN LENGTH(_phone_digits) <> 10 THEN 'phone_not_10_digits' END,
            CASE WHEN loyalty_tier NOT IN ('BRONZE','SILVER','GOLD','PLATINUM') THEN 'loyalty_tier_invalid' END
        )), '')                                                   AS failure_reason
    FROM typed
)
SELECT *, 'PASS' AS dq_status   -- customer issues are Warning-level: all load
FROM checked;
