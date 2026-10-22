"""
include/profiling/submission_queries.py
----------------------------------------
Runs the exact queries needed for the SUBMISSION screenshots:
  (4) dq_results        - the data-quality scorecard (Part D)
  (5) pipeline_runs     - the run-audit log (Part F)
  (6) payment reconciliation - mismatches with reasons (criterion #7 / Q4)
plus the six business-question results.

Run inside the Airflow container (so it sees the warehouse file):
    astro dev bash
    python /usr/local/airflow/include/profiling/submission_queries.py
"""
import duckdb

con = duckdb.connect("/usr/local/airflow/include/warehouse_gaurav.duckdb")


def section(title, sql, limit_note=""):
    print("\n" + "=" * 78)
    print(title + ("  " + limit_note if limit_note else ""))
    print("=" * 78)
    print(con.execute(sql).fetchdf().to_string(index=False))


# ── (4) dq_results — data-quality scorecard ──────────────────────────────────
section("(4) dq_results — per-dimension scorecard (ETL + ELT)", """
    SELECT dag_id, dimension, severity,
           COUNT(*)            AS checks,
           SUM(rows_failed)    AS total_rows_failed,
           SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS checks_failing
    FROM dq_results
    GROUP BY dag_id, dimension, severity
    ORDER BY dag_id, dimension, severity
""")

section("(4b) dq_results — raw rows (first 20)", """
    SELECT dag_id, source, check_name, dimension, severity, rows_failed, status
    FROM dq_results
    ORDER BY dag_id, rows_failed DESC
    LIMIT 20
""")

# ── (5) pipeline_runs — run audit ────────────────────────────────────────────
section("(5) pipeline_runs — run audit (one row per pipeline run)", """
    SELECT dag_id, rows_extracted, rows_loaded, rows_quarantined, status
    FROM pipeline_runs
    ORDER BY dag_id
""")

# ── (6) payment reconciliation — mismatches with reasons ─────────────────────
section("(6) payment reconciliation — ETL (summary by reason)", """
    SELECT reason, COUNT(*) AS mismatches,
           ROUND(SUM(amount_paid), 2) AS total_amount_paid
    FROM etl_payment_reconciliation
    GROUP BY reason
    ORDER BY mismatches DESC
""")

section("(6b) payment reconciliation — sample rows", """
    SELECT order_id, payment_id, amount_paid, order_amount, payment_status, reason
    FROM etl_payment_reconciliation
    LIMIT 10
""")

print("\n" + "#" * 78)
print("# SIX BUSINESS QUESTIONS (ELT mart tables)")
print("#" * 78)

section("Q1 — daily net revenue by zone & store (first 10)", """
    SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
    FROM mart_daily_store_kpis
    ORDER BY net_revenue DESC
    LIMIT 10
""")

section("Q2 — revenue & gross margin by category", """
    SELECT * FROM mart_category_margin
""")

section("Q3 — avg delivery time & % within 15 min, per store", """
    SELECT store_id, store_name, zone, delivered_orders,
           avg_delivery_minutes, pct_within_15_min
    FROM mart_delivery_sla
""")

section("Q4 — revenue by payment mode + non-reconciling total", """
    WITH by_mode AS (
        SELECT payment_mode, ROUND(SUM(order_amount),2) AS revenue
        FROM stg_orders
        WHERE dq_status='PASS' AND order_status='DELIVERED'
        GROUP BY payment_mode
    )
    SELECT payment_mode, revenue FROM by_mode ORDER BY revenue DESC
""")
section("Q4b — total amount that does NOT reconcile", """
    SELECT COUNT(*) AS mismatch_rows,
           ROUND(SUM(amount_paid),2) AS non_reconciling_amount
    FROM mart_payment_reconciliation
""")

section("Q5 — top 10 customers by spend (PII masked)", """
    SELECT customer_id, loyalty_tier, masked_email, masked_phone,
           delivered_orders, total_spend
    FROM mart_top_customers
""")

section("Q6 — rows rejected today by file and reason", """
    SELECT source AS file, check_name AS reason, rows_failed
    FROM dq_results
    WHERE dag_id='nammamart_elt_gaurav' AND status='FAIL'
    ORDER BY rows_failed DESC
""")

con.close()
print("\nDone.")
