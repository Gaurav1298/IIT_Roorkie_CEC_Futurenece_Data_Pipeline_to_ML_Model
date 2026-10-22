"""
include/profiling/inspect_warehouse.py
---------------------------------------
Quick read-only inspection of the DuckDB warehouse. Run it inside the Airflow
container so it sees the same file the DAGs write:

    astro dev bash
    python /usr/local/airflow/include/profiling/inspect_warehouse.py

Or from the host:
    podman exec <scheduler-container> python /usr/local/airflow/include/profiling/inspect_warehouse.py
"""
import os
import duckdb

DB = "/usr/local/airflow/include/warehouse_gaurav.duckdb"
if not os.path.exists(DB):
    DB = os.path.join(os.path.dirname(__file__), "..", "warehouse_gaurav.duckdb")

con = duckdb.connect(DB)
tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
print("TABLES:", tables)
print()

for t in tables:
    n = con.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
    print(f"  {t:34} {n} rows")

def show(title, sql):
    print("\n--- " + title + " ---")
    print(con.execute(sql).fetchdf().to_string(index=False))

if "etl_dim_customer" in tables:
    show("dim_customer: PII masked?",
         "SELECT customer_id, email, phone, loyalty_tier FROM etl_dim_customer LIMIT 3")
if "etl_daily_zone_revenue" in tables:
    show("daily_zone_revenue (first 5)",
         "SELECT * FROM etl_daily_zone_revenue LIMIT 5")
if "etl_category_margin" in tables:
    show("category_margin", "SELECT * FROM etl_category_margin")
if "etl_payment_reconciliation" in tables:
    show("payment_reconciliation (first 5)",
         "SELECT * FROM etl_payment_reconciliation LIMIT 5")
if "etl_fact_orders" in tables:
    show("zone values in fact (NULL vs 'None' string check)",
         "SELECT zone, typeof(zone) AS t, COUNT(*) n FROM etl_fact_orders GROUP BY zone, typeof(zone) ORDER BY zone")
if "dq_results" in tables:
    show("dq_results summary by dimension",
         "SELECT dimension, severity, COUNT(*) checks, SUM(rows_failed) total_failed "
         "FROM dq_results GROUP BY dimension, severity ORDER BY dimension")
if "pipeline_runs" in tables:
    show("pipeline_runs (run audit)",
         "SELECT dag_id, rows_extracted, rows_loaded, rows_quarantined, status FROM pipeline_runs")
if "mart_category_margin" in tables:
    show("ELT mart_category_margin", "SELECT * FROM mart_category_margin")
if "mart_top_customers" in tables:
    show("ELT mart_top_customers (PII masked)",
         "SELECT customer_id, loyalty_tier, masked_email, masked_phone, total_spend FROM mart_top_customers LIMIT 5")

con.close()
