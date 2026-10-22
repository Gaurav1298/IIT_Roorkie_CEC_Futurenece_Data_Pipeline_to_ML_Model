"""
include/profiling/test_elt_sql.py
----------------------------------
Dry-run the ELT SQL layers against a TEMP DuckDB (not the real warehouse) so we
can validate the raw->staging->mart SQL in isolation and compare September
totals to the ETL numbers, without touching Airflow.

Run inside the container:
    python /usr/local/airflow/include/profiling/test_elt_sql.py
"""
import duckdb

DATA = "/usr/local/airflow/include/data/nammamart"
SQL = "/usr/local/airflow/include/sql"
RAW = {
    "raw_orders": "orders.csv", "raw_payments": "payments.csv",
    "raw_customers": "customers.csv", "raw_products": "products.csv",
    "raw_stores": "stores.csv",
}


def run_file(con, name):
    with open(f"{SQL}/{name}") as f:
        con.execute(f.read())


con = duckdb.connect(":memory:")

# raw
for t, f in RAW.items():
    con.execute(f"""CREATE OR REPLACE TABLE {t} AS
        SELECT *, now() AS _loaded_at, '{f}' AS _source_file, 'test' AS _run_id
        FROM read_csv_auto('{DATA}/{f}', all_varchar=true, header=true)""")
    print(f"{t:16} {con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]} raw rows")

# staging
for f in ["stg_customers.sql", "stg_products.sql", "stg_stores.sql",
          "stg_payments.sql", "stg_orders.sql"]:
    run_file(con, f)

p = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status='PASS'").fetchone()[0]
fl = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status='FAIL'").fetchone()[0]
print(f"\nstg_orders: PASS={p}  FAIL={fl}")

deliv = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status='PASS' AND order_status='DELIVERED'").fetchone()[0]
print(f"clean DELIVERED orders: {deliv}")

# marts
for f in ["mart_daily_store_kpis.sql", "mart_category_margin.sql",
          "mart_delivery_sla.sql", "mart_payment_reconciliation.sql",
          "mart_top_customers.sql"]:
    run_file(con, f)

print("\n--- mart_category_margin (compare to ETL) ---")
print(con.execute("SELECT * FROM mart_category_margin").fetchdf().to_string(index=False))
print("\n--- mart_payment_reconciliation count ---")
print(con.execute("SELECT COUNT(*) FROM mart_payment_reconciliation").fetchone()[0], "mismatches")
print("\n--- mart_top_customers (PII masked?) ---")
print(con.execute("SELECT customer_id, loyalty_tier, masked_email, masked_phone, total_spend FROM mart_top_customers LIMIT 5").fetchdf().to_string(index=False))

total_rev = con.execute("SELECT ROUND(SUM(net_revenue),2) FROM mart_daily_store_kpis").fetchone()[0]
print(f"\nTOTAL delivered net_revenue (ELT): {total_rev}")
