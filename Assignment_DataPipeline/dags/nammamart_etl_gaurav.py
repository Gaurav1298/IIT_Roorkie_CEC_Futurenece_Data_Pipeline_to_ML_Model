"""
dags/nammamart_etl_gaurav.py
----------------------------
NammaMart ETL pipeline  (Assignment Part A + Part C + Part D + Part F)

ETL = Extract -> Validate -> Transform (CLEAN FIRST) -> Load.
Bad rows are quarantined BEFORE the warehouse, so the finance tables only ever
contain validated data.

Flow
====
  extract_orders   ─┐
  extract_payments ─┤
  extract_customers├─► validate ─┬─► quarantine ─────────────┐
  extract_products ─┤             │                           │
  extract_stores   ─┘             └─► transform ─► load_star ─┤
                                                               ├─► reconcile ─► report
                                                   (write dq_results & pipeline_runs)
Outputs in DuckDB (include/warehouse_gaurav.duckdb):
  etl_fact_orders, etl_dim_customer, etl_dim_product, etl_dim_store,
  etl_payment_reconciliation, etl_daily_zone_revenue, etl_category_margin,
  dq_results, pipeline_runs
"""

import os
import sys
import time
from io import StringIO
sys.path.insert(0, "/usr/local/airflow/include")

from airflow.sdk import dag, task, get_current_context
import pendulum
from datetime import timedelta
import pandas as pd
import duckdb

from nammamart.quality_checks import (
    validate_orders, validate_customers, validate_products, validate_stores,
)
from nammamart.masking import mask_customer_frame


def read_json(s):
    """Wrapper so we read JSON from a StringIO (silences the pandas warning)."""
    return pd.read_json(StringIO(s), dtype=str) if isinstance(s, str) else pd.read_json(s)


def connect_warehouse(retries: int = 10, delay: float = 1.5):
    """Open DuckDB, retrying briefly if another task still holds the write lock.

    DuckDB allows only ONE writer at a time. Our write tasks are chained so they
    don't overlap, but this retry is a safety net against lock races.
    """
    last = None
    for _ in range(retries):
        try:
            return duckdb.connect(WAREHOUSE)
        except (duckdb.IOException, Exception) as e:  # noqa: BLE001
            if "lock" not in str(e).lower():
                raise
            last = e
            time.sleep(delay)
    raise last


def run_sql(con, sql, params=None):
    """Execute a statement and FULLY DRAIN its result.

    DuckDB's Python API reuses one cursor per connection. If you run a statement
    and don't consume its result, the next execute() raises
    "connection has an unfinished result from execute()". Calling fetchall()
    here drains every statement so the connection is always clean for the next
    one. Returns the fetched rows (empty list for DDL/DML).
    """
    cur = con.execute(sql, params) if params is not None else con.execute(sql)
    try:
        return cur.fetchall()
    except Exception:  # noqa: BLE001  (DDL statements have no result to fetch)
        return []


def scalar(con, sql, params=None):
    """Run a query expected to return a single value; drains the result."""
    rows = run_sql(con, sql, params)
    return rows[0][0] if rows else None

# ── Paths (container paths; Astro mounts include/ into the container) ─────────
DATA_DIR = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE = "/usr/local/airflow/include/warehouse_gaurav.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/gaurav"

# ── Scheduling (Part C): 9:00 AM IST, Monday–Saturday ────────────────────────
# Cron is interpreted in the DAG's timezone, which we pin to Asia/Kolkata via
# the pendulum start_date. "0 9 * * 1-6" = at 09:00, Mon(1)..Sat(6).
default_args = {
    "owner": "gaurav",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="nammamart_etl_gaurav",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    tags=["fde", "nammamart", "etl", "gaurav"],
)
def nammamart_etl_gaurav():

    # ── EXTRACT: five parallel tasks, one per file ───────────────────────────
    @task
    def extract_orders():
        df = pd.read_csv(f"{DATA_DIR}/orders.csv", dtype=str)
        print(f"[Extract] orders: {len(df)} rows")
        return df.to_json()

    @task
    def extract_payments():
        df = pd.read_csv(f"{DATA_DIR}/payments.csv", dtype=str)
        print(f"[Extract] payments: {len(df)} rows")
        return df.to_json()

    @task
    def extract_customers():
        df = pd.read_csv(f"{DATA_DIR}/customers.csv", dtype=str)
        print(f"[Extract] customers: {len(df)} rows")
        return df.to_json()

    @task
    def extract_products():
        df = pd.read_csv(f"{DATA_DIR}/products.csv", dtype=str)
        print(f"[Extract] products: {len(df)} rows")
        return df.to_json()

    @task
    def extract_stores():
        df = pd.read_csv(f"{DATA_DIR}/stores.csv", dtype=str)
        print(f"[Extract] stores: {len(df)} rows")
        return df.to_json()

    # ── VALIDATE: run quality checks, split passed/failed, collect dq rows ────
    @task
    def validate(orders_j, payments_j, customers_j, products_j, stores_j):
        orders = read_json(orders_j)
        payments = read_json(payments_j)
        customers = read_json(customers_j)
        products = read_json(products_j)
        stores = read_json(stores_j)

        r_cust = validate_customers(customers)
        r_prod = validate_products(products)
        r_store = validate_stores(stores)
        # orders validated against the CLEAN masters (passed dims)
        r_ord = validate_orders(orders, r_cust["passed"], r_prod["passed"], r_store["passed"])

        dq_rows = []
        for dag_part, res in [("orders", r_ord), ("customers", r_cust),
                              ("products", r_prod), ("stores", r_store)]:
            for row in res["dq_rows"]:
                row = {**row, "source": dag_part}
                dq_rows.append(row)

        print(f"[Validate] orders passed={r_ord['passed_count']} "
              f"failed={r_ord['failed_count']}")
        return {
            "orders_passed": r_ord["passed"].to_json(),
            "orders_failed": r_ord["failed"].to_json(),
            "customers_passed": r_cust["passed"].to_json(),
            "customers_failed": r_cust["failed"].to_json(),
            "products_passed": r_prod["passed"].to_json(),
            "products_failed": r_prod["failed"].to_json(),
            "stores_passed": r_store["passed"].to_json(),
            "stores_failed": r_store["failed"].to_json(),
            "payments_raw": payments.to_json(),
            "dq_rows": dq_rows,
            "counts": {
                "orders_passed": r_ord["passed_count"],
                "orders_failed": r_ord["failed_count"],
                "customers_failed": r_cust["failed_count"],
                "products_failed": r_prod["failed_count"],
                "stores_failed": r_store["failed_count"],
            },
        }

    # ── QUARANTINE: write every file's failed rows with a failure_reason ─────
    @task
    def quarantine(v: dict):
        run_id = get_current_context()["run_id"]
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        safe_run = run_id.replace(":", "_").replace("+", "_")
        total = 0
        for name in ["orders", "customers", "products", "stores"]:
            failed = read_json(v[f"{name}_failed"])
            if len(failed):
                path = f"{QUARANTINE_DIR}/{name}_failed_{safe_run}.csv"
                failed.to_csv(path, index=False)
                print(f"[Quarantine] {name}: {len(failed)} rows -> {path}")
                total += len(failed)
        print(f"[Quarantine] total quarantined: {total}")
        return total

    # ── TRANSFORM: dedupe, mask PII, join, derive business columns ───────────
    @task
    def transform(v: dict):
        orders = read_json(v["orders_passed"])
        customers = read_json(v["customers_passed"])
        products = read_json(v["products_passed"])
        stores = read_json(v["stores_passed"])

        # numeric casts for the fact table
        for col in ["quantity", "unit_price", "discount_pct", "order_amount",
                    "delivery_minutes"]:
            orders[col] = pd.to_numeric(orders[col], errors="coerce")
        products["cost_price"] = pd.to_numeric(products["cost_price"], errors="coerce")
        products["mrp"] = pd.to_numeric(products["mrp"], errors="coerce")

        # dedupe dimensions (keep first)
        customers = customers.drop_duplicates(subset="customer_id", keep="first")
        products = products.drop_duplicates(subset="product_id", keep="first")

        # mask PII on the customer dimension
        customers = mask_customer_frame(customers)

        # join orders -> product (for cost_price), store (for zone)
        merged = orders.merge(
            products[["product_id", "category", "cost_price"]],
            on="product_id", how="left",
        ).merge(
            stores[["store_id", "zone", "store_name"]],
            on="store_id", how="left",
        )

        # Normalise join-miss / null labels BEFORE they become the string "None"
        # when serialised through JSON. These come from the planted null zone /
        # null category rows (Warning-level, so they load but get a clear label).
        merged["zone"] = merged["zone"].fillna("UNKNOWN").replace(
            {"None": "UNKNOWN", "nan": "UNKNOWN", "": "UNKNOWN"})
        merged["category"] = merged["category"].fillna("UNKNOWN").replace(
            {"None": "UNKNOWN", "nan": "UNKNOWN", "": "UNKNOWN"})

        # derived columns
        merged["order_date"] = pd.to_datetime(merged["order_ts"]).dt.date.astype(str)
        is_delivered = merged["order_status"] == "DELIVERED"
        merged["net_revenue"] = merged["order_amount"].where(is_delivered, 0.0)
        merged["gross_margin"] = (
            merged["net_revenue"] - (merged["quantity"] * merged["cost_price"])
        ).where(is_delivered, 0.0)
        merged["within_15_min"] = (merged["delivery_minutes"] <= 15) & is_delivered

        print(f"[Transform] fact rows: {len(merged)}")
        return {
            "fact": merged.to_json(),
            "dim_customer": customers.to_json(),
            "dim_product": products.to_json(),
            "dim_store": stores.to_json(),
            "payments_raw": v["payments_raw"],
        }

    # ── LOAD: write the star schema idempotently (CREATE OR REPLACE) ─────────
    @task
    def load_star(t: dict):
        fact = pd.read_json(StringIO(t["fact"]))
        dim_customer = read_json(t["dim_customer"])
        dim_product = read_json(t["dim_product"])
        dim_store = read_json(t["dim_store"])

        con = connect_warehouse()
        # CREATE OR REPLACE makes the load idempotent: re-running rebuilds the
        # tables from the current passed rows, so counts never double.
        con.register("fact_df", fact)
        con.register("cust_df", dim_customer)
        con.register("prod_df", dim_product)
        con.register("store_df", dim_store)
        con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM fact_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM cust_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM prod_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM store_df")
        count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()
        print(f"[Load] etl_fact_orders now has {count} rows (idempotent rebuild)")
        return t["payments_raw"]

    # ── RECONCILE: clean orders vs payments, one row per mismatch ────────────
    @task
    def reconcile(payments_json: str):
        payments = read_json(payments_json)
        payments["amount_paid"] = pd.to_numeric(payments["amount_paid"], errors="coerce")

        con = connect_warehouse()
        orders = con.execute(
            "SELECT order_id, order_amount, order_status FROM etl_fact_orders"
        ).fetchdf()
        con.register("pay_df", payments)

        merged = payments.merge(orders, on="order_id", how="left")
        rows = []
        for _, r in merged.iterrows():
            reason = None
            if pd.isna(r["order_amount"]):
                reason = "payment references unknown/quarantined order"
            elif r["payment_status"] == "SUCCESS" and abs(
                (r["amount_paid"] or 0) - (r["order_amount"] or 0)) > 0.01:
                reason = "SUCCESS amount != order_amount"
            if reason:
                rows.append({
                    "order_id": r["order_id"],
                    "payment_id": r["payment_id"],
                    "amount_paid": r["amount_paid"],
                    "order_amount": r["order_amount"],
                    "payment_status": r["payment_status"],
                    "reason": reason,
                })
        recon = pd.DataFrame(rows)
        con.register("recon_df", recon)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon_df")
        con.close()
        print(f"[Reconcile] {len(recon)} mismatch row(s)")
        return len(recon)

    # ── REPORT: build the summary tables and print them ──────────────────────
    @task
    def report():
        con = connect_warehouse()
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date,
                   COALESCE(zone, 'UNKNOWN') AS zone,
                   ROUND(SUM(net_revenue), 2) AS net_revenue,
                   COUNT(*) AS orders
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY order_date, COALESCE(zone, 'UNKNOWN')
            ORDER BY order_date, zone
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT COALESCE(category, 'UNKNOWN') AS category,
                   ROUND(SUM(net_revenue), 2) AS revenue,
                   ROUND(SUM(gross_margin), 2) AS gross_margin
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY COALESCE(category, 'UNKNOWN')
            ORDER BY revenue DESC
        """)
        zone = con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 10").fetchdf()
        cat = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        con.close()
        print("[Report] daily_zone_revenue (first 10):\n" + zone.to_string(index=False))
        print("[Report] category_margin:\n" + cat.to_string(index=False))

    # ── DQ_RESULTS + PIPELINE_RUNS: scorecard (Part D) & run audit (Part F) ──
    @task
    def write_dq_results(v: dict):
        run_id = get_current_context()["run_id"]
        con = connect_warehouse()
        rows = pd.DataFrame(v["dq_rows"])
        rows["run_id"] = run_id
        rows["dag_id"] = "nammamart_etl_gaurav"
        rows["checked_at"] = pendulum.now("UTC").to_iso8601_string()
        con.register("dq_df", rows)
        run_sql(con, """
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR, dag_id VARCHAR, source VARCHAR, check_name VARCHAR,
                dimension VARCHAR, severity VARCHAR, rows_checked INT,
                rows_failed INT, status VARCHAR, checked_at VARCHAR
            )
        """)
        # idempotent: clear this run's rows before inserting
        run_sql(con, "DELETE FROM dq_results WHERE run_id = ? AND dag_id = 'nammamart_etl_gaurav'", [run_id])
        run_sql(con, """INSERT INTO dq_results
            (run_id, dag_id, source, check_name, dimension, severity,
             rows_checked, rows_failed, status, checked_at)
            SELECT run_id, dag_id, source, check_name, dimension, severity,
                   rows_checked, rows_failed, status, checked_at FROM dq_df""")
        con.close()
        print(f"[DQ] wrote {len(rows)} check results")

    @task(trigger_rule="all_done")
    def run_audit(v: dict, quarantined: int):
        run_id = get_current_context()["run_id"]
        con = connect_warehouse()
        # Check the table exists via the catalog FIRST, so we never run a SELECT
        # on a missing table. Every statement is drained via run_sql/scalar so
        # the connection never carries an "unfinished result" into the next one.
        table_exists = scalar(con,
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'etl_fact_orders'") > 0
        loaded = scalar(con, "SELECT COUNT(*) FROM etl_fact_orders") if table_exists else 0
        counts = v.get("counts", {}) if isinstance(v, dict) else {}
        extracted = sum([
            counts.get("orders_passed", 0), counts.get("orders_failed", 0),
        ])
        quarantined = quarantined if isinstance(quarantined, int) else 0
        # Report an honest status: if the fact table loaded rows, the core
        # pipeline succeeded; otherwise flag it. (run_audit fires on all_done,
        # so it must not blindly claim success.)
        status = "success" if loaded > 0 else "failed_or_empty"
        audit = pd.DataFrame([{
            "run_id": run_id,
            "dag_id": "nammamart_etl_gaurav",
            "start_time": pendulum.now("UTC").to_iso8601_string(),
            "end_time": pendulum.now("UTC").to_iso8601_string(),
            "rows_extracted": extracted,
            "rows_loaded": loaded,
            "rows_quarantined": quarantined,
            "status": status,
        }])
        con.register("audit_df", audit)
        run_sql(con, """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR, dag_id VARCHAR, start_time VARCHAR, end_time VARCHAR,
                rows_extracted INT, rows_loaded INT, rows_quarantined INT, status VARCHAR
            )
        """)
        run_sql(con, "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = 'nammamart_etl_gaurav'", [run_id])
        run_sql(con, "INSERT INTO pipeline_runs SELECT * FROM audit_df")
        con.close()
        print(f"[Audit] run recorded: loaded={loaded} quarantined={quarantined} status={status}")

    # ── WIRING ───────────────────────────────────────────────────────────────
    o = extract_orders()
    p = extract_payments()
    c = extract_customers()
    pr = extract_products()
    s = extract_stores()

    v = validate(o, p, c, pr, s)

    # Quarantine writes files (no DB lock) — safe to run off to the side.
    q = quarantine(v)

    # All DuckDB WRITE tasks are chained into ONE serial path so they never hold
    # the warehouse lock at the same time (DuckDB = single writer):
    #   transform -> load_star -> reconcile -> report -> write_dq_results -> run_audit
    t = transform(v)
    payments_after_load = load_star(t)
    recon = reconcile(payments_after_load)
    rep = report()
    dq = write_dq_results(v)
    audit = run_audit(v, q)

    recon >> rep >> dq >> audit


nammamart_etl_gaurav()
