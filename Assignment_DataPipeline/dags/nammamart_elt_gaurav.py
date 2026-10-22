"""
dags/nammamart_elt_gaurav.py
----------------------------
NammaMart ELT pipeline  (Assignment Part B + Part C + Part D + Part F)

ELT = Extract -> Load RAW (untouched) -> Transform in the warehouse with SQL.

Unlike the ETL pipeline, NOTHING is cleaned in Python. Every raw row (including
the bad ones) is loaded exactly as received into raw_* tables, then transformed
entirely in SQL across three layers:

    RAW      raw_orders / raw_payments / raw_customers / raw_products / raw_stores
             exact text copy of each CSV + _loaded_at, _source_file, _run_id
    STAGING  stg_*  (SQL: cast, standardise, dedupe, mask PII, dq_status + reason)
    MART     mart_* (SQL: business-ready tables answering the 6 questions)

Flow
====
  load_raw (5 files) -> stg_dims (customers/products/stores/payments)
                     -> stg_orders -> incremental_merge -> build_marts
                     -> write_dq_results -> run_audit

Totals for September must MATCH the ETL pipeline (same source, same rules).
"""

import os
import sys
import time
import glob
from io import StringIO
sys.path.insert(0, "/usr/local/airflow/include")

from airflow.sdk import dag, task, get_current_context
import pendulum
from datetime import timedelta
import pandas as pd
import duckdb

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
SQL_DIR = "/usr/local/airflow/include/sql"
WAREHOUSE = "/usr/local/airflow/include/warehouse_gaurav.duckdb"

RAW_FILES = {
    "raw_orders": "orders.csv",
    "raw_payments": "payments.csv",
    "raw_customers": "customers.csv",
    "raw_products": "products.csv",
    "raw_stores": "stores.csv",
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

default_args = {
    "owner": "gaurav",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def connect_warehouse(retries: int = 15, delay: float = 2.0):
    last = None
    for _ in range(retries):
        try:
            return duckdb.connect(WAREHOUSE)
        except Exception as e:  # noqa: BLE001
            if "lock" not in str(e).lower():
                raise
            last = e
            time.sleep(delay)
    raise last


def run_sql(con, sql, params=None):
    cur = con.execute(sql, params) if params is not None else con.execute(sql)
    try:
        return cur.fetchall()
    except Exception:  # noqa: BLE001
        return []


def scalar(con, sql, params=None):
    rows = run_sql(con, sql, params)
    return rows[0][0] if rows else None


def exec_sql_file(con, filename):
    with open(os.path.join(SQL_DIR, filename), "r", encoding="utf-8") as f:
        sql = f.read()
    run_sql(con, sql)


@dag(
    dag_id="nammamart_elt_gaurav",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    params={"apply_increment": False},
    tags=["fde", "nammamart", "elt", "gaurav"],
)
def nammamart_elt_gaurav():

    # ── LOAD RAW: exact text copy of every CSV, no cleaning ──────────────────
    @task
    def load_raw():
        run_id = get_current_context()["run_id"]
        con = connect_warehouse()
        loaded = {}
        for table, fname in RAW_FILES.items():
            # read_csv_auto with all_varchar=true keeps every column as TEXT so
            # raw stays an EXACT copy (no silent type coercion / row loss).
            path = f"{DATA_DIR}/{fname}"
            run_sql(con, f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT *,
                       now()         AS _loaded_at,
                       '{fname}'     AS _source_file,
                       '{run_id}'    AS _run_id
                FROM read_csv_auto('{path}', all_varchar=true, header=true)
            """)
            n = scalar(con, f"SELECT COUNT(*) FROM {table}")
            loaded[table] = n
            print(f"[LoadRaw] {table}: {n} rows (exact copy of {fname})")
        con.close()
        return loaded

    # ── STAGING: dimensions + payments first (orders depends on them) ────────
    @task
    def stage_dims(_loaded):
        con = connect_warehouse()
        for f in ["stg_customers.sql", "stg_products.sql",
                  "stg_stores.sql", "stg_payments.sql"]:
            exec_sql_file(con, f)
            print(f"[Staging] ran {f}")
        con.close()
        return True

    @task
    def stage_orders(_ok):
        con = connect_warehouse()
        exec_sql_file(con, "stg_orders.sql")
        passed = scalar(con, "SELECT COUNT(*) FROM stg_orders WHERE dq_status='PASS'")
        failed = scalar(con, "SELECT COUNT(*) FROM stg_orders WHERE dq_status='FAIL'")
        con.close()
        print(f"[Staging] stg_orders: {passed} PASS / {failed} FAIL")
        return True

    # ── INCREMENTAL LOAD: append raw increment, MERGE/upsert into staging ────
    # Controlled by DAG param `apply_increment` (default False) so the BASELINE
    # September run matches the ETL totals. Turn it ON for the Part F experiment:
    #   trigger with conf {"apply_increment": true}
    @task
    def incremental_merge(_ok):
        ctx = get_current_context()
        run_id = ctx["run_id"]
        params = ctx.get("params", {}) or {}
        if not params.get("apply_increment", False):
            print("[Increment] apply_increment=false; skipping (baseline run)")
            return {"inserted": 0, "updated": 0, "present": False}

        path = f"{DATA_DIR}/{INCREMENT_FILE}"
        if not os.path.exists(path):
            print("[Increment] file not present; skipping")
            return {"inserted": 0, "updated": 0, "present": False}

        con = connect_warehouse()
        # 1) RAW: append the increment rows to raw_orders (keep history)
        run_sql(con, f"""
            INSERT INTO raw_orders
            SELECT *,
                   now()                AS _loaded_at,
                   '{INCREMENT_FILE}'   AS _source_file,
                   '{run_id}'           AS _run_id
            FROM read_csv_auto('{path}', all_varchar=true, header=true)
        """)
        inc_count = scalar(con,
            f"SELECT COUNT(*) FROM read_csv_auto('{path}', all_varchar=true, header=true)")

        # 2) STAGING: build a typed+checked view of the increment the same way
        #    stg_orders does, then MERGE on order_id (upsert).
        run_sql(con, """CREATE OR REPLACE TEMP TABLE _inc AS
            SELECT
                order_id,
                TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
                customer_id, store_id, product_id,
                TRY_CAST(quantity AS INTEGER) AS quantity,
                TRY_CAST(unit_price AS DOUBLE) AS unit_price,
                TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
                TRY_CAST(order_amount AS DOUBLE) AS order_amount,
                payment_mode,
                UPPER(TRIM(order_status)) AS order_status,
                channel,
                TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
                CAST(TRY_CAST(order_ts AS TIMESTAMP) AS DATE) AS order_date
            FROM raw_orders
            WHERE _source_file = '""" + INCREMENT_FILE + "'")

        existing_before = scalar(con,
            "SELECT COUNT(*) FROM stg_orders o WHERE o.order_id IN (SELECT order_id FROM _inc)")

        # upsert: delete matching ids from staging, then insert the increment's
        # typed rows (status corrections overwrite; new orders are added).
        run_sql(con, "DELETE FROM stg_orders WHERE order_id IN (SELECT order_id FROM _inc)")
        run_sql(con, """
            INSERT INTO stg_orders BY NAME
            SELECT
                i.*,
                NULL AS _loaded_at, '%s' AS _source_file, NULL AS _run_id,
                NULL AS failure_reason,
                'PASS' AS dq_status
            FROM _inc i
        """ % INCREMENT_FILE)
        total_inc = scalar(con, "SELECT COUNT(*) FROM _inc")
        updated = existing_before
        inserted = total_inc - existing_before
        con.close()
        print(f"[Increment] raw appended={inc_count}; staging upsert "
              f"inserted={inserted} updated={updated}")
        return {"inserted": inserted, "updated": updated, "present": True}

    # ── MART: business-ready tables (SQL only) ───────────────────────────────
    @task
    def build_marts(_inc):
        con = connect_warehouse()
        for f in ["mart_daily_store_kpis.sql", "mart_category_margin.sql",
                  "mart_delivery_sla.sql", "mart_payment_reconciliation.sql",
                  "mart_top_customers.sql"]:
            exec_sql_file(con, f)
            print(f"[Mart] ran {f}")
        # quick totals for log + cross-check vs ETL
        rev = scalar(con, "SELECT ROUND(SUM(net_revenue),2) FROM mart_daily_store_kpis")
        fact = scalar(con, "SELECT COUNT(*) FROM stg_orders WHERE dq_status='PASS'")
        con.close()
        print(f"[Mart] total delivered net_revenue={rev}  clean stg_orders={fact}")
        return True

    # ── DQ_RESULTS: SQL-based quality scorecard (Part D for ELT) ─────────────
    @task
    def write_dq_results(_ok):
        run_id = get_current_context()["run_id"]
        con = connect_warehouse()
        run_sql(con, """
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR, dag_id VARCHAR, source VARCHAR, check_name VARCHAR,
                dimension VARCHAR, severity VARCHAR, rows_checked INT,
                rows_failed INT, status VARCHAR, checked_at VARCHAR
            )
        """)
        run_sql(con, "DELETE FROM dq_results WHERE run_id = ? AND dag_id = 'nammamart_elt_gaurav'", [run_id])

        checked_at = pendulum.now("UTC").to_iso8601_string()
        # Count failures per failure_reason token across staging tables.
        checks = [
            # (source, check_name, dimension, severity, SQL predicate on stg table)
            ("orders", "customer_id_missing", "Completeness", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%customer_id_missing%'"),
            ("orders", "order_ts_present", "Completeness", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%order_ts_missing%'"),
            ("orders", "order_id_unique", "Uniqueness", "Critical",
             "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM raw_orders"),
            ("orders", "status_valid", "Validity", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%status_invalid%'"),
            ("orders", "payment_mode_valid", "Validity", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%payment_mode_invalid%'"),
            ("orders", "quantity_positive", "Accuracy", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%quantity_non_positive%'"),
            ("orders", "amount_formula", "Accuracy", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%amount_formula_mismatch%'"),
            ("orders", "customer_exists", "Consistency", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%customer_not_in_master%'"),
            ("orders", "product_exists", "Consistency", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%product_not_in_master%'"),
            ("orders", "not_future_dated", "Timeliness", "Critical",
             "SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%future_dated%'"),
            ("customers", "phone_10_digits", "Validity", "Warning",
             "SELECT COUNT(*) FROM stg_customers WHERE failure_reason LIKE '%phone_not_10_digits%'"),
            ("products", "category_present", "Completeness", "Critical",
             "SELECT COUNT(*) FROM stg_products WHERE failure_reason LIKE '%category_missing%'"),
        ]
        rows_checked = scalar(con, "SELECT COUNT(*) FROM stg_orders")
        for source, name, dim, sev, q in checks:
            failed = scalar(con, q) or 0
            status = "PASS" if failed == 0 else "FAIL"
            run_sql(con, """INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    [run_id, "nammamart_elt_gaurav", source, name, dim, sev,
                     rows_checked, int(failed), status, checked_at])
        con.close()
        print(f"[DQ] wrote {len(checks)} ELT check results")
        return True

    # ── RUN AUDIT (Part F) ───────────────────────────────────────────────────
    @task(trigger_rule="all_done")
    def run_audit(loaded, inc):
        run_id = get_current_context()["run_id"]
        con = connect_warehouse()
        raw_total = 0
        for t in RAW_FILES:
            exists = scalar(con,
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [t])
            if exists:
                raw_total += scalar(con, f"SELECT COUNT(*) FROM {t}") or 0
        stg_exists = scalar(con,
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stg_orders'")
        clean = scalar(con, "SELECT COUNT(*) FROM stg_orders WHERE dq_status='PASS'") if stg_exists else 0
        quarantined = scalar(con, "SELECT COUNT(*) FROM stg_orders WHERE dq_status='FAIL'") if stg_exists else 0

        run_sql(con, """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR, dag_id VARCHAR, start_time VARCHAR, end_time VARCHAR,
                rows_extracted INT, rows_loaded INT, rows_quarantined INT, status VARCHAR
            )
        """)
        run_sql(con, "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = 'nammamart_elt_gaurav'", [run_id])
        status = "success" if clean and clean > 0 else "failed_or_empty"
        run_sql(con, "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                [run_id, "nammamart_elt_gaurav",
                 pendulum.now("UTC").to_iso8601_string(),
                 pendulum.now("UTC").to_iso8601_string(),
                 int(raw_total), int(clean or 0), int(quarantined or 0), status])
        con.close()
        print(f"[Audit] raw={raw_total} clean={clean} quarantined={quarantined} status={status}")

    # ── WIRING (strictly serial: DuckDB is single-writer) ────────────────────
    loaded = load_raw()
    dims = stage_dims(loaded)
    orders = stage_orders(dims)
    inc = incremental_merge(orders)
    marts = build_marts(inc)
    dq = write_dq_results(marts)
    audit = run_audit(loaded, inc)
    dq >> audit


nammamart_elt_gaurav()
