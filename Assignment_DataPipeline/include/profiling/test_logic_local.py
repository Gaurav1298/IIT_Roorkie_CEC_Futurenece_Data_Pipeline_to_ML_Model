"""
include/profiling/test_logic_local.py
--------------------------------------
Quick LOCAL sanity check (no Airflow needed) for the ETL helper logic:
  - masking.py          (PII masking)
  - quality_checks.py   (validation + dq rows + passed/failed split)

Run:
    python include/profiling/test_logic_local.py

This mirrors what the `validate` and `transform` tasks do inside the DAG, so if
this passes locally, the DAG logic is sound and we only have to worry about the
Airflow wiring.
"""
import os
import sys

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..")))  # include/ on path

import pandas as pd
from nammamart.masking import mask_email, mask_phone
from nammamart.quality_checks import (
    validate_orders, validate_customers, validate_products, validate_stores,
)

DATA = os.path.abspath(os.path.join(HERE, "..", "data", "nammamart"))


def test_masking():
    print("== Masking ==")
    assert mask_email("priya.hegde@example.com") == "pr***@example.com", mask_email("priya.hegde@example.com")
    assert mask_phone("9876543210") == "98******10", mask_phone("9876543210")
    assert mask_email("") == ""
    assert mask_phone(None) == ""
    print("  email sample:", mask_email("priya.hegde199@example.com"))
    print("  phone sample:", mask_phone("9774349021"))
    print("  OK")


def test_quality():
    orders = pd.read_csv(f"{DATA}/orders.csv", dtype=str)
    customers = pd.read_csv(f"{DATA}/customers.csv", dtype=str)
    products = pd.read_csv(f"{DATA}/products.csv", dtype=str)
    stores = pd.read_csv(f"{DATA}/stores.csv", dtype=str)

    r_cust = validate_customers(customers)
    r_prod = validate_products(products)
    r_store = validate_stores(stores)
    r_ord = validate_orders(orders, r_cust["passed"], r_prod["passed"], r_store["passed"])

    print("\n== Validation results ==")
    for name, r in [("orders", r_ord), ("customers", r_cust),
                    ("products", r_prod), ("stores", r_store)]:
        print(f"  {name:<10} passed={r['passed_count']:<6} failed={r['failed_count']}")

    print("\n== orders dq_results (dimension / check / rows_failed) ==")
    for row in r_ord["dq_rows"]:
        print(f"  [{row['dimension']:<12} {row['severity']:<8}] "
              f"{row['check_name']:<34} failed={row['rows_failed']}")

    # sanity: every dimension appears at least once for orders
    dims = {row["dimension"] for row in r_ord["dq_rows"]}
    need = {"Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"}
    missing = need - dims
    assert not missing, f"orders missing dimensions: {missing}"
    print("\n  All 6 quality dimensions present for orders:", sorted(dims))

    # show a couple of quarantined rows with reasons
    print("\n== sample quarantined orders (failure_reason) ==")
    failed = r_ord["failed"]
    if len(failed):
        print(failed[["order_id", "failure_reason"]].head(8).to_string(index=False))


if __name__ == "__main__":
    test_masking()
    test_quality()
    print("\nAll local checks passed.")
