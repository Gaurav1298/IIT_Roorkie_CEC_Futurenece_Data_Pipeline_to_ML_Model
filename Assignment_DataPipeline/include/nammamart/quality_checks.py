"""
include/nammamart/quality_checks.py
------------------------------------
Data-quality rules for the ETL pipeline, derived directly from the Part 0
Data Issues Log. This single module powers BOTH:

  * Part A  — splitting each file into passed / quarantined rows
  * Part D  — the six quality dimensions, each check tagged with a dimension
              and a severity, producing rows for the `dq_results` scorecard.

Design
======
Every check is a small function that takes a DataFrame and returns a boolean
Series: True = this row FAILS the check. We tag each check with:
  - name       a short identifier
  - dimension  one of the 6: Completeness, Uniqueness, Validity, Accuracy,
               Consistency, Timeliness
  - severity   "Critical" (row is quarantined) or "Warning" (row loads, logged)

`validate_orders(...)` runs all order checks, records per-check results for
`dq_results`, and returns (passed_df, failed_df_with_reason, dq_rows).

We standardise FIRST (fix casing / trim), because a value like `upi ` is not a
real error — it just needs cleaning. Only genuinely bad rows get quarantined.
"""

from __future__ import annotations
import pandas as pd


VALID_ORDER_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}

# Map the dirty payment_mode variants we found in Part 0 to canonical values.
PAYMENT_MODE_MAP = {
    "upi": "UPI", "UPI": "UPI", "UPI ": "UPI",
    "card": "Card", "Card": "Card", "CARD": "Card",
    "cod": "Cash on Delivery", "cash on delivery": "Cash on Delivery",
    "Cash on Delivery": "Cash on Delivery",
    "wallet": "Wallet", "Wallet": "Wallet",
    # 'Cashh' is NOT mapped on purpose -> it stays invalid and gets quarantined.
}


# ---------------------------------------------------------------------------
# Standardisation (runs before validation; fixes Warning-level mess in place)
# ---------------------------------------------------------------------------
def standardise_orders(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # trim whitespace on all string columns
    for col in out.select_dtypes(include="object").columns:
        out[col] = out[col].str.strip()
    # order_status: fix casing (delivered -> DELIVERED) but keep bad ones as-is
    out["order_status"] = out["order_status"].str.upper().where(
        out["order_status"].str.upper().isin(VALID_ORDER_STATUS),
        out["order_status"],
    )
    # payment_mode: apply the mapping; unmapped values stay as-is (to be caught)
    out["payment_mode"] = out["payment_mode"].map(
        lambda v: PAYMENT_MODE_MAP.get(v, v)
    )
    return out


def standardise_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.select_dtypes(include="object").columns:
        out[col] = out[col].str.strip()
    # loyalty_tier casing: BRONZE/Bronze/bronze -> BRONZE
    out["loyalty_tier"] = out["loyalty_tier"].str.upper()
    return out


# ---------------------------------------------------------------------------
# A tiny helper to describe each check
# ---------------------------------------------------------------------------
def _check(name, dimension, severity, fail_mask):
    return {
        "name": name,
        "dimension": dimension,
        "severity": severity,
        "fail_mask": fail_mask.fillna(True),  # treat NaN comparisons as failing
    }


# ---------------------------------------------------------------------------
# ORDERS validation
# ---------------------------------------------------------------------------
def validate_orders(df: pd.DataFrame, customers, products, stores) -> dict:
    df = standardise_orders(df)

    qty = pd.to_numeric(df["quantity"], errors="coerce")
    price = pd.to_numeric(df["unit_price"], errors="coerce")
    disc = pd.to_numeric(df["discount_pct"], errors="coerce")
    amount = pd.to_numeric(df["order_amount"], errors="coerce")
    dmin = pd.to_numeric(df["delivery_minutes"], errors="coerce")
    ts = pd.to_datetime(df["order_ts"], errors="coerce")
    expected_amount = (qty * price * (1 - disc / 100)).round(2)

    cust_ids = set(customers["customer_id"].dropna())
    prod_ids = set(products["product_id"].dropna())
    store_ids = set(stores["store_id"].dropna())

    checks = [
        # Completeness
        _check("orders_customer_id_present", "Completeness", "Critical",
               df["customer_id"].isna() | (df["customer_id"].astype(str).str.strip() == "")),
        _check("orders_order_ts_present", "Completeness", "Critical",
               df["order_ts"].isna()),
        _check("delivered_has_delivery_minutes", "Completeness", "Critical",
               (df["order_status"] == "DELIVERED") & dmin.isna()),
        # Uniqueness
        _check("orders_order_id_unique", "Uniqueness", "Critical",
               df["order_id"].duplicated(keep="first")),
        # Validity
        _check("orders_status_valid", "Validity", "Critical",
               ~df["order_status"].isin(VALID_ORDER_STATUS)),
        _check("orders_payment_mode_valid", "Validity", "Critical",
               ~df["payment_mode"].isin(VALID_PAYMENT_MODE)),
        _check("orders_discount_in_range", "Validity", "Critical",
               (disc < 0) | (disc > 100)),
        _check("orders_ts_parses", "Validity", "Critical", ts.isna()),
        # Accuracy
        _check("orders_quantity_positive", "Accuracy", "Critical", qty <= 0),
        _check("orders_amount_matches_formula", "Accuracy", "Critical",
               (expected_amount - amount).abs() > 0.01),
        _check("orders_amount_non_negative", "Accuracy", "Critical", amount < 0),
        _check("orders_delivery_minutes_sane", "Accuracy", "Critical", dmin > 120),
        # Consistency (foreign keys exist in masters)
        _check("orders_customer_exists", "Consistency", "Critical",
               ~df["customer_id"].isin(cust_ids)),
        _check("orders_product_exists", "Consistency", "Critical",
               ~df["product_id"].isin(prod_ids)),
        _check("orders_store_exists", "Consistency", "Critical",
               ~df["store_id"].isin(store_ids)),
        # Timeliness
        _check("orders_not_future_dated", "Timeliness", "Critical",
               ts > pd.Timestamp.now()),
    ]
    return _assemble(df, checks)


def validate_customers(df: pd.DataFrame) -> dict:
    df = standardise_customers(df)
    phone_digits = df["phone"].astype(str).str.replace(r"\D", "", regex=True)
    checks = [
        _check("customers_id_unique", "Uniqueness", "Critical",
               df["customer_id"].duplicated(keep="first")),
        _check("customers_city_present", "Completeness", "Warning",
               df["city"].isna()),
        _check("customers_phone_10_digits", "Validity", "Warning",
               phone_digits.str.len() != 10),
        _check("customers_loyalty_tier_valid", "Consistency", "Warning",
               ~df["loyalty_tier"].isin({"BRONZE", "SILVER", "GOLD", "PLATINUM"})),
    ]
    return _assemble(df, checks)


def validate_products(df: pd.DataFrame) -> dict:
    mrp = pd.to_numeric(df["mrp"], errors="coerce")
    cost = pd.to_numeric(df["cost_price"], errors="coerce")
    checks = [
        _check("products_id_unique", "Uniqueness", "Critical",
               df["product_id"].duplicated(keep="first")),
        _check("products_category_present", "Completeness", "Warning",
               df["category"].isna()),
        _check("products_mrp_positive", "Accuracy", "Critical", mrp <= 0),
        _check("products_cost_le_mrp", "Accuracy", "Warning", cost > mrp),
    ]
    return _assemble(df, checks)


def validate_stores(df: pd.DataFrame) -> dict:
    cap = pd.to_numeric(df["capacity_orders_per_day"], errors="coerce")
    checks = [
        _check("stores_id_unique", "Uniqueness", "Critical",
               df["store_id"].duplicated(keep="first")),
        _check("stores_zone_present", "Completeness", "Warning",
               df["zone"].isna()),
        _check("stores_capacity_positive", "Accuracy", "Warning", cap <= 0),
    ]
    return _assemble(df, checks)


# ---------------------------------------------------------------------------
# Assemble: split rows, build failure_reason, and emit dq_results rows
# ---------------------------------------------------------------------------
def _assemble(df: pd.DataFrame, checks: list) -> dict:
    n = len(df)
    # A row is quarantined only if it fails at least one CRITICAL check.
    critical_fail = pd.Series(False, index=df.index)
    reason = pd.Series("", index=df.index)
    dq_rows = []

    for c in checks:
        mask = c["fail_mask"].reindex(df.index, fill_value=False)
        rows_failed = int(mask.sum())
        dq_rows.append({
            "check_name": c["name"],
            "dimension": c["dimension"],
            "severity": c["severity"],
            "rows_checked": n,
            "rows_failed": rows_failed,
            "status": "PASS" if rows_failed == 0 else "FAIL",
        })
        # accumulate human-readable reasons on failing rows
        if rows_failed:
            reason = reason.where(~mask, reason + f"{c['name']};")
        if c["severity"] == "Critical":
            critical_fail = critical_fail | mask

    passed = df[~critical_fail].copy()
    failed = df[critical_fail].copy()
    failed["failure_reason"] = reason[critical_fail]

    return {
        "passed": passed,
        "failed": failed,
        "dq_rows": dq_rows,
        "passed_count": len(passed),
        "failed_count": len(failed),
    }
