"""
include/profiling/profile_data.py
----------------------------------
Part 0 — Discovery & Data Profiling (the FDE's first step).

This script does NOT clean anything and does NOT need Airflow. It just READS the
six NammaMart CSVs and prints a profile so you can SEE the data-quality problems
with your own eyes before writing any pipeline code.

What it reports, per the assignment (Section 5, Part 0):
  1. Row counts, columns, nulls per column, duplicate keys
  2. Min/max of every numeric and date column
  3. Distinct values of every category column
  4. Referential integrity across the 4 relationships
  5. Business-rule checks (e.g. recompute order_amount and compare)

Run it locally (from the project root):
    python include/profiling/profile_data.py

You do NOT need Docker/Airflow running for this. You only need pandas installed
locally, OR run it inside the Airflow container later. If pandas isn't installed
on your machine, see the note printed at the bottom.
"""

import os
import sys

try:
    import pandas as pd
except ImportError:
    print(
        "\n[!] pandas is not installed in the Python you're running.\n"
        "    Option A (quick): pip install pandas\n"
        "    Option B: run this inside the Airflow container once Airflow is up:\n"
        "        astro dev bash  ->  python /usr/local/airflow/include/profiling/profile_data.py\n"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Locate the data folder whether we run locally or inside the container.
# ---------------------------------------------------------------------------
LOCAL_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "nammamart")
CONTAINER_DIR = "/usr/local/airflow/include/data/nammamart"
DATA_DIR = CONTAINER_DIR if os.path.isdir(CONTAINER_DIR) else os.path.abspath(LOCAL_DIR)

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "increment": "orders_2026-10-01_increment.csv",
}

# Which column is the "key" for each file (used for duplicate-key checks).
KEY_COLUMN = {
    "orders": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
    "increment": "order_id",
}

# Columns we treat as categories (we'll print their distinct values).
CATEGORY_COLUMNS = {
    "orders": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_mode", "payment_status"],
    "customers": ["loyalty_tier", "is_active", "city"],
    "products": ["category", "sub_category", "unit", "is_perishable"],
    "stores": ["zone"],
    "increment": ["order_status", "payment_mode", "channel"],
}


def hr(char="-", n=78):
    print(char * n)


def load_all():
    """Read every CSV as raw strings first so profiling reflects the file as-is."""
    frames = {}
    for name, fname in FILES.items():
        path = os.path.join(DATA_DIR, fname)
        # dtype=str keeps the data EXACTLY as written (no silent type coercion),
        # which is what you want when profiling for problems.
        frames[name] = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    return frames


def profile_file(name, df):
    hr("=")
    print(f"FILE: {name}  ({FILES[name]})")
    hr("=")
    print(f"Rows: {len(df)}   Columns: {len(df.columns)}")
    print(f"Columns: {list(df.columns)}")

    # ---- Nulls per column ----
    print("\n[Nulls per column]")
    nulls = df.isna().sum()
    for col, n in nulls.items():
        if n > 0:
            print(f"  {col:<22} {n} null(s)")
    if nulls.sum() == 0:
        print("  (no nulls)")

    # ---- Duplicate keys ----
    key = KEY_COLUMN[name]
    if key in df.columns:
        dup_mask = df[key].duplicated(keep=False) & df[key].notna()
        dup_count = int(dup_mask.sum())
        print(f"\n[Duplicate keys on '{key}']: {dup_count} row(s)")
        if dup_count > 0:
            dup_vals = df.loc[dup_mask, key].value_counts().head(10)
            for val, cnt in dup_vals.items():
                print(f"  {val}  -> appears {cnt} times")

    # ---- Numeric min/max ----
    print("\n[Numeric columns — min / max]")
    numeric_found = False
    for col in df.columns:
        converted = pd.to_numeric(df[col], errors="coerce")
        # treat as numeric if most non-null values parse as numbers
        non_null = df[col].notna().sum()
        if non_null > 0 and converted.notna().sum() >= 0.8 * non_null:
            numeric_found = True
            bad = int(non_null - converted.notna().sum())
            flag = f"   <-- {bad} value(s) do NOT parse as numeric" if bad else ""
            print(f"  {col:<22} min={converted.min()}  max={converted.max()}{flag}")
    if not numeric_found:
        print("  (none detected)")

    # ---- Date min/max ----
    print("\n[Date/timestamp columns — min / max]")
    date_found = False
    for col in df.columns:
        if "ts" in col or "date" in col:
            parsed = pd.to_datetime(df[col], errors="coerce")
            non_null = df[col].notna().sum()
            if non_null > 0:
                date_found = True
                bad = int(non_null - parsed.notna().sum())
                flag = f"   <-- {bad} value(s) do NOT parse as a date" if bad else ""
                print(f"  {col:<22} min={parsed.min()}  max={parsed.max()}{flag}")
    if not date_found:
        print("  (none detected)")

    # ---- Distinct category values ----
    print("\n[Category columns — distinct values]")
    for col in CATEGORY_COLUMNS.get(name, []):
        if col in df.columns:
            vals = sorted(df[col].dropna().unique().tolist())
            print(f"  {col} ({len(vals)} distinct): {vals}")
    print()


def check_referential_integrity(frames):
    hr("=")
    print("REFERENTIAL INTEGRITY  (do foreign keys exist in the master files?)")
    hr("=")
    orders = frames["orders"]
    customers = set(frames["customers"]["customer_id"].dropna())
    products = set(frames["products"]["product_id"].dropna())
    stores = set(frames["stores"]["store_id"].dropna())
    order_ids = set(orders["order_id"].dropna())

    def report(label, bad_series):
        bad = bad_series.dropna().unique().tolist()
        print(f"  orders.{label}: {len(bad)} value(s) missing from master "
              f"-> {bad[:10]}{' ...' if len(bad) > 10 else ''}")

    report("customer_id", orders.loc[~orders["customer_id"].isin(customers), "customer_id"])
    report("product_id", orders.loc[~orders["product_id"].isin(products), "product_id"])
    report("store_id", orders.loc[~orders["store_id"].isin(stores), "store_id"])

    pay = frames["payments"]
    orphan_pay = pay.loc[~pay["order_id"].isin(order_ids), "order_id"]
    print(f"  payments.order_id: {orphan_pay.nunique()} payment(s) reference a "
          f"non-existent order -> {orphan_pay.dropna().unique().tolist()[:10]}")
    print()


def check_business_rules(frames):
    hr("=")
    print("BUSINESS RULES  (does the data obey the client's stated rules?)")
    hr("=")
    orders = frames["orders"].copy()

    qty = pd.to_numeric(orders["quantity"], errors="coerce")
    price = pd.to_numeric(orders["unit_price"], errors="coerce")
    disc = pd.to_numeric(orders["discount_pct"], errors="coerce")
    amount = pd.to_numeric(orders["order_amount"], errors="coerce")

    # Rule: order_amount = quantity * unit_price * (1 - discount/100), rounded 2dp
    expected = (qty * price * (1 - disc / 100)).round(2)
    mismatch = (expected - amount).abs() > 0.01
    mismatch = mismatch & expected.notna() & amount.notna()
    print(f"  order_amount formula mismatches: {int(mismatch.sum())} row(s)")
    if mismatch.any():
        sample = orders.loc[mismatch, ["order_id", "quantity", "unit_price",
                                       "discount_pct", "order_amount"]].head(5)
        print(sample.to_string(index=False))

    # Rule: quantity must be > 0
    bad_qty = (qty <= 0) | qty.isna()
    print(f"\n  quantity <= 0 or non-numeric: {int(bad_qty.sum())} row(s)")

    # Rule: valid order_status
    valid_status = {"DELIVERED", "CANCELLED", "RETURNED"}
    bad_status = ~orders["order_status"].isin(valid_status)
    print(f"  order_status not in {valid_status}: {int(bad_status.sum())} row(s)"
          f" -> {orders.loc[bad_status, 'order_status'].dropna().unique().tolist()}")

    # Rule: valid payment_mode
    valid_mode = {"UPI", "Card", "Cash on Delivery", "Wallet"}
    bad_mode = ~orders["payment_mode"].isin(valid_mode)
    print(f"  payment_mode not in allowed list: {int(bad_mode.sum())} row(s)"
          f" -> {orders.loc[bad_mode, 'payment_mode'].dropna().unique().tolist()}")

    # Rule: DELIVERED must have delivery_minutes; normal range 8-35, cap 120
    dmin = pd.to_numeric(orders["delivery_minutes"], errors="coerce")
    delivered = orders["order_status"] == "DELIVERED"
    missing_dm = delivered & dmin.isna()
    extreme_dm = dmin > 120
    print(f"  DELIVERED orders missing delivery_minutes: {int(missing_dm.sum())} row(s)")
    print(f"  delivery_minutes > 120 (impossible): {int(extreme_dm.sum())} row(s)")

    # Rule: no future-dated order_ts (today is the run date)
    ts = pd.to_datetime(orders["order_ts"], errors="coerce")
    future = ts > pd.Timestamp.now()
    print(f"  future-dated order_ts: {int(future.sum())} row(s)"
          f" -> max order_ts = {ts.max()}")

    # Payments: SUCCESS amount should equal the order's amount
    pay = frames["payments"].copy()
    amt_paid = pd.to_numeric(pay["amount_paid"], errors="coerce")
    order_amt_map = dict(zip(orders["order_id"], amount))
    pay["expected_amt"] = pay["order_id"].map(order_amt_map)
    success = pay["payment_status"] == "SUCCESS"
    amt_mismatch = success & (amt_paid - pay["expected_amt"]).abs().gt(0.01) & pay["expected_amt"].notna()
    print(f"\n  SUCCESS payments whose amount != order_amount: {int(amt_mismatch.sum())} row(s)")
    print()


def main():
    print(f"\nReading data from: {DATA_DIR}\n")
    frames = load_all()
    for name in ["orders", "payments", "customers", "products", "stores", "increment"]:
        profile_file(name, frames[name])
    check_referential_integrity(frames)
    check_business_rules(frames)
    hr("=")
    print("DONE. Use the findings above to fill in your Data Issues Log (Part 0).")
    hr("=")


if __name__ == "__main__":
    main()
