"""
include/nammamart/masking.py
----------------------------
PII masking helpers for the ETL pipeline (Part A, step 4) and reused by the
governance answers (Part E).

Why mask instead of delete?
  The reporting tables still need to *group by* and *count* customers (e.g. "top
  10 customers by spend"), so we cannot drop the customer entirely. We keep a
  stable id (customer_id) but hide the human-identifying fields (email, phone).

The exact masking format is taken from the assignment examples:
  email  pr***@example.com   -> keep first 2 chars of the local part, then ***, keep domain
  phone  98******21          -> keep first 2 and last 2 digits, mask the middle
"""

import pandas as pd


def mask_email(email) -> str:
    """pri...@example.com -> 'pr***@example.com'. Safe on nulls/garbage."""
    if email is None or (isinstance(email, float) and pd.isna(email)):
        return ""
    email = str(email).strip()
    if email == "":
        return ""
    if "@" not in email:
        # not a valid email shape — mask the whole thing rather than leak it
        return "***"
    local, _, domain = email.partition("@")
    if len(local) <= 2:
        masked_local = (local[:1] or "*") + "***"
    else:
        masked_local = local[:2] + "***"
    return f"{masked_local}@{domain}"


def mask_phone(phone) -> str:
    """9876543210 -> '98******10'. Keeps first 2 + last 2 digits."""
    if phone is None or (isinstance(phone, float) and pd.isna(phone)):
        return ""
    # strip to digits only (handles '98765 43210', '+91...', floats like 9.8e9)
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if len(digits) < 4:
        return "*" * len(digits) if digits else ""
    return digits[:2] + "*" * (len(digits) - 4) + digits[-2:]


def mask_customer_frame(customers: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of the customers frame with email & phone masked in place."""
    out = customers.copy()
    if "email" in out.columns:
        out["email"] = out["email"].map(mask_email)
    if "phone" in out.columns:
        out["phone"] = out["phone"].map(mask_phone)
    return out
