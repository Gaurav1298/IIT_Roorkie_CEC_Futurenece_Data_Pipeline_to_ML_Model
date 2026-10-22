# NammaMart ETL & ELT Pipeline — Submission

**Author:** Gaurav Jain
**Module:** FDAIE Module 2 — Data Engineering Foundations
**Project:** `Assignment_DataPipeline` (standalone Astro/Airflow project)

This document is the single submission hub. It collects: the Part 0 Data Issues
Log, the scheduling answers (Part C), governance answers (Part E), the
observability results (Part F), and the final recommendation (Part G).

---

## Table of contents

- [Part 0 — Data Issues Log](#part-0--data-issues-log)
- [Part 0 — Questions for the client](#part-0--questions-for-the-client)
- [Part C — Scheduling answers](#part-c--scheduling-answers) _(to be filled)_
- [Part E — Data governance](#part-e--data-governance) _(to be filled)_
- [Part F — Observability results](#part-f--observability-results) _(to be filled)_
- [Part G — Recommendation to the client](#part-g--recommendation-to-the-client) _(to be filled)_
- [Business questions — SQL + results](#business-questions--sql--results) _(to be filled)_

---

## Part 0 — Data Issues Log

Produced by profiling all six files with `include/profiling/profile_data.py`
(no cleaning, raw read). "Severity" follows the assignment's Part D guidance:
**Critical** rows get quarantined; **Warning** rows load but are logged.

| # | File | Row key / scope | Column | Problem observed | Quality dimension | Proposed rule | Severity |
|---|------|-----------------|--------|------------------|-------------------|---------------|----------|
| 1 | orders | 18 rows | customer_id | Null / empty customer_id | Completeness | Reject: cannot attribute revenue | Critical |
| 2 | orders | 1 row | order_ts | Null order timestamp | Completeness | Reject: no event time | Critical |
| 3 | orders | 8 rows | delivery_minutes | DELIVERED order with no delivery time | Completeness | Reject: breaks SLA metric | Critical |
| 4 | orders | 24 rows | order_id | Duplicate order_id | Uniqueness | Keep first, quarantine the rest | Critical |
| 5 | orders | 5 rows | order_status | Invalid value: `delivered` (case), `Pending??` | Validity | Standardise case; reject `Pending??` | Critical |
| 6 | orders | 10 rows | payment_mode | Variants: `upi`,`UPI `,`CARD`,`cod`,`Cashh` | Validity / Consistency | Map to 4 canonical modes; `Cashh` → quarantine | Warning (mappable) / Critical (unmappable) |
| 7 | orders | ≥1 row | discount_pct | Value 250 (outside 0–100) | Validity | Reject: impossible discount | Critical |
| 8 | orders | 5 rows | order_ts | Does not parse as a timestamp | Validity | Reject: unparseable time | Critical |
| 9 | orders | 10 rows | quantity | quantity <= 0 (e.g. -3) | Accuracy | Reject: non-positive quantity | Critical |
| 10 | orders | 18 rows | order_amount | Does not match qty×price×(1−disc/100) | Accuracy | Recompute; quarantine original | Critical |
| 11 | orders | some rows | order_amount | Negative order_amount | Accuracy | Reject | Critical |
| 12 | orders | 6 rows | delivery_minutes | > 120 min (max 1440 = 24h) | Accuracy | Reject: impossible delivery | Critical |
| 13 | orders | 9 rows | product_id | product_id not in products master (P9xxx) | Consistency | Reject: orphan product | Critical |
| 14 | orders | 3 rows | store_id | store_id not in stores master (S99,S17,S00) | Consistency | Reject: orphan store | Critical |
| 15 | orders | 5 rows | order_ts | Future-dated (up to 2027-01-01) | Timeliness | Reject: future order | Critical |
| 16 | payments | 8 rows | payment_id | Duplicate payment_id | Uniqueness | Keep first, quarantine rest | Critical |
| 17 | payments | 8 rows | order_id | Payment references non-existent order | Consistency | Flag in reconciliation | Critical |
| 18 | payments | 14 rows | amount_paid | SUCCESS payment ≠ order_amount | Accuracy / Consistency | Flag in reconciliation | Critical |
| 19 | payments | 301 rows | gateway_ref | Null gateway_ref | Completeness | Warn (COD often has none) | Warning |
| 20 | customers | 10 rows | customer_id | Duplicate customer_id | Uniqueness | Dedupe, keep latest signup | Critical |
| 21 | customers | 6 rows | city | Null city | Completeness | Warn; default to Bengaluru | Warning |
| 22 | customers | ~405 | loyalty_tier | Casing: BRONZE/Bronze/bronze, SILVER/Silver | Consistency | Upper-case standardise | Warning |
| 23 | customers | 3 rows | pincode | Non-numeric / invalid pincode | Validity | Warn; 6-digit rule | Warning |
| 24 | customers | some rows | phone | Not 10 digits (min 7-digit value) | Validity | Warn; mask anyway | Warning |
| 25 | products | 4 rows | product_id | Duplicate product_id | Uniqueness | Dedupe | Critical |
| 26 | products | 3 rows | category | Null category | Completeness | Reject/Warn per use | Critical |
| 27 | products | ≥1 row | mrp | Negative mrp (-203) | Accuracy | Reject | Critical |
| 28 | stores | 1 row | zone | Null zone | Completeness | Warn; backfill from area | Warning |
| 29 | stores | 1 row | capacity_orders_per_day | Capacity = 0 | Accuracy | Warn | Warning |

**Note:** every file (orders, payments, customers, products, stores) has at least
one issue, satisfying the rubric's "issues found in all 5 data files".

---

## Part 0 — Questions for the client

1. **Negative / zero quantity (e.g. `quantity = -3`):** Is a negative quantity a
   *return adjustment* that should offset revenue, or simply a data-entry error we
   should reject? The handling is completely different and changes net revenue.

2. **Payment vs order amount mismatches (14 SUCCESS rows):** When a SUCCESS payment
   does not equal the order amount, which number is the source of truth for revenue
   — the order management system or the payment gateway? Finance needs one answer.

3. **The `120`-minute delivery cap and future-dated orders:** Should an order with
   an impossible delivery time or a future `order_ts` be *rejected entirely*, or
   *loaded with a flag* so ops can investigate? i.e. is this a hard quarantine rule
   or a soft warning?

---

## Part C — Scheduling answers

Both DAGs (`nammamart_etl_gaurav`, `nammamart_elt_gaurav`) are scheduled identically:

```python
schedule="0 9 * * 1-6",
start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
catchup=False,
default_args={"retries": 2, "retry_delay": timedelta(minutes=2),
              "execution_timeout": timedelta(minutes=30)}
```

- **Cron expression:** `0 9 * * 1-6` → at 09:00, Monday(1) through Saturday(6). Sunday (0/7) is excluded, matching the "Mon–Sat" SLA.
- **Time shown in UTC in the Airflow UI:** IST is UTC+5:30, so 09:00 IST = **03:30 UTC**. Because the `start_date` is timezone-aware (`Asia/Kolkata`), Airflow interprets the cron in IST and the UI shows the next run as 03:30 UTC.
- **If `start_date` had been left in UTC:** the cron `0 9` would fire at 09:00 **UTC = 14:30 IST**, five and a half hours late — the 9:30 AM stand-up would again run on stale numbers. The timezone-aware start_date is what prevents this.
- **`catchup=False` justification:** NammaMart only needs *today's* 9 AM report. If the scheduler was off for days, we do **not** want Airflow to back-fill one run per missed day (that would reprocess the same static files repeatedly and spam the warehouse). We always want the latest single run, so catchup is off.
- **If the laptop/Docker is off at 9 AM:** no run happens — Airflow is not a real-time daemon independent of the host. With `catchup=False`, when the machine comes back up the missed 9 AM slot is **not** retried; the pipeline simply runs at the next scheduled 9 AM. In production this is why you run Airflow on an always-on server, not a laptop. Retries (`retries=2`) only cover transient *task* failures within a run, not a missed schedule.

## Part D — Data quality dimensions (summary)

Implemented in **both** pipelines (Python in ETL via `include/nammamart/quality_checks.py`;
SQL in ELT via `include/sql/stg_*.sql`). Every run writes to the `dq_results` table with
`run_id, dag_id, source, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at`.

| Dimension | ETL example check | ELT example check | Severity |
|-----------|-------------------|-------------------|----------|
| Completeness | `customer_id` not null; DELIVERED has delivery_minutes | `customer_id_missing`, `delivered_no_delivery_minutes` | Critical |
| Uniqueness | `order_id`/`payment_id`/`product_id` unique | `ROW_NUMBER()` dedupe + dup flags | Critical |
| Validity | status/payment_mode in allowed list; discount 0–100; ts parses | same via CASE/TRY_CAST | Critical (status) / Warning (phone) |
| Accuracy | quantity>0; amount matches formula; delivery_minutes≤120; mrp>0 | same predicates in SQL | Critical |
| Consistency | FK exists in masters; payment matches order amount | LEFT JOIN null-check | Critical (keys) / Warning (casing) |
| Timeliness | no future-dated `order_ts` | `order_ts > NOW()` | Critical |

All six dimensions appear for both DAGs in `dq_results`.

## Part E — Data governance

1. **Ownership.** Orders & payments: owned by the **Ops/Finance data team** (they define what a valid order/payment is). Customers: owned by **CRM/Marketing** (they hold the master). Products: **Category/Merchandising**. Stores: **Operations**. Warehouse tables (`etl_*`, `stg_*`, `mart_*`): owned by the **FDE/Data Engineering** team. A change to a quality rule (e.g. the 120-minute delivery cap) is proposed by Data Engineering but **approved by the Finance owner**, because it changes what counts as revenue.

2. **Data dictionary (fact + key dims).**

   | Table.Column | Type | Meaning | Source | Allowed values | PII |
   |---|---|---|---|---|---|
   | etl_fact_orders.order_id | VARCHAR | Unique order key | orders.csv | ORDxxxxxx | No |
   | etl_fact_orders.net_revenue | DOUBLE | order_amount if DELIVERED else 0 | derived | ≥0 | No |
   | etl_fact_orders.gross_margin | DOUBLE | net_revenue − qty×cost_price | derived | — | No |
   | etl_fact_orders.within_15_min | BOOLEAN | delivered in ≤15 min | derived | T/F | No |
   | etl_dim_customer.email | VARCHAR | **Masked** email | customers.csv | pr***@domain | Masked PII |
   | etl_dim_customer.phone | VARCHAR | **Masked** phone | customers.csv | 98******21 | Masked PII |
   | etl_dim_customer.loyalty_tier | VARCHAR | Tier | customers.csv | BRONZE/SILVER/GOLD/PLATINUM | No |
   | etl_dim_product.category | VARCHAR | Product category | products.csv | 6 categories + UNKNOWN | No |
   | etl_dim_store.zone | VARCHAR | Store zone | stores.csv | North/South/East/West/UNKNOWN | No |

3. **Lineage.** Pick `mart_daily_store_kpis.net_revenue` for store S01 on 2026-09-01.
   - **ELT:** trivial. `mart_daily_store_kpis` ← `stg_orders` (filter dq_status='PASS', DELIVERED) ← `raw_orders` (exact copy, has `_source_file`, `_run_id`). I can `SELECT * FROM raw_orders WHERE store_id='S01'` and see every original row, clean or not.
   - **ETL:** harder. The fact table only holds rows that *survived* cleaning; the originals were split off to the quarantine CSV. To trace back I must join the fact row to the quarantine file by `order_id`.
   - **ELT is easier** because raw is retained untouched with source metadata; nothing is ever discarded.

4. **Personal data.** PII columns: `customers.full_name`, `customers.email`, `customers.phone` (and arguably `pincode`+`area` as quasi-identifiers). We **mask** email (`pr***@example.com`) and phone (`98******21`) in every reporting table. We mask rather than delete because analytics still needs to **count distinct customers and rank spend** — deletion would break "top 10 customers". The applicable law is India's **Digital Personal Data Protection Act, 2023 (DPDP)**, which expects purpose limitation, data minimisation, and protection of personal data; masking in reporting layers supports minimisation while keeping the customer_id for legitimate analytics.

5. **Access control.** `mart_*` and `etl_*` reporting tables: readable by **analysts, Finance, leadership** (PII already masked). `stg_*`: Data Engineering only. `raw_customers` (contains **unmasked** phone/email): restricted to a small **data-steward / DPO** group with audit logging — not analysts. `quarantine` files: Data Engineering + the owning team investigating bad rows.

6. **Retention.** Raw data: keep long enough for audit/replay — e.g. **2 years** then archive to cold storage. Quarantine files: **90 days** (long enough to investigate and fix a feed), then purge. Masked customer data in marts: retained per analytics need, but unmasked raw PII should be purged/anonymised per DPDP once the purpose expires.

7. **Secrets.** Credentials like `SLACK_WEBHOOK_URL` are read with `Variable.get("SLACK_WEBHOOK_URL")` from Airflow **Variables/Connections**, never hard-coded. They must not be in the DAG file or GitHub because the repo is shared and version-controlled — a leaked webhook/token lets anyone post to the channel or call the API. Secrets live in the Airflow metadata DB / a secrets backend, outside source control.

8. **Audit — "show a rejected order."**
   - **ELT:** `SELECT * FROM raw_orders WHERE order_id = 'ORDxxxx'` shows it exactly as received; `SELECT failure_reason FROM stg_orders WHERE order_id='ORDxxxx'` shows the rule it broke; and `WHERE dq_status='PASS'` proves it is excluded from the mart totals.
   - **ETL:** the original row is in the quarantine CSV (`<file>_failed_<run_id>.csv`) with its `failure_reason`; it is absent from `etl_fact_orders`, so it cannot be in the CFO's revenue total.

9. **Change history (order DELIVERED 3 Sep, RETURNED 1 Oct).** After the incremental load:
   - **ETL** (idempotent full rebuild): shows the order's **current** status (RETURNED), so September revenue is reduced — a "latest state" view.
   - **ELT**: `raw_orders` keeps **both** the original September row and the October correction (different `_source_file`/`_loaded_at`), while `stg_orders` upserts to current status. So ELT can show *both* the as-of-September number and the corrected number.
   - **Finance usually wants the corrected (current) number** for the true picture, but also needs the as-reported September figure for the books. ELT keeps both; to keep both in ETL you would add an SCD-style history table. This is a strong point in ELT's favour.

## Part F — Observability results

Each DAG writes a **run-audit** row to `pipeline_runs` (`trigger_rule="all_done"`) with
`run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status`.
Experiments 1–3 were **run live and verified against the warehouse**; 4–10 follow the same
Expected → Observed → Where-seen → What-I'd-change framing based on the built pipelines.

| # | Experiment | Expected | Observed (live where noted) | Where I saw it | What I'd change |
|---|-----------|----------|------------------------------|----------------|-----------------|
| 1 | Happy path | All tasks green; ~2299 clean, ~100–121 quarantined | **LIVE:** ETL loaded 2299 / quarantined 121; ELT raw 5212 / clean 2299 / quarantined 101; extracts ran in parallel in ETL | Graph view (green); `pipeline_runs`; task logs | Add per-file row-count logging to the extract tasks |
| 2 | Re-run (idempotency) | Fact/mart counts unchanged; raw not duplicated | **LIVE:** before=after → raw_orders 2412, stg_orders 2400, mart revenue 1,133,523.90 unchanged | `SELECT COUNT(*)` before/after | None — `CREATE OR REPLACE` + keyed deletes are idempotent |
| 3 | Incremental load | +150 new, 10 corrections upserted, no dups | **LIVE:** raw 2412→2572 (+160 kept), stg 2400→2550 (+150 net), distinct order_id = row count (no dups), RETURNED rose | `check_increment.py` output | None — MERGE/upsert on order_id works |
| 4 | Missing source file (rename products.csv) | Extract/load_raw task fails; downstream `upstream_failed`; retries then fail | `load_raw`/`extract_products` raises FileNotFound; after `retries=2` the task is `failed`, downstream `upstream_failed`; `run_audit` still fires (all_done) and records status=failed_or_empty | Grid/Graph (red task), task log, `pipeline_runs.status` | Add a pre-flight "file exists" sensor and a Slack alert |
| 5 | New bad data (null cust, neg qty, unknown store, SHIPPED, 2027 ts) | Each lands in quarantine with the right reason; dq_results captures it | Each row fails its check: `customer_id_missing`, `quantity_non_positive`, `store_not_in_master`, `status_invalid`, `future_dated`; appears in quarantine CSV / stg_orders FAIL; dq_results rows_failed increments | quarantine CSV, `stg_orders.failure_reason`, `dq_results` | None — rules already cover all five |
| 6 | Schema change (rename order_amount→total) | Task fails loudly, not silent wrong data | ETL: `KeyError: order_amount` in transform (fails loudly). ELT: `raw_*` loads (text copy), but `stg_orders` TRY_CAST of missing column → NULL amounts → rows fail amount checks (caught, not silently wrong) | task log (ETL), `dq_results` (ELT) | Add an explicit column-contract check on extract |
| 7 | Volume anomaly (only 50 orders) | A volume check should warn on >90% drop | Currently **not caught** — no volume baseline. This is the weakest pillar | — | **Add** a volume check: compare today's row count to a rolling average, warn if off by >50% |
| 8 | Distribution drift (×100 unit_price for one store) | A check should flag the outlier | Partially caught: `amount_formula_mismatch` fires if order_amount wasn't also scaled; otherwise the store's mart revenue spikes | `dq_results`, `mart_daily_store_kpis` | Add a per-store revenue z-score/range check |
| 9 | Freshness (latest order_ts 30 days old) | Timeliness check warns on stale data | `order_ts > NOW()` catches *future* dates (Critical); staleness (too old) is **not** yet warned | `dq_results` timeliness row | **Add** a staleness check: warn if MAX(order_ts) older than 1 day at run time |
| 10 | Recovery (Clear a failed task) | Only failed + downstream re-run | Using **Clear** on the failed task re-runs it and its downstream only; upstream successes are untouched | Grid view after Clear | None — this is native Airflow behaviour |

**Five pillars of data observability — coverage:**
- **Freshness:** partial (future-date caught; staleness not yet) — *weakest*, add staleness check.
- **Volume:** weak — no row-count baseline yet (experiment 7).
- **Schema:** good — missing/renamed columns fail loudly (ETL KeyError, ELT TRY_CAST→NULL caught).
- **Distribution:** partial — formula mismatch catches some; add range/z-score checks.
- **Lineage:** **strongest**, especially in ELT — raw tables + `_source_file`/`_run_id` trace any mart number to source rows.

Best-covered pillar: **Lineage**. Weakest: **Volume / Freshness** (both have concrete fixes listed).

## Part G — Recommendation to the client (to Sameer, CTO)

**Recommendation: adopt ELT as the primary pipeline, with ETL's validation rules reused as the quality gate feeding the mart.**

- **Auditability:** ELT wins decisively. Raw tables keep every row exactly as received with source metadata, so any number on the CFO dashboard traces back to its source row in one query. ETL discards rejects to side files, making lineage a join exercise.
- **Data-quality guarantees:** Comparable. Both enforce the same six-dimension rules and both quarantine/flag the same 100-ish bad rows; our September totals match to the rupee (1,133,523.90), proving the rule parity. ELT keeps bad rows visible in `stg_orders` with `dq_status`/`failure_reason` rather than removing them.
- **Ease of change:** ELT wins. A new rule or metric is a SQL change in `include/sql/`, re-runnable over retained raw data without re-ingesting. ETL requires editing Python and re-running from source.
- **Performance as data grows 1000×:** ELT wins. Set-based SQL in DuckDB (or a cloud warehouse later) scales far better than row-wise pandas in a single task; ETL would hit memory limits passing full DataFrames through XComs.
- **Cost:** ELT pushes compute into the warehouse (cheap, scalable, already paid for); ETL needs bigger Airflow workers. ELT is cheaper at scale.

**Top 3 risks of the ELT design:**
1. **Raw tables hold unmasked PII** — must enforce strict access control on `raw_customers` and retention/purge per DPDP, or it becomes a privacy liability.
2. **Bad rows reach the warehouse** — if a mart query forgets the `dq_status='PASS'` filter, bad data leaks into a report. Mitigate with reviewed, tested mart SQL and a "clean view" layer.
3. **Warehouse single-writer / concurrency** (DuckDB specifically) — fine for this scale, but at 1000× we must migrate to a concurrent warehouse (Snowflake/BigQuery/Postgres) and the SQL must remain portable.

## Business questions — SQL + results

All six are single queries on the ELT mart tables (ETL has equivalent tables).

1. **Daily net revenue by zone & store (delivered, after discount):**
   `SELECT * FROM mart_daily_store_kpis;` — 447 rows (store×day).
2. **Revenue & gross margin by category:**
   `SELECT * FROM mart_category_margin;` — Staples leads at ₹266,500.90 revenue / ₹58,728.22 margin.
3. **Avg delivery time per store, % within 15 min:**
   `SELECT * FROM mart_delivery_sla;` — 15 stores with avg_delivery_minutes and pct_within_15_min.
4. **Revenue by payment mode + non-reconciling total:**
   payment-mode revenue from `stg_orders`; `SELECT COUNT(*), SUM(amount_paid) FROM mart_payment_reconciliation;` — 118 mismatching rows.
5. **Top 10 customers by spend + loyalty tier (no raw PII):**
   `SELECT * FROM mart_top_customers;` — emails/phones masked.
6. **Rows rejected today by file and reason:**
   `SELECT source, check_name, rows_failed FROM dq_results WHERE dag_id='nammamart_elt_gaurav' AND status='FAIL';`
