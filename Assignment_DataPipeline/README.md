# NammaMart ETL & ELT Pipelines — FDAIE Module 2

Author: **Gaurav Jain**

Two Apache Airflow pipelines (ETL and ELT) that turn NammaMart's six messy CSV
exports into reconciled, business-ready tables in a DuckDB warehouse, scheduled
for 9:00 AM IST, Monday–Saturday. Built as a standalone Astro project.

## What's here

```
dags/
  nammamart_etl_gaurav.py     ETL: extract -> validate -> quarantine -> transform -> load star -> reconcile -> report
  nammamart_elt_gaurav.py     ELT: load raw -> SQL staging -> SQL marts (+ param-gated MERGE incremental)
include/
  data/nammamart/             the six source CSVs (never edited by hand)
  nammamart/
    quality_checks.py         ETL quality rules (6 dimensions, severities)
    masking.py                PII masking helpers (email, phone)
  sql/
    stg_*.sql                 ELT staging layer (cast, standardise, dedupe, mask, dq_status)
    mart_*.sql                ELT mart layer (answers the 6 business questions)
  profiling/
    profile_data.py           Part 0 data profiling (run first, no Airflow needed)
    inspect_warehouse.py      read-only warehouse inspection
    test_logic_local.py       local unit check of the ETL helpers
    test_elt_sql.py           in-memory dry-run of the ELT SQL layers
  quarantine/gaurav/          rejected rows with failure_reason (evidence)
  warehouse_gaurav.duckdb      the DuckDB warehouse (created by the DAGs)
SUBMISSION.md                 Parts 0, C, D, E, F, G answers + business questions
Business_Questions_Result.docx Contains the result of SQL ran for business questions
```

## Run it

Prerequisites: Docker/Podman + Astro CLI.

```bash
astro dev start                      # starts Airflow (UI on the mapped port, admin/admin)
# Part 0 profiling (optional, local):
python include/profiling/profile_data.py
```

In the Airflow UI, unpause and trigger `nammamart_etl_gaurav` and
`nammamart_elt_gaurav`. To run the ELT incremental load experiment:

```bash
astro dev run dags trigger nammamart_elt_gaurav --conf '{"apply_increment": true}'
```

Inspect results:

```bash
python include/profiling/inspect_warehouse.py      # (inside the container)
```

## Key results (verified)

- Both pipelines quarantine the same bad rows and produce **matching September
  totals**: total delivered revenue **1,133,523.90** in both ETL and ELT.
- ETL: 2,299 clean fact rows, 121 quarantined. ELT: raw kept exactly (5,212
  rows across files), 2,299 clean staging orders, 101 quarantined.
- PII (email/phone) masked in every reporting table.
- Idempotent re-runs (counts don't change) and a working MERGE incremental load
  (+150 new, 10 corrections upserted, no duplicates).

See `SUBMISSION.md` for the full write-up.
