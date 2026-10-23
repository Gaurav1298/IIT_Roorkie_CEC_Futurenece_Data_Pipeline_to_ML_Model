# NammaMart Data Pipelines — Learning Guide

An interactive, **offline HTML guide** that explains the NammaMart data pipeline
project from zero — the business problem, the core concepts, the technologies and
why they were chosen, how everything connects, a per-file code walkthrough, and a
glossary.

It is documentation *about* the pipelines (for learning, revision, and interview
prep). It is **not** part of the pipeline runtime — nothing here is imported or run
by Airflow.

## Who it's for

Anyone who wants to understand the project without a data-engineering background —
including the author revising for interviews. Plain-English explanations sit next to
the precise technical detail.

## How to open it

Just open `index.html` in any web browser (double-click it, or drag it into a
browser tab). No server, build step, or internet connection is required — all
styles and scripts are bundled locally.

## What's inside

| File | Purpose |
|---|---|
| `index.html` | All the content, organised into 7 tabs |
| `assets/style.css` | Self-contained styles (no external fonts or CDNs) |
| `assets/app.js` | Tabs, toggles, theme/style switchers, and the code explainer |

### The 7 tabs

1. **Start Here** — overview and how to read the guide.
2. **The Business Problem** — who NammaMart is, what was broken, the FDE role, and the client's acceptance criteria.
3. **Core Concepts** — pipelines, ETL vs ELT, the warehouse, star schema, the six data-quality dimensions, quarantine, idempotency, incremental/upsert, PII masking, orchestration, and observability.
4. **Technologies & Why** — Airflow, DuckDB, pandas, Astro, Docker/Podman, cron/pendulum, with the rationale for each.
5. **How It Connects** — the end-to-end flow, the ETL path, the ELT path, and the three ELT layers.
6. **Code Walkthrough** — every important file explained, plus interactive "walk me through" explainers for the ETL DAG and `stg_orders.sql`.
7. **Glossary** — every term in plain English.

## Interactive features

- **Story mode** — narrative and analogies on (for learning) or off (for quick revision).
- **Deep dive** — extra technical depth under each concept.
- **Theme** — dark, light, sepia, ocean, grape, forest, or rosé.
- **Style** — soft, sharp, or playful.
- **Code explainer** — click any code block, or "Walk me through", to step through explanations.

Your toggle, theme, and style choices are remembered between visits (via the
browser's local storage).

## Note on syncing

This folder is mirrored one-way to a fork copy whenever it changes (via
`sync_learning_guide.ps1` and a Kiro save hook). Treat this copy as the source of
truth and avoid editing the mirror directly.
