# FHIR API Data Ingestion & Analytics Pipeline

## Overview

A complete end-to-end data pipeline that ingests healthcare data from a FHIR R4 API into Azure Databricks using the **Medallion Architecture** (Raw → Bronze → Silver → Gold) with **SCD Type 2** versioning for historical tracking.

## Architecture

```text
FHIR API (hapi.fhir.org/baseR4)
    |
    v
[Raw Layer] -- JSON files saved to Unity Catalog Volumes
    |           /Volumes/vamsi/default/fhir_raw/{resource}/{date}/{run_id}/
    v
[Bronze Layer] -- Delta tables with MERGE for idempotency
    |              vamsi.bronze.fhir_{resource}_bronze
    v
[Silver Layer] -- Transformed columns + SCD Type 2 history
    |              vamsi.silver.fhir_{resource}_silver
    v
[Gold Layer] -- Analytical views (is_current = true only)
                   vamsi.gold.fhir_{resource}_summary
```

## FHIR Resources Ingested

| Resource | Description | Key Fields |
|---|---|---|
| Patient | Person receiving care | name, gender, birthDate, address, phone |
| Encounter | Hospital visit | status, class, type, period, reason |
| Observation | Test result / measurement | code, value, unit, effectiveDate |
| Condition | Diagnosis / health issue | clinicalStatus, code, onset, category |

## Key Features

- **Incremental Ingestion** — `_lastUpdated` fetches recent changes using a configurable lookback period.
- **Pagination** — Follows FHIR Bundle `next` links, with a configurable maximum of 50 pages per resource.
- **Idempotency** — MERGE-based Bronze loading helps prevent duplicates on re-runs.
- **SCD Type 2** — `content_hash` comparison detects changes, expires old rows, and inserts new versions.
- **Full Metadata** — Records include metadata such as timestamps, `run_id`, `raw_file_path`, and `content_hash`.
- **Data Quality** — Null checks, duplicate detection, and row-count validation.
- **Serverless Compatible** — Avoids `.cache()`, `.persist()`, and `.unpersist()` and includes explicit Window imports for Spark 4.2.
- **Modular Code** — Centralized configuration, reusable functions, and a `TRANSFORM_MAP` for adding resources.

## Folder Structure

```text
FHIR_Project/
├── README.md
├── notebooks/
│   └── FHIR_Pipeline.py
├── config/
│   └── workflow.json
├── docs/
│   └── FHIR_Documentation.html
└── reference/
    └── FHIR_Clean_Notebook.md
```

## How to Run

### Prerequisites

- Azure Databricks workspace with Unity Catalog enabled
- Catalog and Bronze, Silver, and Gold schemas created
- Volume created at `/Volumes/{catalog}/default/fhir_raw`
- Serverless compute enabled

### Manual Run

1. Import `notebooks/FHIR_Pipeline.py` into your Databricks workspace.
2. In Cell 1, change `CATALOG = "vamsi"` to your catalog name if required.
3. Run all cells sequentially.

### Scheduled Run (Databricks Workflow)

1. Open **Workflows** in the Databricks sidebar.
2. Create a job using `config/workflow.json`.
3. Update `notebook_path` and `run_as` to match your environment.
4. Review and enable the schedule (configured for daily at 6 AM IST, if retained in the workflow configuration).

## Configuration (Cell 1)

| Parameter | Default | Description |
|---|---|---|
| `CATALOG` | `vamsi` | Unity Catalog name |
| `LOOKBACK_DAYS` | `3` | Days of data to fetch |
| `FHIR_BASE_URL` | `https://hapi.fhir.org/baseR4` | FHIR server endpoint |
| `MAX_PAGES_PER_RESOURCE` | `50` | Pagination safety limit |
| `HTTP_RETRIES` | `3` | API retry attempts |
| `HTTP_TIMEOUT` | `30` | Request timeout in seconds |

## Tables Created

| Layer | Table | Type |
|---|---|---|
| Bronze | `bronze.fhir_patient_bronze` | Delta table |
| Bronze | `bronze.fhir_encounter_bronze` | Delta table |
| Bronze | `bronze.fhir_observation_bronze` | Delta table |
| Bronze | `bronze.fhir_condition_bronze` | Delta table |
| Silver | `silver.fhir_patient_silver` | Delta table with SCD Type 2 |
| Silver | `silver.fhir_encounter_silver` | Delta table with SCD Type 2 |
| Silver | `silver.fhir_observation_silver` | Delta table with SCD Type 2 |
| Silver | `silver.fhir_condition_silver` | Delta table with SCD Type 2 |
| Gold | `gold.fhir_patient_summary` | View |
| Gold | `gold.fhir_encounter_summary` | View |
| Gold | `gold.fhir_observation_summary` | View |
| Gold | `gold.fhir_condition_summary` | View |

## SCD Type 2 Versioning

Silver tables track historical changes using:

- `content_hash` — SHA-256 fingerprint for change detection.
- `is_current` — `true` for the current version and `false` for historical versions.
- `effective_from` — Timestamp when the version became active.
- `effective_to` — Timestamp when the version was replaced; `9999-12-31` represents the active version.

## Author

**Mohan Vamsi G** — Data & AI Engineer
