# Qiddiya Theme & Aqua Park – Synthetic Data + dbt Databricks

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.11.7+-orange.svg)](https://www.getdbt.com/)
[![Databricks](https://img.shields.io/badge/Databricks-SQL-red.svg)](https://databricks.com/)

An end-to-end data engineering project for a hypothetical theme park (Qiddiya Six Flags "SF" and Aqua Park "AQ"):

1. **Synthetic data generation**: a Python script generates realistic, referentially intact raw data with controlled data-quality anomalies. It then uploads the data to a Databricks SQL Warehouse landing zone.
2. **dbt Databricks project**: models the landing data into a **Medallion architecture** (Bronze → Silver → Gold star schema).

The two components run **independently**. The landing-zone tables in Databricks are the only link between them.

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Python Environment Setup](#2-python-environment-setup)
3. [Synthetic Data Generation](#3-synthetic-data-generation)
4. [dbt Databricks Setup](#4-dbt-databricks-setup)
5. [dbt Execution](#5-dbt-execution)
6. [Data Model](#6-data-model)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. Project Structure

```text
qiddiya_dbt/
├── _qiddiya_synthetic_data_generartion/
│   ├── qiddiya_load_databricks.py   # Synthetic data generator + Databricks uploader
│   └── .env                         # Local only (gitignored): DATABRICKS_TOKEN=...
│
├── dheeraj_qiddiya_dbt_databricks/  # dbt project
│   ├── dbt_project.yml
│   ├── packages.yml / package-lock.yml   # dbt_utils
│   ├── macros/generate_schema_name.sql   # Uses bronze/silver/gold schema names as-is
│   ├── models/
│   │   ├── source/source.yaml       # edw_dev.landing_zone (18 raw tables)
│   │   ├── bronze/                  # 9 models: SF + AQ union per entity
│   │   ├── silver/                  # 8 incremental models: typed, cleansed, deduplicated
│   │   └── gold/                    # 8 dimensions + 5 facts (star schema)
│   ├── tests/                       # Singular data tests + profiles_template.yml
│   ├── analyses/  seeds/  snapshots/
│
├── main.py                          # Launcher: `uv run main.py generate`
├── pyproject.toml                   # Dependencies (primary)
├── uv.lock                          # Locked dependency versions
├── requirements.txt                 # pip fallback, exported from uv.lock
├── .python-version                  # 3.11
├── .gitignore
└── README.md
```

---

## 2. Python Environment Setup

**Prerequisites:** Python 3.11, a Databricks workspace with a SQL Warehouse, and a Databricks Personal Access Token.

### Option A: uv (recommended, primary)

```powershell
cd qiddiya-data-sql-upload\src\qiddiya_dbt
uv sync                 # creates/updates .venv from uv.lock
```

You can prefix commands with `uv run`, or activate the environment with `.venv\Scripts\activate` (Windows) / `source .venv/bin/activate` (Linux/macOS).

### Option B: pip

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Dependencies (`pyproject.toml`)

| Package | Used by |
|---|---|
| `dbt-core`, `dbt-databricks` | dbt project |
| `databricks-sql-connector`, `pandas`, `numpy`, `faker`, `python-dotenv` | Synthetic data generator |

After changing `pyproject.toml`, run `uv lock`. Then run `uv export --format requirements-txt --no-hashes --no-emit-project -o requirements.txt` to keep `requirements.txt` in sync.

---

## 3. Synthetic Data Generation

### What it does

`_qiddiya_synthetic_data_generartion/qiddiya_load_databricks.py`:

1. Generates SF master data (staff, customers, products) and transactional data (transactions, gate events, ride operations, feedback, weather, master data).
2. Validates referential integrity, injects controlled anomalies (malformed timestamps, ~1% duplicates, inconsistent booleans) while protecting FK columns, then validates again.
3. Derives the AQ dataset from a clean SF copy and repeats the anomaly injection and validation.
4. Uploads **18 tables** (`sf_raw_*` and `aq_raw_*`) to `edw_dev.landing_zone`.

> **Warning:** Each upload runs `DROP TABLE IF EXISTS` + `CREATE TABLE` on the target tables in `edw_dev.landing_zone`.

The connection settings (`DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`) and row counts (`COUNTS`) are constants at the top of the script.

### Credentials

The script reads `DATABRICKS_TOKEN` from the environment. It also loads `_qiddiya_synthetic_data_generartion/.env` (gitignored) through `python-dotenv`:

```dotenv
DATABRICKS_TOKEN=<your-personal-access-token>
```

Or set it in the shell for the current session:

```powershell
$env:DATABRICKS_TOKEN = "<your-personal-access-token>"   # PowerShell
```

```bash
export DATABRICKS_TOKEN="<your-personal-access-token>"   # bash
```

### Run

```powershell
uv run main.py generate
```

Equivalent direct invocation:

```powershell
uv run python _qiddiya_synthetic_data_generartion/qiddiya_load_databricks.py
```

---

## 4. dbt Databricks Setup

### Profile

`dbt_project.yml` uses the profile **`dheeraj_qiddiya_dbt_databricks`**. dbt looks for `profiles.yml` in this order: the `--profiles-dir` argument, `DBT_PROFILES_DIR`, the current directory, then `~/.dbt/profiles.yml` (the default used by this project).

The template is in `dheeraj_qiddiya_dbt_databricks/tests/profiles_template.yml`. Read the token from an environment variable rather than hardcoding it:

```yaml
dheeraj_qiddiya_dbt_databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<warehouse-id>
      catalog: edw_dev
      schema: default
      threads: 1
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
```

All `profiles.yml` files are gitignored. Never commit a real token.

### Catalog and schemas

| Layer | Location | Materialization |
|---|---|---|
| Source | `edw_dev.landing_zone` | written by the generator |
| Bronze | `edw_dev.bronze` | table |
| Silver | `edw_dev.silver` | incremental (`on_schema_change: sync_all_columns`) |
| Gold | `edw_dev.gold` | table |

`macros/generate_schema_name.sql` makes dbt use `bronze` / `silver` / `gold` literally, without the default `<target_schema>_` prefix.

---

## 5. dbt Execution

Run all dbt commands from the dbt project directory:

```powershell
cd dheeraj_qiddiya_dbt_databricks

uv run dbt --version
uv run dbt deps          # installs dbt_utils into dbt_packages/
uv run dbt debug         # validates profile + Databricks connection
uv run dbt parse
uv run dbt compile
```

Commands that write to Databricks:

```powershell
uv run dbt run                         # all layers
uv run dbt run --select bronze         # one layer (bronze | silver | gold)
uv run dbt run --select silver_transactions --full-refresh
uv run dbt test                        # schema + singular tests
uv run dbt build                       # run + test in DAG order
```

Add `--profiles-dir <dir>` to use a `profiles.yml` from a location other than `~/.dbt`.

### Recommended end-to-end order

1. `uv run main.py generate` refreshes the landing zone.
2. `cd dheeraj_qiddiya_dbt_databricks && uv run dbt build` builds Bronze → Silver → Gold and runs the tests.

---

## 6. Data Model

**Bronze:** `raw_customers`, `raw_feedback`, `raw_gate_events`, `raw_master_data`, `raw_products`, `raw_ride_operations`, `raw_staff`, `raw_transactions`, `raw_weather`. Each one unions the SF and AQ landing tables and adds `source_region`.

**Silver:** `silver_customers`, `silver_feedback`, `silver_gate_events`, `silver_products`, `silver_ride_opeartions`, `silver_staff`, `silver_transactions`, `silver_weather`. These handle casting, boolean normalization, deduplication and `dbt_utils` surrogate keys.

**Gold (star schema):**

| Dimensions | Facts |
|---|---|
| `dim_customer`, `dim_date`, `dim_gate`, `dim_park`, `dim_product`, `dim_ride`, `dim_staff`, `dim_weather` | `fact_customer_feedback`, `fact_gate_event`, `fact_ride_operations`, `fact_transaction`, `fact_weather_observation` |

**Tests:** schema tests in `models/*/schema.yml`, plus singular tests in `tests/` (`gold_audit_reconciliation`, `gold_dim_park_count`, `gold_inactive_staff_in_facts`, `gold_late_arriving_dimensions`).

---

## 7. Troubleshooting

| Symptom | Fix |
|---|---|
| `ModuleNotFoundError: No module named 'faker'` / `'dotenv'` | Run `uv sync` (or `pip install -r requirements.txt`) in the project root. |
| `FATAL: DATABRICKS_TOKEN is not set` | Create `_qiddiya_synthetic_data_generartion/.env` or set `DATABRICKS_TOKEN` in the shell. |
| `Could not find profile named 'dheeraj_qiddiya_dbt_databricks'` | Create `~/.dbt/profiles.yml` from the template, or pass `--profiles-dir`. |
| `Env var required but not provided: 'DATABRICKS_TOKEN'` (dbt) | Your profile uses `env_var('DATABRICKS_TOKEN')`. Set it in the shell before running dbt. |
| `dbt_utils` macro not found | Run `uv run dbt deps`. |
| `dbt --version` hangs on a restricted network | It checks PyPI for updates. Set `DBT_VERSION_CHECK=false`. |
| `CustomTopLevelKeyDeprecation` warning for `tests/profiles*.yml` | dbt parses YAML files in `tests/`. The warning is harmless. Moving profile files out of `tests/` removes it. |
| `MissingArgumentsPropertyInGenericTestDeprecation` warnings | dbt 1.11 prefers generic-test arguments nested under `arguments:`. These are warnings only. |
| Silver model schema mismatch after source changes | Re-run with `--full-refresh` for the affected incremental model. |

---
*Maintained and developed by Dheeraj Kumar.*
