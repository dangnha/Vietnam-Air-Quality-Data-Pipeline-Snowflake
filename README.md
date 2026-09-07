# Vietnam Air Quality — Snowflake-Native Data Warehouse

An  end-to-end **ELT pipeline** that ingests air-quality data for
5 Vietnamese provinces and models it with the **Medallion architecture**
(Bronze → Silver → Gold) entirely inside **Snowflake**.

The project re-implements a classic [AWS Data Pipeline](https://github.com/ThuanNaN/Vietnam-Air-Quality-Data-Pipeline) (S3 + Glue + Lambda +
Step Functions + Athena) using only Snowflake-native features: `VARIANT` +
`FLATTEN`, **Streams** (change data capture), **Tasks** (scheduler), **SQL
stored procedures**, **Time Travel**, **Zero-Copy Clone**, automatic
**micro-partitioning**, and a **resource monitor** for cost control.

> **Same data, same business outcome - a different platform philosophy.**
> Snowflake collapses storage, catalog, compute, and query engine into a single
> purpose-built warehouse.

---

## Table of Contents

1. [What this project teaches](#what-this-project-teaches)
2. [Architecture](#architecture)
3. [AWS → Snowflake mapping](#aws--snowflake-mapping)
4. [Data model](#data-model)
5. [Project structure](#project-structure)
6. [Prerequisites](#prerequisites)
7. [Setup guide](#setup-guide)
8. [Running the pipeline](#running-the-pipeline)
9. [Snowflake-native orchestration](#snowflake-native-orchestration)
10. [Data quality gate](#data-quality-gate)
11. [Snowflake features demo](#snowflake-features-demo)
12. [Cost control](#cost-control)
13. [Testing](#testing)
14. [Why Snowflake as the warehouse](#why-snowflake-as-the-warehouse)

---

## What this project teaches

| Concept | Where you learn it |
|---------|-------------------|
| Medallion architecture (Bronze → Silver → Gold) | `sql/01–05` |
| ELT vs ETL, raw preservation via `VARIANT` | `sql/02–03`, `python/load/bronze.py` |
| Dimensional modeling (star schema: `dim_station` + `fact_aqi`) | `sql/04` |
| Incremental processing with **Streams + Tasks** | `sql/06` |
| SQL stored procedures as reusable transforms | `sql/06` |
| A native data-quality gate that blocks downstream Gold | `sql/06` `CONTROL.RUN_DQ_GATE` |
| Snowpark Python loader with key-pair auth | `python/session.py`, `python/load/bronze.py` |
| Time Travel, Zero-Copy Clone, micro-partitions | `sql/07` |
| Cost control with a resource monitor | `sql/00` |

---

## Architecture

```
                 Extract (Python)                      Load (Snowpark)
  ┌──────────────────────────────┐            ┌─────────────────────────────┐
  │  WAQI API (3x/day, 5 cities) │──────────▶ │  BRONZE (raw, VARIANT JSON) │
  │  Seed data (deterministic)   │            │  + historical CSV           │
  └──────────────────────────────┘            └─────────────────────────────┘
                                                           │  Streams (CDC)
                                                           ▼
                                           ┌─────────────────────────────┐
                                           │  SILVER (star schema)       │
                                           │  dim_station + fact_aqi     │
                                           │  SQL stored procedures      │
                                           └─────────────────────────────┘
                                                           │  DQ gate (SQL proc)
                                                           ▼
                                           ┌─────────────────────────────┐
                                           │  GOLD (analytics-ready)     │
                                           │  daily / city / station     │
                                           │  SQL stored procedures      │
                                           └─────────────────────────────┘
                                                           │
                                           Tasks / Time Travel /
                                           Zero-Copy Clone  →  any SQL BI
```

The pipeline can be driven two ways, using the **same** stored procedures:

1. **Snowpark client** — `python/orchestrate/native_pipeline.py` runs the
   Bronze → Silver → DQ → Gold sequence locally.
2. **Snowflake Tasks** — a self-contained task DAG schedules and runs the same
   procedures on Snowflake's own compute, with zero local process.

That dual path is the core teaching point: Snowflake can often **replace the
external orchestrator and transformation cluster entirely**.

---

## AWS → Snowflake mapping

| AWS service | Role in the original pipeline | Snowflake equivalent |
|-------------|-------------------------------|----------------------|
| S3 | Object storage for every layer | Tables inside the warehouse (storage is built-in) |
| Glue Data Catalog + Crawler | Schema discovery | `VARIANT` column + `FLATTEN` (no crawler needed) |
| Glue Jobs (PySpark) | Transform Bronze → Silver → Gold | SQL **stored procedures** (no Spark cluster) |
| Lambda `lambda_waqi_ingestion` | API ingestion | `python/extract/waqi_api.py` |
| Lambda `dq_check_silver` | Quality gate | `CONTROL.RUN_DQ_GATE()` (SQL proc) |
| Step Functions | Orchestration | **Task DAG** (`AFTER` chains) or Snowpark client |
| EventBridge Schedules | Trigger on a schedule | **Task `SCHEDULE`** |
| Athena | Query Silver | Snowflake SQL (same engine, no separate query service) |
| QuickSight | Dashboards | Any Snowflake BI tool (Tableau, Power BI, Streamlit, …) |
| SNS | Alerts | `CONTROL.DQ_RESULTS` audit + `RAISE` on failure |

---

## Data model

The warehouse is split into four schemas, one per layer plus a control schema.

### Bronze — raw, immutable

| Table | Grain | Notes |
|-------|-------|-------|
| `BRONZE.API_RAW` | one WAQI API response | full JSON kept in a `VARIANT` column |
| `BRONZE.HISTORICAL_CSV` | one 2021 CSV record | all columns kept as `STRING` |

### Silver — cleaned star schema

| Table | Grain | Notes |
|-------|-------|-------|
| `SILVER.DIM_STATION` | one station per city | conformed dimension (`waqi_idx + queried_city`) |
| `SILVER.FACT_AQI` | one measurement at one station/time | clustered by city + time |

### Gold — analytics-ready aggregates

| Table | Grain |
|-------|-------|
| `GOLD.AQI_DAILY_SUMMARY` | city + day |
| `GOLD.AQI_CITY_RANKING` | city + month (ranked) |
| `GOLD.STATION_SUMMARY` | station + month (ranked within city) |

### Control — pipeline metadata

| Table | Purpose |
|-------|---------|
| `CONTROL.CITY_MAP` | normalizes CSV city names to WAQI slugs |
| `CONTROL.DQ_RESULTS` | audit log of quality checks |
| `CONTROL.LOADED_FILES` | idempotency guard for the loader |

**City conformance** is important: the CSV source uses human-readable names
(`Ha Noi`, `Ho Chi Minh City`) while the API uses slugs (`ha-noi`,
`ho-chi-minh-city`). `CONTROL.CITY_MAP` normalizes both so station dedup and the
quality gate work across sources.

---

## Project structure

```
snowflake/
├── sql/                            # All Snowflake DDL/DML 
│   ├── 00_account_setup.sql        # warehouse + $400 resource monitor
│   ├── 01_databases_schemas.sql    # schemas + CONTROL tables + city map
│   ├── 02_file_formats_stages.sql  # file formats + internal stage
│   ├── 03_bronze_tables.sql        # raw VARIANT/STRING tables
│   ├── 04_silver_tables.sql        # star schema (dim + fact)
│   ├── 05_gold_tables.sql          # aggregate tables
│   ├── 06_streams_tasks.sql        # native ELT: procedures + DQ + task DAG
│   └── 07_snowflake_features_demo.sql  # Time Travel / Clone / clustering
├── python/                         # Snowpark Python (loader + orchestration)
│   ├── extract/                    #   live WAQI API fetch
│   ├── load/                       #   Snowpark Bronze loader (PUT + COPY)
│   ├── orchestrate/                #   end-to-end native pipeline runner
│   ├── session.py                  #   key-pair Snowpark session factory
│   └── config.py                   #   typed config accessors
├── scripts/
│   ├── deploy.py                   # apply sql/*.sql in order
│   └── generate_seed.py            # deterministic 5-city seed data
├── config/
│   └── config.example.yaml         # template for config.yaml
├── tests/                          # offline unit + opt-in integration tests
├── Makefile                        # convenience commands
└── requirements.txt
```

> `data/`, `docs/`, `.commandcode/`, and `config/config.yaml` are git-ignored.
> They are created locally during setup.

---

## Prerequisites

- **Python 3.9+**
- **A Snowflake account** (a free trial works)
- **`openssl`** (to generate an RSA key pair)
- *(Optional)* a **WAQI API token** for live data — the project ships with a
  deterministic seed generator, so no token is required to run offline

---

## Setup guide

### 1. Install dependencies

```bash
make bootstrap
```

This creates a virtual environment and installs `requirements.txt`
(`snowflake-snowpark-python`, `snowflake-connector-python`, `cryptography`,
`PyYAML`, `requests`, `pytest`).

### 2. Generate an RSA key pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

The `.p8` file is your private key (keep it secret). The `.pub` file is assigned
to your Snowflake user.

### 3. Assign the public key to your Snowflake user

In Snowflake (Snowsight → Worksheets), run:

```sql
ALTER USER <your_user> SET RSA_PUBLIC_KEY = '<paste the full contents of rsa_key.pub>';
```

### 4. Configure the project

```bash
cp config/config.example.yaml config/config.yaml
```

Edit `config/config.yaml` with your values:

```yaml
snowflake:
  account: "xy12345.ap-southeast-2"   # your account locator
  user: "your_user"
  role: "SYSADMIN"
  warehouse: "AQ_WH"
  database: "AQ_WAREHOUSE"
  private_key_path: "~/.snowflake/rsa_key.p8"
  private_key_passphrase: ""
```

### 5. Deploy the Snowflake objects

```bash
make deploy
```

`scripts/deploy.py` executes `sql/*.sql` in order to create:

- the `AQ_WH` warehouse and `AQ_BUDGET_MONITOR` resource monitor
- the `AQ_WAREHOUSE` database and its four schemas
- file formats, stages, tables, streams, stored procedures, and the task DAG

> **Important:** `sql/00_account_setup.sql` must run as `ACCOUNTADMIN`. If your
> configured role is not `ACCOUNTADMIN`, run file `00` manually first in
> Snowsight, then run `make deploy` for the remaining files.

### 6. Generate seed data

```bash
make seed
```

This creates deterministic sample data in `data/sample/`: 5 cities × 30 days ×
3 measurements/day (WAQI-style JSON) plus a 2021 historical CSV. No network
access is required.

---

## Running the pipeline

### Option A — one command (Snowpark client)

```bash
make etl
```

This runs the full sequence:

```
load Bronze  →  CALL SILVER.LOAD_FROM_BRONZE()  →  CALL CONTROL.RUN_DQ_GATE()  →  CALL GOLD.LOAD_ALL()
```

If the quality gate fails, Gold is skipped.

### Option B — step by step

```bash
make load     # PUT files into the stage + COPY INTO Bronze
make etl      # Silver + DQ + Gold via stored procedures
```

### Verify the result

```sql
SELECT queried_city, COUNT(*) AS rows
FROM AQ_WAREHOUSE.GOLD.AQI_CITY_RANKING
GROUP BY 1;
```

You should see all 5 cities with populated rankings.

---

## Snowflake-native orchestration

After Bronze data exists, you can hand scheduling to Snowflake itself. Resume the
task DAG and Streams + Tasks will keep Silver → DQ → Gold fresh on their own:

```sql
ALTER TASK SILVER.LOAD_SILVER_TASK RESUME;
ALTER TASK CONTROL.RUN_DQ_TASK RESUME;
ALTER TASK GOLD.LOAD_GOLD_TASK RESUME;
```

The DAG:

```
LOAD_SILVER_TASK  →  RUN_DQ_TASK  →  LOAD_GOLD_TASK
     (streams)          (gate)          (gold)
```

`LOAD_SILVER_TASK` only runs when a stream reports new Bronze rows; the `AFTER`
dependencies guarantee correct ordering with no external scheduler.

---

## Data quality gate

`CONTROL.RUN_DQ_GATE()` runs six checks against `SILVER.FACT_AQI`:

| Check | Description |
|-------|-------------|
| `row_count` | enough rows (min 10) |
| `null_pct` | critical columns under 5% null |
| `aqi_range` | AQI within [0, 500] |
| `city_coverage` | all 5 cities present |
| `source_validity` | only `kaggle` / `api` |
| `freshness` | at least one row ingested within 48h |

Each check writes a row to `CONTROL.DQ_RESULTS`. If any fail, the procedure
`RAISE`s — which fails `RUN_DQ_TASK` and prevents `LOAD_GOLD_TASK` from running.
This is the Snowflake-native replacement for the AWS "stop the pipeline" SNS
behavior.

---

## Snowflake features demo

Run `sql/07_snowflake_features_demo.sql` interactively in Snowsight to explore:

- **`VARIANT` + `FLATTEN`** — query raw JSON with SQL, no crawler
- **Time Travel** — rewind a table with `AT (OFFSET => -60*60)`
- **Zero-Copy Clone** — instant dev copy with `CLONE`
- **Micro-partitions** — inspect `SYSTEM$CLUSTERING_INFORMATION`
- **Elastic warehouses** — spin up an `XL` on demand, pay per second

---

## Cost control

The project enforces a hard budget with a Snowflake resource monitor:

- `AQ_WH` = **XSMALL**, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`, single cluster
- `AQ_BUDGET_MONITOR` = **400 credits/month**

```sql
CREATE RESOURCE MONITOR AQ_BUDGET_MONITOR
  WITH CREDIT_QUOTA = 400 FREQUENCY = MONTHLY
  TRIGGERS
    ON 75 PERCENT  DO NOTIFY
    ON 90 PERCENT  DO NOTIFY
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;
```

These XSMALL jobs cost cents per run; even a full month of scheduled runs plus
demo queries stays orders of magnitude under the budget.

---

## Testing

```bash
make test
```

Runs offline unit tests (no Snowflake credentials required):

- configuration parsing and accessors
- seed-data generator structure
- city conformance map coverage
- Snowflake-disabled detection without credentials

To run the opt-in integration tests against your real account:

```bash
RUN_SNOWFLAKE_INTEGRATION=1 make test
```

These verify the warehouse and resource monitor actually exist in your account.

---

## Why Snowflake as the warehouse

1. **No separate storage, catalog, and query engine** — tables replace S3,
   `VARIANT` replaces the Glue Crawler, and SQL replaces Athena.
2. **ELT instead of strict ETL** — load raw first, transform later, re-derive
   Silver/Gold anytime.
3. **Compute that scales to zero** — warehouses auto-suspend and bill per second.
4. **Streams + Tasks replace external orchestration** — CDC and scheduling are
   native.
5. **A native quality gate can stop the pipeline** — `RAISE` on failure skips
   downstream tasks.
6. **Time Travel & Zero-Copy Clone** — instant rewinds and dev copies with no
   storage duplication.
7. **Automatic micro-partitioning** — clustering keys replace manual Parquet
   folder partitioning.
8. **Resource monitors give you a hard budget** — exactly the guardrail the
   original pipeline would have needed to build manually.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `make deploy` fails on file `00` | role lacks `ACCOUNTADMIN` | run `00` manually as `ACCOUNTADMIN` |
| `make etl` can't connect | wrong account locator or key path | check `config/config.yaml` |
| private key auth rejected | public key not assigned to user | re-run `ALTER USER ... SET RSA_PUBLIC_KEY` |
| DQ gate fails `freshness` | rows loaded more than 48h ago | reload seed data, or re-run the load |
| no Snowflake modules found | `make bootstrap` not run | run `make bootstrap` first |

## Stop Snowflake resources after testing

> **⚠️ Important:** Snowflake compute can incur charges while your project is running. After testing the pipeline, suspend the warehouse and scheduled tasks if you do not need them anymore.

### 1. Suspend the warehouse

Run the following in Snowsight:

```sql
ALTER WAREHOUSE AQ_WH SUSPEND;
```

You can verify its state with:

```sql
SHOW WAREHOUSES LIKE 'AQ_WH';
```

### 2. Suspend the pipeline tasks

If you enabled the Snowflake Task DAG, suspend the tasks after testing:

```sql
ALTER TASK GOLD.LOAD_GOLD_TASK SUSPEND;
ALTER TASK CONTROL.RUN_DQ_TASK SUSPEND;
ALTER TASK SILVER.LOAD_SILVER_TASK SUSPEND;
```

This prevents the scheduled pipeline from continuing to execute.

### 3. Optional: remove the project

If you are completely finished with the project and no longer need its data or Snowflake objects, you can remove the warehouse and database:

```sql
DROP WAREHOUSE IF EXISTS AQ_WH;
DROP DATABASE IF EXISTS AQ_WAREHOUSE;
```

> **Warning:** `DROP DATABASE` permanently removes the project's Snowflake data and objects. Only run it if you are sure you no longer need them.

### Quick cleanup

For a temporary test, the minimum recommended cleanup is:

```sql
ALTER TASK GOLD.LOAD_GOLD_TASK SUSPEND;
ALTER TASK CONTROL.RUN_DQ_TASK SUSPEND;
ALTER TASK SILVER.LOAD_SILVER_TASK SUSPEND;

ALTER WAREHOUSE AQ_WH SUSPEND;
```

The project also configures a Snowflake resource monitor as a cost-control guardrail, but users should still suspend unused resources after testing rather than relying solely on the monitor.
