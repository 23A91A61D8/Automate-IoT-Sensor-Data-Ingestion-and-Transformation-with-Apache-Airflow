# IoT Sensor Data Ingestion & Transformation Pipeline

> **Domain:** Data Engineering | **Difficulty:** Hard  
> **Tools:** Apache Airflow 2.7.2 · Docker Compose · PostgreSQL 13 · Pandas · Python 3.9

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Project Structure](#3-project-structure)
4. [Prerequisites](#4-prerequisites)
5. [Quick Start](#5-quick-start)
6. [Detailed Setup](#6-detailed-setup)
7. [DAG Explanation](#7-dag-explanation)
8. [Data Quality Logic](#8-data-quality-logic)
9. [Idempotency Design](#9-idempotency-design)
10. [Error Handling & Retries](#10-error-handling--retries)
11. [Running Unit Tests](#11-running-unit-tests)
12. [Verifying Data in the Database](#12-verifying-data-in-the-database)
13. [Architectural Decisions](#13-architectural-decisions)
14. [Environment Variables Reference](#14-environment-variables-reference)
15. [Troubleshooting](#15-troubleshooting)
16. [Clean-up](#16-clean-up)

---

## 1. Project Overview

This project implements a **production-ready, automated data pipeline** using Apache Airflow that:

- **Simulates** IoT sensor data (temperature, humidity, pressure) for 10 virtual devices.
- **Ingests** raw CSV batches into a PostgreSQL data warehouse.
- **Transforms** data: type coercion → null imputation → outlier clipping → unit conversion → normalisation → enrichment.
- **Validates** data with 10 structured quality checks (null checks, range checks, duplicate detection, etc.).
- **Loads** daily aggregated metrics (per-device averages) into a processed table using idempotent UPSERTs.

The entire environment runs in Docker Compose — no local Python or PostgreSQL installation required.

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Network                        │
│                                                                       │
│  ┌────────────┐   ┌───────────────┐   ┌───────────────────────────┐ │
│  │   Redis    │   │  Airflow      │   │  Airflow Metadata DB       │ │
│  │  (Celery   │   │  Webserver    │   │  (postgres:13, port 5432)  │ │
│  │   broker)  │   │  port 8080    │   └───────────────────────────┘ │
│  └────────────┘   └───────────────┘                                  │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │                   Airflow Scheduler                           │    │
│  │  ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌──────────┐  │    │
│  │  │ generate │→ │  ingest   │→ │ transform │→ │  quality │  │    │
│  │  │  _data   │  │ _raw_data │  │  _data    │  │  _checks │  │    │
│  │  └──────────┘  └───────────┘  └───────────┘  └──────────┘  │    │
│  │                                                      ↓        │    │
│  │                                               ┌──────────┐   │    │
│  │                                               │   load   │   │    │
│  │                                               │ _data    │   │    │
│  │                                               └──────────┘   │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                  ↕                                    │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │         IoT Data Warehouse (postgres:13, port 5433)           │    │
│  │   raw_sensor_data  │  processed_sensor_data  │  quarantine    │    │
│  └──────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

**Data flow per hourly DAG run:**

```
data/sensor_readings_YYYYMMDDHHMM.csv
        │
        ▼
raw_sensor_data (PostgreSQL)
        │
        ▼ (transformation pipeline)
Pandas in-memory DataFrame
        │
        ▼ (10 DQ checks)
processed_sensor_data (PostgreSQL)
```

---

## 3. Project Structure

```
project_root/
├── dags/
│   └── iot_data_pipeline.py        # Airflow DAG (5 tasks)
├── scripts/
│   ├── generate_sensor_data.py     # CSV simulation script
│   ├── data_transformations.py     # Pure transformation functions
│   └── data_quality.py             # DQ check functions + reporter
├── tests/
│   ├── conftest.py                 # Shared pytest fixtures
│   ├── test_data_transformations.py
│   ├── test_data_quality.py
│   └── test_dag.py                 # DAG structural tests
├── config/
│   └── database_schema.sql         # Auto-applied on container startup
├── data/                           # Runtime CSV files (git-ignored)
├── logs/                           # Airflow task logs (git-ignored)
├── plugins/                        # Airflow plugins (empty placeholder)
├── .env.example                    # Environment variable template
├── .gitignore
├── docker-compose.yml
├── Dockerfile                      # Custom Airflow image
├── requirements.txt
└── README.md
```

---

## 4. Prerequisites

| Requirement | Minimum Version |
|-------------|----------------|
| Docker Desktop | 24.x |
| Docker Compose | 2.x (`docker compose` CLI) |
| Available RAM | 4 GB |
| Available Disk | 10 GB |
| OS | Windows 10/11, macOS 12+, Ubuntu 20.04+ |

> **Windows users:** Enable WSL 2 backend in Docker Desktop settings.

---

## 5. Quick Start

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd <project-root>

# 2. Configure environment
cp .env.example .env
# Edit .env and set secure passwords + a Fernet key

# 3. Generate Fernet key (run once)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into AIRFLOW__CORE__FERNET_KEY in .env

# 4. Create required local directories
mkdir -p logs plugins data

# 5. (Linux/macOS only) Set AIRFLOW_UID
echo "AIRFLOW_UID=$(id -u)" >> .env

# 6. Start all services
docker compose up -d

# 7. Wait ~2 minutes for initialisation, then open:
#    Airflow UI: http://localhost:8080  (user: admin, password: from .env)
#    Data WH:   localhost:5433         (user/db: from .env)
```

---

## 6. Detailed Setup

### 6.1 Environment Configuration

Copy `.env.example` to `.env` and update every value:

```bash
cp .env.example .env
```

Key variables to change:

| Variable | Description |
|---|---|
| `POSTGRES_PASSWORD` | Airflow metadata DB password |
| `DW_USER` / `DW_PASSWORD` | Data warehouse credentials |
| `AIRFLOW__CORE__FERNET_KEY` | Encryption key (generate once) |
| `_AIRFLOW_WWW_USER_PASSWORD` | Airflow web UI admin password |

### 6.2 Build and Start

```bash
# Build the custom Airflow image (installs project Python deps)
docker compose build

# Start all containers in detached mode
docker compose up -d

# Tail logs to watch initialisation
docker compose logs -f airflow-init
```

### 6.3 Verify All Containers Are Healthy

```bash
docker compose ps
```

Expected output — all containers should show `healthy` or `running`:

```
NAME                   STATUS          PORTS
airflow_init           Exited (0)      # one-shot, exits after setup
airflow_webserver      Up (healthy)    0.0.0.0:8080->8080/tcp
airflow_scheduler      Up (healthy)
airflow_worker         Up (healthy)
airflow_triggerer      Up (healthy)
airflow_metadata_db    Up (healthy)    5432/tcp
data_warehouse_db      Up (healthy)    0.0.0.0:5433->5432/tcp
airflow_redis          Up (healthy)    6379/tcp
```

### 6.4 Register the Data Warehouse Connection in Airflow

The connection is auto-registered via the `AIRFLOW_CONN_DATA_WAREHOUSE_POSTGRES` environment variable in `docker-compose.yml`. To verify:

```bash
docker compose exec airflow-scheduler airflow connections get data_warehouse_postgres
```

Or add it manually in the Airflow UI:  
**Admin → Connections → + Add**

| Field | Value |
|---|---|
| Connection Id | `data_warehouse_postgres` |
| Connection Type | `Postgres` |
| Host | `data-warehouse-db` |
| Schema | `iot_sensors` |
| Login | value of `DW_USER` |
| Password | value of `DW_PASSWORD` |
| Port | `5432` |

### 6.5 Enable and Trigger the DAG

1. Open [http://localhost:8080](http://localhost:8080) and log in.
2. Search for `iot_data_pipeline`.
3. Toggle the DAG from **Paused** → **Active** (the blue switch).
4. Click **▶ Trigger DAG** to run it immediately.

---

## 7. DAG Explanation

**DAG ID:** `iot_data_pipeline`  
**Schedule:** `@hourly`  
**Catchup:** Disabled (only the current run executes)  
**Max active runs:** 1 (prevents concurrent overlapping runs)

### Task Graph

```
generate_sensor_data
        │
        ▼
ingest_raw_data
        │
        ▼
transform_data
        │
        ▼
run_data_quality_checks
        │
        ▼
load_processed_data
```

### Task Descriptions

| Task | Operator | Description |
|---|---|---|
| `generate_sensor_data` | `PythonOperator` | Generates a timestamped CSV with 100–500 simulated sensor readings. XComs the file path. |
| `ingest_raw_data` | `PythonOperator` | Reads the CSV via Pandas; UPSERTs rows into `raw_sensor_data`. `ON CONFLICT DO NOTHING` ensures idempotency. |
| `transform_data` | `PythonOperator` | Fetches unprocessed raw rows (2-hour window), runs the full transformation pipeline, aggregates to daily device averages. XComs a JSON payload. |
| `run_data_quality_checks` | `PythonOperator` | Runs 10 DQ checks on the aggregated DataFrame. Critical failures raise `RuntimeError` → DAG halts. Non-critical failures are logged. |
| `load_processed_data` | `PythonOperator` | UPSERTs validated aggregates into `processed_sensor_data`. Marks source raw rows as `processed_flag = TRUE`. |

### XCom Data Flow

```
generate_sensor_data  →  filepath (str)
                            │
ingest_raw_data        ← filepath
                            │
transform_data         → JSON payload {transformed_records, raw_ids}
                            │
run_data_quality_checks ← JSON payload → DQ report dict
                            │
load_processed_data    ← JSON payload + DQ report
```

---

## 8. Data Quality Logic

All quality checks live in [`scripts/data_quality.py`](scripts/data_quality.py).

### Checks Implemented

| Check | Type | Action on Failure |
|---|---|---|
| `required_columns` | Critical | Raise `RuntimeError` → DAG halts |
| `null_critical_columns` (device_id, timestamp) | Critical | Raise `RuntimeError` → DAG halts |
| `null_numeric_columns` (temp, humidity, pressure) | Soft | Log warning |
| `temperature_range` (−50 to 150 °C) | Critical | Raise `RuntimeError` → DAG halts |
| `humidity_range` (0–100 %) | Critical | Raise `RuntimeError` → DAG halts |
| `pressure_range` (870–1085 hPa) | Soft | Log warning |
| `device_id_format` (no blanks) | Soft | Log warning |
| `timestamp_monotonicity` | Advisory | Log warning (never fails) |
| `duplicate_records` | Soft | Log warning |
| `row_count` (≥ 1 row) | Soft | Log warning |

### Quarantine Strategy

Before DQ checks run, `quarantine_bad_rows()` separates rows that violate hard constraints (null critical fields, out-of-range sensors). Quarantined rows are logged with a failure reason. Production extension: write them to `quarantined_sensor_data` table.

### DQ Report Structure (XCom)

```json
{
  "overall_passed": true,
  "checks": {
    "required_columns": {"passed": true, "issues": [], "critical": true},
    "temperature_range": {"passed": true, "issues": [], "critical": true},
    ...
  }
}
```

---

## 9. Idempotency Design

Each task is safe to re-run with the same `execution_date`:

| Task | Idempotency Mechanism |
|---|---|
| `generate_sensor_data` | File named `sensor_readings_YYYYMMDDHHMM.csv` — re-run overwrites same file |
| `ingest_raw_data` | `ON CONFLICT (device_id, timestamp) DO NOTHING` |
| `transform_data` | Queries `WHERE processed_flag = FALSE` — already-processed rows are skipped |
| `run_data_quality_checks` | Stateless check function; re-running is safe |
| `load_processed_data` | `ON CONFLICT (device_id, reading_date) DO UPDATE` (UPSERT) |

---

## 10. Error Handling & Retries

### Retry Configuration (all tasks)

```python
"retries": 3,
"retry_delay": timedelta(minutes=5),
"retry_exponential_backoff": True,
"max_retry_delay": timedelta(minutes=30),
```

### Failure Callbacks

Every task has `on_failure_callback=_on_failure_callback` which logs:
- DAG ID, Task ID, Execution Date, Exception details

Production extension points (pre-wired as TODO comments):
- Slack webhook notification
- PagerDuty incident creation
- Email alert via Airflow's SMTP integration

### Exception Handling

All DB operations are wrapped in `try/finally` to ensure connections are closed even on failure. Critical DQ failures raise `RuntimeError` which propagates cleanly through Airflow's retry mechanism.

---

## 11. Running Unit Tests

### Local (with virtual environment)

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate         # Linux/macOS
venv\Scripts\activate            # Windows

# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ -v --cov=scripts --cov-report=html
# Open htmlcov/index.html to view coverage

# Run specific test file
pytest tests/test_data_transformations.py -v
pytest tests/test_data_quality.py -v
pytest tests/test_dag.py -v      # requires airflow installed
```

### Inside Docker

```bash
docker compose exec airflow-worker bash -c "
  cd /opt/airflow && \
  pip install pytest pytest-cov --quiet && \
  python -m pytest /opt/airflow/dags/../tests/ -v
"
```

### Expected Test Output

```
tests/test_data_transformations.py::TestCoerceDtypes::test_timestamp_becomes_datetime PASSED
tests/test_data_transformations.py::TestCoerceDtypes::test_numeric_columns_are_float PASSED
... (40+ tests)
tests/test_data_quality.py::TestRunAllChecks::test_clean_data_all_pass PASSED
... (50+ tests)
tests/test_dag.py::TestDagLoading::test_no_import_errors PASSED
... (15+ tests)
```

---

## 12. Verifying Data in the Database

Connect to the data warehouse from your host machine:

```bash
# Using psql (if installed locally)
psql -h localhost -p 5433 -U <DW_USER> -d iot_sensors

# Or via Docker
docker compose exec data-warehouse-db psql -U <DW_USER> -d iot_sensors
```

### Verification Queries

```sql
-- Count raw ingested records
SELECT COUNT(*) FROM raw_sensor_data;

-- View latest raw readings
SELECT device_id, timestamp, temperature, humidity, pressure, processed_flag
FROM raw_sensor_data
ORDER BY ingested_at DESC
LIMIT 10;

-- Count processed daily aggregates
SELECT COUNT(*) FROM processed_sensor_data;

-- View processed aggregates
SELECT device_id, reading_date, avg_temperature, avg_humidity,
       avg_pressure, reading_count, data_quality_flags
FROM processed_sensor_data
ORDER BY reading_date DESC, device_id;

-- Check processing rates
SELECT
    COUNT(*) FILTER (WHERE processed_flag = TRUE)  AS processed,
    COUNT(*) FILTER (WHERE processed_flag = FALSE) AS pending,
    COUNT(*)                                        AS total
FROM raw_sensor_data;

-- View quarantined records (if any)
SELECT * FROM quarantined_sensor_data;
```

---

## 13. Architectural Decisions

### Why CeleryExecutor?

The pipeline uses `CeleryExecutor` with Redis as the broker, mirroring production-grade Airflow deployments. This enables horizontal scaling by adding more `airflow-worker` containers without changing the DAG code.

### Why separate metadata DB and data warehouse?

Airflow's metadata DB (`postgres:5432`) manages DAG runs, task states, XComs, and connections. The data warehouse (`data-warehouse-db:5433`) is a clean, purpose-built store for IoT data. Mixing them would create operational coupling and complicate backup/restore procedures.

### Why XCom for inter-task data?

The aggregated DataFrame (post-transformation) is small (≤ 500 device-date rows per run) — well within Airflow's XCom size limits (~48 KB default). For larger datasets, the recommended pattern is to push data to object storage (S3/GCS) and XCom only the path.

### Why min-max normalisation with fixed bounds?

Using the physically plausible sensor bounds (e.g., temperature: −50 to 150 °C) instead of per-batch statistics ensures the normalisation function is deterministic and consistent across runs. Per-batch min-max would produce different scales for different batches, making time-series comparisons meaningless.

### Why `processed_flag` instead of a separate "watermark" table?

The `processed_flag` column on `raw_sensor_data` is simpler, requires no extra table, and makes it trivially easy to identify un-processed rows with an indexed query. It's atomically updated in the same transaction as the load step, preventing partial-processing scenarios.

### Why daily aggregation for `processed_sensor_data`?

Storing per-reading data in the processed table would bloat storage without adding analytical value for the target use case (dashboards, ML feature stores). Daily aggregates are the natural grain for trend analysis and predictive maintenance models while keeping query performance high.

---

## 14. Environment Variables Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_USER` | Yes | `airflow` | Airflow metadata DB username |
| `POSTGRES_PASSWORD` | Yes | — | Airflow metadata DB password |
| `DW_USER` | Yes | `airflow` | Data warehouse DB username |
| `DW_PASSWORD` | Yes | — | Data warehouse DB password |
| `DW_DB` | No | `iot_sensors` | Data warehouse database name |
| `AIRFLOW_UID` | Yes (Linux) | `50000` | Linux UID for file permissions |
| `AIRFLOW__CORE__FERNET_KEY` | Yes | — | Fernet key for encrypting secrets |
| `_AIRFLOW_WWW_USER_USERNAME` | No | `airflow` | Web UI admin username |
| `_AIRFLOW_WWW_USER_PASSWORD` | Yes | — | Web UI admin password |
| `SENSOR_BATCH_SIZE` | No | `250` | Rows per sensor CSV batch |
| `DW_CONN_ID` | No | `data_warehouse_postgres` | Airflow connection ID for data WH |

---

## 15. Troubleshooting

### Container fails to start

```bash
# View container logs
docker compose logs airflow-init
docker compose logs airflow-scheduler

# Check resource usage
docker stats
```

### DAG not appearing in Airflow UI

```bash
# Check for DAG parse errors
docker compose exec airflow-scheduler airflow dags list-import-errors
```

### Database connection errors

```bash
# Test connectivity to data warehouse from within Airflow
docker compose exec airflow-scheduler bash -c \
  "python -c \"import psycopg2; psycopg2.connect(host='data-warehouse-db', port=5432, dbname='iot_sensors', user='$DW_USER', password='$DW_PASSWORD'); print('OK')\""
```

### XCom size errors

If your batch size exceeds XCom limits, reduce `SENSOR_BATCH_SIZE` in `.env` or switch to file-based XCom storage (Airflow 2.7+ supports custom XCom backends).

### Permission errors on Linux

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose down && docker compose up -d
```

---

## 16. Clean-up

```bash
# Stop all containers (preserve data volumes)
docker compose down

# Stop and remove all data volumes (full reset)
docker compose down -v

# Remove generated CSV files
rm -f data/sensor_readings_*.csv

# Remove built Docker image
docker rmi iot-airflow:2.7.2
```

---

## License

This project is submitted as part of the Partnr Global Placement Program – Data Engineering track.

---

*Built with ❤️ using Apache Airflow 2.7.2, PostgreSQL 13, Pandas 2.1.4, and Docker Compose.*
