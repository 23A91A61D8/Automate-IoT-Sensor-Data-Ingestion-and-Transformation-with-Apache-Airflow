"""
iot_data_pipeline.py
--------------------
Apache Airflow DAG: IoT Sensor Data Ingestion and Transformation Pipeline.

Architecture
============
    generate_data
        │
        ▼
    ingest_raw_data          ← Extract: read CSVs → raw_sensor_data table
        │
        ▼
    transform_data           ← Transform: clean, enrich, aggregate raw rows
        │
        ▼
    run_data_quality_checks  ← Validate transformed data; quarantine bad rows
        │
        ▼
    load_processed_data      ← Load: UPSERT daily aggregates into
                                      processed_sensor_data table

All tasks are idempotent; re-running the same execution_date is safe.
Task failures trigger on_failure_callback for alerting / cleanup hooks.
XComs are used to pass lightweight payloads between tasks.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure scripts/ is importable when Airflow runs the DAG
DAG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAG_DIR.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DATA_DIR = PROJECT_ROOT / "data"
sys.path.insert(0, str(SCRIPTS_DIR))

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Project modules
from generate_sensor_data import generate_sensor_batch, write_sensor_csv
from data_transformations import apply_all_transformations, aggregate_daily
from data_quality import run_all_checks, quarantine_bad_rows

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG default args
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------------
DW_CONN_ID = os.getenv("DW_CONN_ID", "data_warehouse_postgres")
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_db_connection(conn_id: str):
    """
    Return a live psycopg2 connection obtained from an Airflow Connection.
    Falls back to direct environment-variable credentials when the
    Airflow metadata DB is unavailable (e.g. unit tests).
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        return psycopg2.connect(
            host=conn.host,
            port=conn.port or 5432,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
        )
    except Exception:  # noqa: BLE001
        # Fallback: read directly from environment variables
        logger.warning(
            "Airflow connection '%s' unavailable – falling back to env vars.", conn_id
        )
        return psycopg2.connect(
            host=os.environ.get("DW_HOST", "data-warehouse-db"),
            port=int(os.environ.get("DW_PORT", "5432")),
            dbname=os.environ.get("DW_DB", "iot_sensors"),
            user=os.environ.get("DW_USER", "airflow"),
            password=os.environ.get("DW_PASSWORD", "airflow"),
        )


def _on_failure_callback(context: dict) -> None:
    """
    Called by Airflow when any task fails.
    In production this would publish a PagerDuty / Slack alert.
    Here we log the failure and leave a placeholder for alert logic.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception")

    logger.error(
        "[ALERT] DAG '%s' | Task '%s' | Execution '%s' FAILED. Exception: %s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )
    # TODO: Integrate alerting system (Slack / PagerDuty / email)
    # Example:
    # send_slack_alert(channel="#data-alerts", dag=dag_id, task=task_id)


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def generate_sensor_data_fn(**context) -> str:
    """
    Generate a new batch of simulated sensor data CSV for this DAG run.

    Idempotency: the filename includes YYYYMMDDHHMM so re-running the same
    execution_date always writes to the same file; if it already exists the
    file is silently overwritten.

    XCom output: the path of the generated CSV.
    """
    execution_date: datetime = context["execution_date"]
    run_time = execution_date.replace(tzinfo=timezone.utc) if execution_date.tzinfo is None else execution_date

    logger.info("Generating sensor data for execution_date=%s", run_time)

    num_rows = int(os.getenv("SENSOR_BATCH_SIZE", "250"))
    df = generate_sensor_batch(num_rows=num_rows, start_time=run_time)
    filepath = write_sensor_csv(df, output_dir=str(DATA_DIR), run_time=run_time)

    logger.info("Sensor CSV ready: %s (%d rows)", filepath, len(df))
    return filepath  # pushed to XCom automatically


def ingest_raw_data_fn(**context) -> int:
    """
    Extract: Read the sensor CSV generated this run and UPSERT rows into
    raw_sensor_data.

    Idempotency: uses ON CONFLICT (device_id, timestamp) DO NOTHING to prevent
    duplicate inserts on task retry.

    XCom input : filepath from generate_sensor_data_fn.
    XCom output: number of rows inserted.
    """
    ti = context["task_instance"]
    filepath: str = ti.xcom_pull(task_ids="generate_sensor_data")
    if not filepath or not Path(filepath).exists():
        raise FileNotFoundError(
            f"Sensor CSV not found: '{filepath}'. "
            "Ensure generate_sensor_data task succeeded."
        )

    logger.info("Ingesting CSV: %s", filepath)
    df = pd.read_csv(filepath)
    logger.info("Read %d rows from CSV.", len(df))

    conn = _get_db_connection(DW_CONN_ID)
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO raw_sensor_data
                        (device_id, timestamp, temperature, humidity, pressure)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, timestamp) DO NOTHING
                    """,
                    (
                        row.get("device_id"),
                        row.get("timestamp"),
                        row.get("temperature") if pd.notna(row.get("temperature")) else None,
                        row.get("humidity") if pd.notna(row.get("humidity")) else None,
                        row.get("pressure") if pd.notna(row.get("pressure")) else None,
                    ),
                )
                if cur.rowcount:
                    inserted += 1

        logger.info("Inserted %d new rows into raw_sensor_data.", inserted)
    finally:
        conn.close()

    return inserted  # XCom


def transform_data_fn(**context) -> str:
    """
    Transform: Fetch unprocessed raw rows from raw_sensor_data and apply the
    full transformation pipeline.

    Idempotency: queries rows where processed_flag IS FALSE and marks them
    after processing. Re-running re-processes only un-flagged rows.

    XCom output: JSON string of the transformed + aggregated DataFrame.
    """
    execution_date: datetime = context["execution_date"]
    # Process rows from the past 2-hour window to cover slight timing drift
    window_start = execution_date - timedelta(hours=2)
    window_end = execution_date + timedelta(hours=1)

    logger.info(
        "Fetching raw data: window %s → %s", window_start.isoformat(), window_end.isoformat()
    )

    conn = _get_db_connection(DW_CONN_ID)
    try:
        df_raw = pd.read_sql(
            """
            SELECT id, device_id, timestamp, temperature, humidity, pressure
            FROM raw_sensor_data
            WHERE processed_flag = FALSE
              AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
            """,
            conn,
            params=(window_start.isoformat(), window_end.isoformat()),
        )
    finally:
        conn.close()

    if df_raw.empty:
        logger.warning("No unprocessed raw rows found in window. Skipping transformation.")
        return json.dumps([])

    logger.info("Fetched %d raw rows for transformation.", len(df_raw))

    # Store raw IDs to mark as processed after loading
    raw_ids = df_raw["id"].tolist()

    # Drop the DB id column before transforming
    df = df_raw.drop(columns=["id"])

    # Apply full transformation pipeline
    df_transformed = apply_all_transformations(df)

    # Aggregate to daily device averages
    df_agg = aggregate_daily(df_transformed)

    # Embed raw_ids so the load task can mark them processed
    xcom_payload = {
        "transformed_records": df_agg.to_dict(orient="records"),
        "raw_ids": raw_ids,
    }
    logger.info(
        "Transformation complete: %d aggregated records from %d raw rows.",
        len(df_agg),
        len(df_raw),
    )
    return json.dumps(xcom_payload, default=str)


def run_data_quality_checks_fn(**context) -> dict:
    """
    Validate: Retrieve transformed data from XCom and run all DQ checks.

    Strategy:
        - Critical check failures raise RuntimeError → task fails → DAG halts.
        - Non-critical failures are logged and captured in the DQ report XCom.

    XCom input : JSON payload from transform_data_fn.
    XCom output: DQ report dict.
    """
    ti = context["task_instance"]
    raw_payload: str = ti.xcom_pull(task_ids="transform_data")

    if not raw_payload or raw_payload == "[]":
        logger.warning("No transformed data received; skipping DQ checks.")
        return {"overall_passed": True, "checks": {}, "skipped": True}

    payload = json.loads(raw_payload)
    records = payload.get("transformed_records", [])

    if not records:
        logger.warning("Transformed records list is empty; skipping DQ checks.")
        return {"overall_passed": True, "checks": {}, "skipped": True}

    df = pd.DataFrame(records)
    logger.info("Running DQ checks on %d aggregated records.", len(df))

    # For aggregated data the column names differ; map them back for checks
    df_check = df.rename(
        columns={
            "avg_temperature": "temperature",
            "avg_humidity": "humidity",
            "avg_pressure": "pressure",
        }
    )

    # Quarantine rows before checking (soft quarantine logging)
    clean_df, quarantine_df = quarantine_bad_rows(df_check)
    if not quarantine_df.empty:
        logger.warning(
            "Quarantined %d records before DQ checks: %s",
            len(quarantine_df),
            quarantine_df.to_dict(orient="records"),
        )

    # Run checks on clean subset only (halt on critical failures)
    try:
        report = run_all_checks(clean_df, halt_on_critical=True)
    except RuntimeError as exc:
        logger.error("Critical DQ failure – halting pipeline: %s", exc)
        raise  # propagate to Airflow

    logger.info("DQ report: overall_passed=%s", report["overall_passed"])
    return report  # XCom


def load_processed_data_fn(**context) -> int:
    """
    Load: Insert/update daily aggregated sensor data into processed_sensor_data.

    Idempotency: uses ON CONFLICT (device_id, reading_date) DO UPDATE to safely
    re-run without creating duplicates.

    After loading, marks the source raw_sensor_data rows as processed.

    XCom input : JSON payload from transform_data_fn.
    XCom output: number of rows upserted.
    """
    ti = context["task_instance"]
    raw_payload: str = ti.xcom_pull(task_ids="transform_data")
    dq_report: dict = ti.xcom_pull(task_ids="run_data_quality_checks")

    if not raw_payload or raw_payload == "[]":
        logger.info("No data to load.")
        return 0

    if dq_report and not dq_report.get("overall_passed", True) and not dq_report.get("skipped"):
        logger.error("DQ report indicates failures. Aborting load.")
        raise RuntimeError("Data quality checks failed – load aborted.")

    payload = json.loads(raw_payload)
    records = payload.get("transformed_records", [])
    raw_ids = payload.get("raw_ids", [])

    if not records:
        logger.info("No aggregated records to load.")
        return 0

    df = pd.DataFrame(records)
    logger.info("Loading %d aggregated records into processed_sensor_data.", len(df))

    conn = _get_db_connection(DW_CONN_ID)
    upserted = 0
    try:
        with conn, conn.cursor() as cur:
            for _, row in df.iterrows():
                dq_flags = json.dumps({"quarantined": False})
                cur.execute(
                    """
                    INSERT INTO processed_sensor_data
                        (device_id, reading_date, avg_temperature, avg_humidity,
                         avg_pressure, reading_count, data_quality_flags)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, reading_date) DO UPDATE SET
                        avg_temperature   = EXCLUDED.avg_temperature,
                        avg_humidity      = EXCLUDED.avg_humidity,
                        avg_pressure      = EXCLUDED.avg_pressure,
                        reading_count     = EXCLUDED.reading_count,
                        data_quality_flags = EXCLUDED.data_quality_flags,
                        processed_at      = NOW()
                    """,
                    (
                        row["device_id"],
                        row["reading_date"],
                        round(float(row["avg_temperature"]), 4),
                        round(float(row["avg_humidity"]), 4),
                        round(float(row["avg_pressure"]), 4),
                        int(row.get("reading_count", 1)),
                        dq_flags,
                    ),
                )
                upserted += 1

            # Mark raw rows as processed
            if raw_ids:
                psycopg2.extras.execute_values(
                    cur,
                    "UPDATE raw_sensor_data SET processed_flag = TRUE WHERE id = %s",
                    [(rid,) for rid in raw_ids],
                )
                logger.info("Marked %d raw rows as processed.", len(raw_ids))

        logger.info("Successfully upserted %d records into processed_sensor_data.", upserted)
    finally:
        conn.close()

    return upserted


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="iot_data_pipeline",
    description=(
        "End-to-end IoT sensor data pipeline: generate → ingest → "
        "transform → quality-check → load."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["iot", "data-engineering", "etl"],
    doc_md=__doc__,
) as dag:

    t_generate = PythonOperator(
        task_id="generate_sensor_data",
        python_callable=generate_sensor_data_fn,
        on_failure_callback=_on_failure_callback,
        doc_md=(
            "Generates a fresh CSV of simulated IoT sensor readings for this "
            "DAG run and writes it to the shared data/ directory."
        ),
    )

    t_ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=ingest_raw_data_fn,
        on_failure_callback=_on_failure_callback,
        doc_md=(
            "Reads the generated CSV and UPSERTs rows into raw_sensor_data. "
            "ON CONFLICT DO NOTHING ensures idempotency."
        ),
    )

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_fn,
        on_failure_callback=_on_failure_callback,
        doc_md=(
            "Fetches unprocessed raw rows, applies cleaning / normalisation / "
            "enrichment, and aggregates to daily device averages."
        ),
    )

    t_quality = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks_fn,
        on_failure_callback=_on_failure_callback,
        doc_md=(
            "Runs all DQ checks on the transformed data. Critical failures "
            "halt the pipeline; non-critical issues are logged."
        ),
    )

    t_load = PythonOperator(
        task_id="load_processed_data",
        python_callable=load_processed_data_fn,
        on_failure_callback=_on_failure_callback,
        doc_md=(
            "UPSERTs validated, aggregated records into processed_sensor_data "
            "and marks source raw rows as processed."
        ),
    )

    # Task dependency chain
    t_generate >> t_ingest >> t_transform >> t_quality >> t_load
