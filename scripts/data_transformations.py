"""
data_transformations.py
-----------------------
Pure-Python transformation functions applied to IoT sensor DataFrames.
All functions are stateless, side-effect-free, and independently testable.

Transformations applied in this module:
    1. Type coercion  – ensure correct dtypes for all columns.
    2. Missing-value imputation – fill nulls with device-level medians.
    3. Unit conversion – temperature Celsius → Fahrenheit.
    4. Normalisation  – min-max scale numeric sensors to [0, 1].
    5. Enrichment     – add reading_date, reading_hour, and temp_category
       columns for downstream aggregation.
    6. Daily aggregation – collapse per-reading rows into daily device
       averages for the processed_sensor_data table.
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TEMP_MIN_C = -50.0
TEMP_MAX_C = 150.0
HUMIDITY_MIN = 0.0
HUMIDITY_MAX = 100.0
PRESSURE_MIN = 870.0
PRESSURE_MAX = 1085.0

REQUIRED_COLUMNS = {"device_id", "timestamp", "temperature", "humidity", "pressure"}


# ---------------------------------------------------------------------------
# 1. Type coercion
# ---------------------------------------------------------------------------
def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure that every column in the raw sensor DataFrame has the expected
    Python / Pandas dtype.

    Parameters
    ----------
    df : pd.DataFrame
        Raw sensor DataFrame (straight from CSV / database).

    Returns
    -------
    pd.DataFrame
        A copy of *df* with corrected dtypes.

    Raises
    ------
    ValueError
        If any required column is missing from *df*.
    """
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()
    df["device_id"] = df["device_id"].astype(str).str.strip()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")
    df["pressure"] = pd.to_numeric(df["pressure"], errors="coerce")

    invalid_ts = df["timestamp"].isna().sum()
    if invalid_ts:
        logger.warning("Dropped %d rows with un-parseable timestamps.", invalid_ts)
        df = df.dropna(subset=["timestamp"])

    logger.info("dtype coercion complete. Rows remaining: %d", len(df))
    return df


# ---------------------------------------------------------------------------
# 2. Missing-value imputation
# ---------------------------------------------------------------------------
def impute_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing numeric sensor values with the device-level median; fall back
    to the global column median when a device has no non-null observations.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame after dtype coercion.

    Returns
    -------
    pd.DataFrame
        DataFrame with null numeric values filled.
    """
    df = df.copy()
    numeric_cols = ["temperature", "humidity", "pressure"]

    for col in numeric_cols:
        null_count_before = df[col].isna().sum()
        if null_count_before == 0:
            continue

        # Device-level median
        device_medians = df.groupby("device_id")[col].transform("median")
        # Global median as fallback
        global_median = df[col].median()

        df[col] = df[col].fillna(device_medians).fillna(global_median)

        null_count_after = df[col].isna().sum()
        logger.info(
            "Imputed '%s': %d nulls → %d remaining.",
            col,
            null_count_before,
            null_count_after,
        )

    return df


# ---------------------------------------------------------------------------
# 3. Out-of-range clipping (post-imputation safety net)
# ---------------------------------------------------------------------------
def clip_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clip sensor values to physically plausible ranges.

    This is applied *after* imputation so that imputed values derived from
    anomalous peers are also corrected.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    df["temperature"] = df["temperature"].clip(TEMP_MIN_C, TEMP_MAX_C)
    df["humidity"] = df["humidity"].clip(HUMIDITY_MIN, HUMIDITY_MAX)
    df["pressure"] = df["pressure"].clip(PRESSURE_MIN, PRESSURE_MAX)
    logger.info("Outlier clipping applied.")
    return df


# ---------------------------------------------------------------------------
# 4. Unit conversion – Celsius → Fahrenheit
# ---------------------------------------------------------------------------
def celsius_to_fahrenheit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a *temperature_f* column (Fahrenheit) computed from *temperature* (°C).

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    df["temperature_f"] = (df["temperature"] * 9 / 5 + 32).round(4)
    logger.info("Added 'temperature_f' column (Fahrenheit conversion).")
    return df


# ---------------------------------------------------------------------------
# 5. Min-max normalisation
# ---------------------------------------------------------------------------
def normalize_sensor_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add min-max normalised variants of the three sensor columns.

    New columns: temperature_norm, humidity_norm, pressure_norm.
    Each value is scaled to [0, 1] using the physically plausible min/max
    constants so that the scaling is stable across batches.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()

    ranges = {
        "temperature": (TEMP_MIN_C, TEMP_MAX_C),
        "humidity": (HUMIDITY_MIN, HUMIDITY_MAX),
        "pressure": (PRESSURE_MIN, PRESSURE_MAX),
    }
    for col, (lo, hi) in ranges.items():
        df[f"{col}_norm"] = ((df[col] - lo) / (hi - lo)).round(6)

    logger.info("Min-max normalisation applied to temperature, humidity, pressure.")
    return df


# ---------------------------------------------------------------------------
# 6. Enrichment – reading_date, reading_hour, temp_category
# ---------------------------------------------------------------------------
def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns that improve downstream analytical queries.

    New columns:
        reading_date  : date portion of the timestamp.
        reading_hour  : hour-of-day (0–23).
        temp_category : categorical label ('cold', 'comfortable', 'hot').

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    df["reading_date"] = df["timestamp"].dt.date
    df["reading_hour"] = df["timestamp"].dt.hour

    # Temperature category (based on original Celsius value)
    conditions = [
        df["temperature"] < 10,
        (df["temperature"] >= 10) & (df["temperature"] <= 28),
        df["temperature"] > 28,
    ]
    choices = ["cold", "comfortable", "hot"]
    df["temp_category"] = np.select(conditions, choices, default="unknown")

    logger.info("Enrichment columns added: reading_date, reading_hour, temp_category.")
    return df


# ---------------------------------------------------------------------------
# 7. Daily aggregation
# ---------------------------------------------------------------------------
def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-device daily averages from the cleaned, enriched DataFrame.

    This produces rows suitable for the *processed_sensor_data* table.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned and enriched reading-level DataFrame.

    Returns
    -------
    pd.DataFrame
        Aggregated DataFrame with columns:
            device_id, reading_date, avg_temperature, avg_humidity,
            avg_pressure, reading_count.
    """
    agg = (
        df.groupby(["device_id", "reading_date"], as_index=False)
        .agg(
            avg_temperature=("temperature", "mean"),
            avg_humidity=("humidity", "mean"),
            avg_pressure=("pressure", "mean"),
            reading_count=("device_id", "count"),
        )
    )
    agg["avg_temperature"] = agg["avg_temperature"].round(4)
    agg["avg_humidity"] = agg["avg_humidity"].round(4)
    agg["avg_pressure"] = agg["avg_pressure"].round(4)
    logger.info(
        "Daily aggregation produced %d device-date rows.", len(agg)
    )
    return agg


# ---------------------------------------------------------------------------
# Convenience pipeline – apply all transformations in order
# ---------------------------------------------------------------------------
def apply_all_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full transformation pipeline on a raw sensor DataFrame.

    Steps (in order):
        1. coerce_dtypes
        2. impute_missing_values
        3. clip_outliers
        4. celsius_to_fahrenheit
        5. normalize_sensor_values
        6. enrich_data

    Parameters
    ----------
    df : pd.DataFrame
        Raw sensor DataFrame.

    Returns
    -------
    pd.DataFrame
        Fully transformed reading-level DataFrame.
    """
    logger.info("Starting full transformation pipeline. Input rows: %d", len(df))
    df = coerce_dtypes(df)
    df = impute_missing_values(df)
    df = clip_outliers(df)
    df = celsius_to_fahrenheit(df)
    df = normalize_sensor_values(df)
    df = enrich_data(df)
    logger.info("Transformation pipeline complete. Output rows: %d", len(df))
    return df
