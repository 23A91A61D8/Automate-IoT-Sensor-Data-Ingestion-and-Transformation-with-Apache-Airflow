"""
data_quality.py
---------------
Reusable data-quality check functions for IoT sensor DataFrames.

Each function:
    • Accepts a pandas DataFrame (and optional parameters).
    • Returns a tuple: (passed: bool, issues: list[str])
      - *passed* is True when the check succeeds.
      - *issues* is a list of human-readable problem descriptions (empty when passed).

A composite runner `run_all_checks` applies every check and aggregates the
results into a structured report that can be pushed to Airflow XComs.
"""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants – thresholds
# ---------------------------------------------------------------------------
TEMP_MIN = -50.0
TEMP_MAX = 150.0
HUMIDITY_MIN = 0.0
HUMIDITY_MAX = 100.0
PRESSURE_MIN = 870.0
PRESSURE_MAX = 1085.0

CRITICAL_COLUMNS = ["device_id", "timestamp"]
NUMERIC_COLUMNS = ["temperature", "humidity", "pressure"]


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------
CheckResult = tuple[bool, list[str]]


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def check_required_columns(df: pd.DataFrame, required: list[str] | None = None) -> CheckResult:
    """
    Verify that all expected columns are present in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
    required : list[str], optional
        Column names that must exist. Defaults to all expected sensor columns.

    Returns
    -------
    CheckResult
    """
    if required is None:
        required = ["device_id", "timestamp", "temperature", "humidity", "pressure"]

    missing = [c for c in required if c not in df.columns]
    if missing:
        msg = f"Missing required columns: {missing}"
        logger.error(msg)
        return False, [msg]

    logger.debug("check_required_columns: PASSED")
    return True, []


def check_null_values(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    threshold: float = 0.0,
) -> CheckResult:
    """
    Ensure that the fraction of null values in each column does not exceed
    *threshold*.

    Parameters
    ----------
    df : pd.DataFrame
    columns : list[str], optional
        Columns to inspect. Defaults to CRITICAL_COLUMNS.
    threshold : float
        Maximum allowed null fraction [0, 1]. Default 0.0 (no nulls allowed).

    Returns
    -------
    CheckResult
    """
    if columns is None:
        columns = CRITICAL_COLUMNS

    issues: list[str] = []
    for col in columns:
        if col not in df.columns:
            issues.append(f"Column '{col}' not present in DataFrame.")
            continue
        null_frac = df[col].isna().mean()
        if null_frac > threshold:
            msg = (
                f"Column '{col}' has {null_frac:.2%} null values "
                f"(threshold: {threshold:.2%})."
            )
            logger.warning(msg)
            issues.append(msg)

    passed = len(issues) == 0
    if passed:
        logger.debug("check_null_values: PASSED")
    return passed, issues


def check_temperature_range(
    df: pd.DataFrame,
    min_temp: float = TEMP_MIN,
    max_temp: float = TEMP_MAX,
) -> CheckResult:
    """
    Validate that all temperature readings fall within [min_temp, max_temp].

    Parameters
    ----------
    df : pd.DataFrame
    min_temp : float
        Lower bound in °C.
    max_temp : float
        Upper bound in °C.

    Returns
    -------
    CheckResult
    """
    if "temperature" not in df.columns:
        msg = "Column 'temperature' is absent – cannot perform range check."
        logger.error(msg)
        return False, [msg]

    out_of_range = df[
        (df["temperature"] < min_temp) | (df["temperature"] > max_temp)
    ]
    if not out_of_range.empty:
        msg = (
            f"{len(out_of_range)} temperature reading(s) outside "
            f"[{min_temp}, {max_temp}] °C. "
            f"Examples: {out_of_range['temperature'].head(3).tolist()}"
        )
        logger.warning(msg)
        return False, [msg]

    logger.debug("check_temperature_range: PASSED")
    return True, []


def check_humidity_range(
    df: pd.DataFrame,
    min_humidity: float = HUMIDITY_MIN,
    max_humidity: float = HUMIDITY_MAX,
) -> CheckResult:
    """
    Validate that all humidity readings fall within [min_humidity, max_humidity].

    Parameters
    ----------
    df : pd.DataFrame
    min_humidity : float
        Lower bound (%).
    max_humidity : float
        Upper bound (%).

    Returns
    -------
    CheckResult
    """
    if "humidity" not in df.columns:
        msg = "Column 'humidity' is absent – cannot perform range check."
        logger.error(msg)
        return False, [msg]

    out_of_range = df[
        (df["humidity"] < min_humidity) | (df["humidity"] > max_humidity)
    ]
    if not out_of_range.empty:
        msg = (
            f"{len(out_of_range)} humidity reading(s) outside "
            f"[{min_humidity}, {max_humidity}]%. "
            f"Examples: {out_of_range['humidity'].head(3).tolist()}"
        )
        logger.warning(msg)
        return False, [msg]

    logger.debug("check_humidity_range: PASSED")
    return True, []


def check_pressure_range(
    df: pd.DataFrame,
    min_pressure: float = PRESSURE_MIN,
    max_pressure: float = PRESSURE_MAX,
) -> CheckResult:
    """
    Validate that all pressure readings fall within [min_pressure, max_pressure].

    Parameters
    ----------
    df : pd.DataFrame
    min_pressure : float
        Lower bound (hPa).
    max_pressure : float
        Upper bound (hPa).

    Returns
    -------
    CheckResult
    """
    if "pressure" not in df.columns:
        msg = "Column 'pressure' is absent – cannot perform range check."
        logger.error(msg)
        return False, [msg]

    out_of_range = df[
        (df["pressure"] < min_pressure) | (df["pressure"] > max_pressure)
    ]
    if not out_of_range.empty:
        msg = (
            f"{len(out_of_range)} pressure reading(s) outside "
            f"[{min_pressure}, {max_pressure}] hPa. "
            f"Examples: {out_of_range['pressure'].head(3).tolist()}"
        )
        logger.warning(msg)
        return False, [msg]

    logger.debug("check_pressure_range: PASSED")
    return True, []


def check_device_id_format(df: pd.DataFrame) -> CheckResult:
    """
    Ensure all device_id values are non-empty strings (no blank / whitespace only).

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    CheckResult
    """
    if "device_id" not in df.columns:
        msg = "Column 'device_id' is absent."
        logger.error(msg)
        return False, [msg]

    bad = df[df["device_id"].astype(str).str.strip() == ""]
    if not bad.empty:
        msg = f"{len(bad)} rows have empty/blank device_id values."
        logger.warning(msg)
        return False, [msg]

    logger.debug("check_device_id_format: PASSED")
    return True, []


def check_timestamp_monotonicity(df: pd.DataFrame) -> CheckResult:
    """
    Warn (soft check) when timestamps within a single device are not
    monotonically non-decreasing. This does not fail the pipeline but
    flags potential clock-skew issues.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    CheckResult
        Always returns *passed=True*; issues are warnings.
    """
    if "timestamp" not in df.columns or "device_id" not in df.columns:
        return True, []

    issues: list[str] = []
    for device, grp in df.groupby("device_id"):
        ts = pd.to_datetime(grp["timestamp"], utc=True, errors="coerce").dropna()
        if (ts.diff().dropna() < pd.Timedelta(0)).any():
            msg = f"Device '{device}' has non-monotonic timestamps."
            logger.warning(msg)
            issues.append(msg)

    # Monotonicity failures are warnings, not hard failures
    return True, issues


def check_duplicate_records(df: pd.DataFrame) -> CheckResult:
    """
    Detect exact duplicate rows (same device_id + timestamp).

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    CheckResult
    """
    if "device_id" not in df.columns or "timestamp" not in df.columns:
        return True, []

    dup_count = df.duplicated(subset=["device_id", "timestamp"]).sum()
    if dup_count > 0:
        msg = f"Found {dup_count} duplicate (device_id, timestamp) pairs."
        logger.warning(msg)
        return False, [msg]

    logger.debug("check_duplicate_records: PASSED")
    return True, []


def check_row_count(
    df: pd.DataFrame,
    min_rows: int = 1,
    max_rows: int | None = None,
) -> CheckResult:
    """
    Ensure the DataFrame has at least *min_rows* records (and optionally at
    most *max_rows*).

    Parameters
    ----------
    df : pd.DataFrame
    min_rows : int
    max_rows : int, optional

    Returns
    -------
    CheckResult
    """
    issues: list[str] = []
    n = len(df)
    if n < min_rows:
        issues.append(f"Row count {n} is below minimum {min_rows}.")
    if max_rows is not None and n > max_rows:
        issues.append(f"Row count {n} exceeds maximum {max_rows}.")

    if issues:
        for msg in issues:
            logger.warning(msg)
        return False, issues

    logger.debug("check_row_count: PASSED (%d rows).", n)
    return True, []


# ---------------------------------------------------------------------------
# Composite runner
# ---------------------------------------------------------------------------

def run_all_checks(
    df: pd.DataFrame,
    halt_on_critical: bool = True,
) -> dict[str, Any]:
    """
    Execute every data-quality check and compile a structured report.

    The report contains:
        - overall_passed : bool
        - checks : dict mapping check name → {passed, issues}

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to validate (post-transformation).
    halt_on_critical : bool
        When True, raises a ``RuntimeError`` if any *critical* check fails.
        Critical checks: required_columns, null_values (critical cols only),
        temperature_range, humidity_range.

    Returns
    -------
    dict
        Structured quality report.

    Raises
    ------
    RuntimeError
        If *halt_on_critical* is True and a critical check fails.
    """
    logger.info("Running all data quality checks on DataFrame with %d rows.", len(df))

    checks: dict[str, dict] = {}

    # ----- helper -----------------------------------------------------------
    def _run(name: str, fn, *args, critical: bool = False, **kwargs) -> bool:
        passed, issues = fn(*args, **kwargs)
        checks[name] = {"passed": passed, "issues": issues, "critical": critical}
        if not passed:
            level = logger.error if critical else logger.warning
            level("DQ check '%s' FAILED: %s", name, issues)
            if critical and halt_on_critical:
                raise RuntimeError(
                    f"Critical data quality check failed: '{name}'. Issues: {issues}"
                )
        else:
            logger.info("DQ check '%s' PASSED.", name)
        return passed
    # ------------------------------------------------------------------------

    _run("required_columns", check_required_columns, df, critical=True)
    _run("null_critical_columns", check_null_values, df, CRITICAL_COLUMNS, critical=True)
    _run("null_numeric_columns", check_null_values, df, NUMERIC_COLUMNS, threshold=0.1)
    _run("temperature_range", check_temperature_range, df, critical=True)
    _run("humidity_range", check_humidity_range, df, critical=True)
    _run("pressure_range", check_pressure_range, df)
    _run("device_id_format", check_device_id_format, df)
    _run("timestamp_monotonicity", check_timestamp_monotonicity, df)
    _run("duplicate_records", check_duplicate_records, df)
    _run("row_count", check_row_count, df, min_rows=1)

    overall_passed = all(v["passed"] for v in checks.values())
    report = {"overall_passed": overall_passed, "checks": checks}

    logger.info(
        "DQ report complete. Overall: %s | Checks: %d passed, %d failed.",
        "PASSED" if overall_passed else "FAILED",
        sum(1 for v in checks.values() if v["passed"]),
        sum(1 for v in checks.values() if not v["passed"]),
    )
    return report


def quarantine_bad_rows(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separate a DataFrame into clean and quarantined subsets.

    A row is quarantined when:
        - device_id or timestamp is null.
        - temperature is outside [TEMP_MIN, TEMP_MAX].
        - humidity is outside [HUMIDITY_MIN, HUMIDITY_MAX].
        - pressure is outside [PRESSURE_MIN, PRESSURE_MAX].

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (clean_df, quarantine_df)
    """
    mask_bad = (
        df["device_id"].isna()
        | df["timestamp"].isna()
        | df["temperature"].isna()
        | df["humidity"].isna()
        | df["pressure"].isna()
        | (df["temperature"] < TEMP_MIN)
        | (df["temperature"] > TEMP_MAX)
        | (df["humidity"] < HUMIDITY_MIN)
        | (df["humidity"] > HUMIDITY_MAX)
        | (df["pressure"] < PRESSURE_MIN)
        | (df["pressure"] > PRESSURE_MAX)
    )
    quarantine_df = df[mask_bad].copy()
    clean_df = df[~mask_bad].copy()

    logger.info(
        "Quarantine split: %d clean rows, %d quarantined rows.",
        len(clean_df),
        len(quarantine_df),
    )
    return clean_df, quarantine_df
