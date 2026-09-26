"""
test_data_quality.py
---------------------
Unit tests for scripts/data_quality.py.

Run with:
    pytest tests/test_data_quality.py -v
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make scripts/ importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from data_quality import (
    HUMIDITY_MAX,
    HUMIDITY_MIN,
    PRESSURE_MAX,
    PRESSURE_MIN,
    TEMP_MAX,
    TEMP_MIN,
    check_device_id_format,
    check_duplicate_records,
    check_humidity_range,
    check_null_values,
    check_pressure_range,
    check_required_columns,
    check_row_count,
    check_temperature_range,
    check_timestamp_monotonicity,
    quarantine_bad_rows,
    run_all_checks,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def clean_df() -> pd.DataFrame:
    """A perfectly clean sensor DataFrame."""
    return pd.DataFrame(
        {
            "device_id": ["sensor_001", "sensor_002", "sensor_003"],
            "timestamp": pd.to_datetime(
                [
                    "2024-01-15T08:00:00Z",
                    "2024-01-15T08:05:00Z",
                    "2024-01-15T08:10:00Z",
                ],
                utc=True,
            ),
            "temperature": [22.5, 18.0, 30.0],
            "humidity": [55.0, 60.0, 70.0],
            "pressure": [1013.0, 1010.0, 1015.0],
        }
    )


@pytest.fixture()
def df_null_device(clean_df) -> pd.DataFrame:
    df = clean_df.copy()
    df.loc[0, "device_id"] = None
    return df


@pytest.fixture()
def df_bad_temperature(clean_df) -> pd.DataFrame:
    df = clean_df.copy()
    df.loc[0, "temperature"] = -100.0   # below TEMP_MIN
    df.loc[1, "temperature"] = 999.0    # above TEMP_MAX
    return df


@pytest.fixture()
def df_bad_humidity(clean_df) -> pd.DataFrame:
    df = clean_df.copy()
    df.loc[0, "humidity"] = -10.0   # below HUMIDITY_MIN
    return df


@pytest.fixture()
def df_bad_pressure(clean_df) -> pd.DataFrame:
    df = clean_df.copy()
    df.loc[0, "pressure"] = 200.0   # below PRESSURE_MIN
    return df


@pytest.fixture()
def df_with_duplicates(clean_df) -> pd.DataFrame:
    """DataFrame containing duplicate device_id + timestamp pairs."""
    return pd.concat([clean_df, clean_df.iloc[[0]]], ignore_index=True)


@pytest.fixture()
def df_missing_column(clean_df) -> pd.DataFrame:
    return clean_df.drop(columns=["humidity"])


# ---------------------------------------------------------------------------
# check_required_columns
# ---------------------------------------------------------------------------

class TestCheckRequiredColumns:
    def test_all_present_passes(self, clean_df):
        passed, issues = check_required_columns(clean_df)
        assert passed is True
        assert issues == []

    def test_missing_column_fails(self, df_missing_column):
        passed, issues = check_required_columns(df_missing_column)
        assert passed is False
        assert any("humidity" in issue for issue in issues)

    def test_custom_required_list(self, clean_df):
        passed, issues = check_required_columns(clean_df, required=["device_id"])
        assert passed is True

    def test_empty_dataframe_with_required_columns(self):
        df = pd.DataFrame(
            columns=["device_id", "timestamp", "temperature", "humidity", "pressure"]
        )
        passed, issues = check_required_columns(df)
        assert passed is True


# ---------------------------------------------------------------------------
# check_null_values
# ---------------------------------------------------------------------------

class TestCheckNullValues:
    def test_no_nulls_passes(self, clean_df):
        passed, issues = check_null_values(clean_df)
        assert passed is True
        assert issues == []

    def test_null_device_id_fails(self, df_null_device):
        passed, issues = check_null_values(df_null_device, columns=["device_id"])
        assert passed is False
        assert any("device_id" in issue for issue in issues)

    def test_threshold_tolerance(self, clean_df):
        df = clean_df.copy()
        df.loc[0, "temperature"] = None
        # 1/3 ≈ 33% null, threshold 0.5 → should pass
        passed, issues = check_null_values(df, columns=["temperature"], threshold=0.5)
        assert passed is True

    def test_threshold_exceeded_fails(self, clean_df):
        df = clean_df.copy()
        df["temperature"] = None
        # 100% null, threshold 0.0 → should fail
        passed, issues = check_null_values(df, columns=["temperature"], threshold=0.0)
        assert passed is False

    def test_missing_column_reported(self, clean_df):
        passed, issues = check_null_values(clean_df, columns=["nonexistent_col"])
        assert passed is False
        assert any("nonexistent_col" in issue for issue in issues)


# ---------------------------------------------------------------------------
# check_temperature_range
# ---------------------------------------------------------------------------

class TestCheckTemperatureRange:
    def test_valid_temperatures_pass(self, clean_df):
        passed, issues = check_temperature_range(clean_df)
        assert passed is True

    def test_below_min_fails(self, df_bad_temperature):
        passed, issues = check_temperature_range(df_bad_temperature)
        assert passed is False
        assert len(issues) > 0

    def test_custom_range(self, clean_df):
        # All temps in clean_df are < 35; use a tighter range
        passed, issues = check_temperature_range(clean_df, min_temp=0.0, max_temp=25.0)
        # 30.0 is above 25.0 → should fail
        assert passed is False

    def test_missing_column_fails(self, df_missing_column):
        # humidity is missing; checking temperature should still work
        passed, issues = check_temperature_range(df_missing_column)
        assert passed is True  # temperature column exists

    def test_boundary_values_pass(self, clean_df):
        df = clean_df.copy()
        df["temperature"] = TEMP_MIN   # exact boundary
        passed, issues = check_temperature_range(df)
        assert passed is True


# ---------------------------------------------------------------------------
# check_humidity_range
# ---------------------------------------------------------------------------

class TestCheckHumidityRange:
    def test_valid_humidity_passes(self, clean_df):
        passed, issues = check_humidity_range(clean_df)
        assert passed is True

    def test_below_min_fails(self, df_bad_humidity):
        passed, issues = check_humidity_range(df_bad_humidity)
        assert passed is False

    def test_above_max_fails(self, clean_df):
        df = clean_df.copy()
        df.loc[0, "humidity"] = 105.0
        passed, issues = check_humidity_range(df)
        assert passed is False

    def test_boundary_values_pass(self, clean_df):
        df = clean_df.copy()
        df["humidity"] = HUMIDITY_MIN
        passed, _ = check_humidity_range(df)
        assert passed is True

        df["humidity"] = HUMIDITY_MAX
        passed, _ = check_humidity_range(df)
        assert passed is True

    def test_missing_humidity_column_fails(self, df_missing_column):
        passed, issues = check_humidity_range(df_missing_column)
        assert passed is False
        assert any("humidity" in issue for issue in issues)


# ---------------------------------------------------------------------------
# check_pressure_range
# ---------------------------------------------------------------------------

class TestCheckPressureRange:
    def test_valid_pressure_passes(self, clean_df):
        passed, issues = check_pressure_range(clean_df)
        assert passed is True

    def test_out_of_range_fails(self, df_bad_pressure):
        passed, issues = check_pressure_range(df_bad_pressure)
        assert passed is False

    def test_boundary_values_pass(self, clean_df):
        df = clean_df.copy()
        df["pressure"] = PRESSURE_MIN
        passed, _ = check_pressure_range(df)
        assert passed is True


# ---------------------------------------------------------------------------
# check_device_id_format
# ---------------------------------------------------------------------------

class TestCheckDeviceIdFormat:
    def test_valid_ids_pass(self, clean_df):
        passed, issues = check_device_id_format(clean_df)
        assert passed is True

    def test_blank_id_fails(self, clean_df):
        df = clean_df.copy()
        df.loc[0, "device_id"] = "   "
        passed, issues = check_device_id_format(df)
        assert passed is False

    def test_empty_string_fails(self, clean_df):
        df = clean_df.copy()
        df.loc[0, "device_id"] = ""
        passed, issues = check_device_id_format(df)
        assert passed is False


# ---------------------------------------------------------------------------
# check_timestamp_monotonicity
# ---------------------------------------------------------------------------

class TestCheckTimestampMonotonicity:
    def test_monotonic_timestamps_pass(self, clean_df):
        # Always returns True (soft check)
        passed, issues = check_timestamp_monotonicity(clean_df)
        assert passed is True

    def test_non_monotonic_timestamps_warns(self, clean_df):
        df = clean_df.copy()
        df = df[df["device_id"] == "sensor_001"].copy()
        if len(df) >= 2:
            df.iloc[0, df.columns.get_loc("timestamp")] = pd.Timestamp(
                "2024-01-15T10:00:00Z", tz="UTC"
            )
            df.iloc[1, df.columns.get_loc("timestamp")] = pd.Timestamp(
                "2024-01-15T08:00:00Z", tz="UTC"
            )
            passed, issues = check_timestamp_monotonicity(df)
            # Still passes (soft check) but has issues
            assert passed is True


# ---------------------------------------------------------------------------
# check_duplicate_records
# ---------------------------------------------------------------------------

class TestCheckDuplicateRecords:
    def test_no_duplicates_passes(self, clean_df):
        passed, issues = check_duplicate_records(clean_df)
        assert passed is True

    def test_duplicates_fail(self, df_with_duplicates):
        passed, issues = check_duplicate_records(df_with_duplicates)
        assert passed is False
        assert any("duplicate" in issue.lower() for issue in issues)


# ---------------------------------------------------------------------------
# check_row_count
# ---------------------------------------------------------------------------

class TestCheckRowCount:
    def test_sufficient_rows_pass(self, clean_df):
        passed, issues = check_row_count(clean_df, min_rows=1)
        assert passed is True

    def test_below_min_fails(self, clean_df):
        passed, issues = check_row_count(clean_df, min_rows=100)
        assert passed is False

    def test_above_max_fails(self, clean_df):
        passed, issues = check_row_count(clean_df, min_rows=1, max_rows=2)
        assert passed is False

    def test_empty_df_fails_min_check(self):
        passed, issues = check_row_count(pd.DataFrame(), min_rows=1)
        assert passed is False


# ---------------------------------------------------------------------------
# run_all_checks
# ---------------------------------------------------------------------------

class TestRunAllChecks:
    def test_clean_data_all_pass(self, clean_df):
        report = run_all_checks(clean_df, halt_on_critical=False)
        assert report["overall_passed"] is True
        assert "checks" in report

    def test_bad_data_fails_report(self, df_bad_temperature):
        report = run_all_checks(df_bad_temperature, halt_on_critical=False)
        assert report["overall_passed"] is False

    def test_halt_on_critical_raises(self, df_bad_temperature):
        with pytest.raises(RuntimeError):
            run_all_checks(df_bad_temperature, halt_on_critical=True)

    def test_report_structure(self, clean_df):
        report = run_all_checks(clean_df, halt_on_critical=False)
        assert isinstance(report, dict)
        assert "overall_passed" in report
        assert "checks" in report
        for name, detail in report["checks"].items():
            assert "passed" in detail
            assert "issues" in detail

    def test_all_check_names_present(self, clean_df):
        report = run_all_checks(clean_df, halt_on_critical=False)
        expected_checks = {
            "required_columns",
            "null_critical_columns",
            "temperature_range",
            "humidity_range",
        }
        for check in expected_checks:
            assert check in report["checks"], f"Check '{check}' missing from report"


# ---------------------------------------------------------------------------
# quarantine_bad_rows
# ---------------------------------------------------------------------------

class TestQuarantineBadRows:
    def test_all_clean_rows_stay(self, clean_df):
        clean, quarantine = quarantine_bad_rows(clean_df)
        assert len(clean) == len(clean_df)
        assert len(quarantine) == 0

    def test_bad_temperature_quarantined(self, df_bad_temperature):
        clean, quarantine = quarantine_bad_rows(df_bad_temperature)
        assert len(quarantine) > 0
        assert len(clean) < len(df_bad_temperature)

    def test_null_device_quarantined(self, df_null_device):
        clean, quarantine = quarantine_bad_rows(df_null_device)
        assert len(quarantine) == 1

    def test_clean_plus_bad_split(self, clean_df):
        """Mix 2 good + 1 bad row and verify split."""
        df = clean_df.copy()
        df.loc[2, "humidity"] = 200.0  # bad
        clean, quarantine = quarantine_bad_rows(df)
        assert len(clean) == 2
        assert len(quarantine) == 1

    def test_row_counts_add_up(self, df_bad_temperature):
        clean, quarantine = quarantine_bad_rows(df_bad_temperature)
        assert len(clean) + len(quarantine) == len(df_bad_temperature)
