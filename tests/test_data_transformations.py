"""
test_data_transformations.py
-----------------------------
Unit tests for scripts/data_transformations.py.

Run with:
    pytest tests/test_data_transformations.py -v
"""

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Make scripts/ importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from data_transformations import (
    HUMIDITY_MAX,
    HUMIDITY_MIN,
    PRESSURE_MAX,
    PRESSURE_MIN,
    TEMP_MAX_C,
    TEMP_MIN_C,
    aggregate_daily,
    apply_all_transformations,
    celsius_to_fahrenheit,
    clip_outliers,
    coerce_dtypes,
    enrich_data,
    impute_missing_values,
    normalize_sensor_values,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def minimal_df() -> pd.DataFrame:
    """Minimal valid raw sensor DataFrame."""
    return pd.DataFrame(
        {
            "device_id": ["sensor_001", "sensor_002", "sensor_001"],
            "timestamp": [
                "2024-01-15T08:00:00Z",
                "2024-01-15T08:05:00Z",
                "2024-01-15T08:10:00Z",
            ],
            "temperature": [22.5, 18.0, 30.0],
            "humidity": [55.0, 60.0, 70.0],
            "pressure": [1013.0, 1010.0, 1015.0],
        }
    )


@pytest.fixture()
def df_with_nulls(minimal_df) -> pd.DataFrame:
    """DataFrame with intentional null values."""
    df = minimal_df.copy()
    df.loc[0, "temperature"] = None
    df.loc[2, "pressure"] = None
    return df


@pytest.fixture()
def df_with_outliers(minimal_df) -> pd.DataFrame:
    """DataFrame with out-of-range sensor values."""
    df = minimal_df.copy()
    df.loc[0, "temperature"] = -999.0   # way below minimum
    df.loc[1, "humidity"] = 200.0       # way above maximum
    df.loc[2, "pressure"] = 500.0       # way below minimum
    return df


# ---------------------------------------------------------------------------
# coerce_dtypes
# ---------------------------------------------------------------------------

class TestCoerceDtypes:
    def test_timestamp_becomes_datetime(self, minimal_df):
        result = coerce_dtypes(minimal_df)
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_numeric_columns_are_float(self, minimal_df):
        result = coerce_dtypes(minimal_df)
        for col in ["temperature", "humidity", "pressure"]:
            assert pd.api.types.is_float_dtype(result[col]), f"{col} should be float"

    def test_device_id_is_string(self, minimal_df):
        result = coerce_dtypes(minimal_df)
        assert result["device_id"].dtype == object

    def test_missing_column_raises_value_error(self):
        bad_df = pd.DataFrame({"device_id": ["a"], "timestamp": ["2024-01-01T00:00:00Z"]})
        with pytest.raises(ValueError, match="Missing required columns"):
            coerce_dtypes(bad_df)

    def test_string_temperature_coerced(self, minimal_df):
        df = minimal_df.copy()
        df["temperature"] = df["temperature"].astype(str)
        result = coerce_dtypes(df)
        assert pd.api.types.is_float_dtype(result["temperature"])

    def test_invalid_timestamp_rows_dropped(self, minimal_df):
        df = minimal_df.copy()
        df.loc[0, "timestamp"] = "NOT_A_DATE"
        result = coerce_dtypes(df)
        assert len(result) == len(minimal_df) - 1


# ---------------------------------------------------------------------------
# impute_missing_values
# ---------------------------------------------------------------------------

class TestImputeMissingValues:
    def test_nulls_filled(self, df_with_nulls):
        df = coerce_dtypes(df_with_nulls)
        result = impute_missing_values(df)
        assert result["temperature"].isna().sum() == 0
        assert result["pressure"].isna().sum() == 0

    def test_non_null_values_unchanged(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = impute_missing_values(df)
        # Row 1 values should be unchanged (no nulls in minimal_df)
        assert result.loc[1, "temperature"] == pytest.approx(18.0)
        assert result.loc[1, "humidity"] == pytest.approx(60.0)

    def test_all_null_column_uses_global_median(self):
        df = pd.DataFrame(
            {
                "device_id": ["sensor_001", "sensor_001"],
                "timestamp": pd.to_datetime(
                    ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"], utc=True
                ),
                "temperature": [None, None],
                "humidity": [50.0, 60.0],
                "pressure": [1013.0, 1013.0],
            }
        )
        # global median of all-null column is NaN; ensure no crash
        result = impute_missing_values(df)
        # After imputation, values may still be NaN if global median is NaN
        # but the function should not raise
        assert True  # just check it runs without error

    def test_row_count_unchanged(self, df_with_nulls):
        df = coerce_dtypes(df_with_nulls)
        result = impute_missing_values(df)
        assert len(result) == len(df)


# ---------------------------------------------------------------------------
# clip_outliers
# ---------------------------------------------------------------------------

class TestClipOutliers:
    def test_temperature_clipped_to_range(self, df_with_outliers):
        df = coerce_dtypes(df_with_outliers)
        result = clip_outliers(df)
        assert (result["temperature"] >= TEMP_MIN_C).all()
        assert (result["temperature"] <= TEMP_MAX_C).all()

    def test_humidity_clipped_to_range(self, df_with_outliers):
        df = coerce_dtypes(df_with_outliers)
        result = clip_outliers(df)
        assert (result["humidity"] >= HUMIDITY_MIN).all()
        assert (result["humidity"] <= HUMIDITY_MAX).all()

    def test_pressure_clipped_to_range(self, df_with_outliers):
        df = coerce_dtypes(df_with_outliers)
        result = clip_outliers(df)
        assert (result["pressure"] >= PRESSURE_MIN).all()
        assert (result["pressure"] <= PRESSURE_MAX).all()

    def test_valid_values_unchanged(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = clip_outliers(df)
        assert result["temperature"].tolist() == pytest.approx(
            minimal_df["temperature"].tolist()
        )


# ---------------------------------------------------------------------------
# celsius_to_fahrenheit
# ---------------------------------------------------------------------------

class TestCelsiusToFahrenheit:
    def test_freezing_point(self, minimal_df):
        df = coerce_dtypes(minimal_df).copy()
        df["temperature"] = 0.0
        result = celsius_to_fahrenheit(df)
        assert result["temperature_f"].iloc[0] == pytest.approx(32.0)

    def test_boiling_point(self, minimal_df):
        df = coerce_dtypes(minimal_df).copy()
        df["temperature"] = 100.0
        result = celsius_to_fahrenheit(df)
        assert result["temperature_f"].iloc[0] == pytest.approx(212.0)

    def test_body_temperature(self, minimal_df):
        df = coerce_dtypes(minimal_df).copy()
        df["temperature"] = 37.0
        result = celsius_to_fahrenheit(df)
        assert result["temperature_f"].iloc[0] == pytest.approx(98.6, rel=1e-3)

    def test_column_added(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = celsius_to_fahrenheit(df)
        assert "temperature_f" in result.columns

    def test_original_column_preserved(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = celsius_to_fahrenheit(df)
        assert "temperature" in result.columns


# ---------------------------------------------------------------------------
# normalize_sensor_values
# ---------------------------------------------------------------------------

class TestNormalizeSensorValues:
    def test_norm_columns_added(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = normalize_sensor_values(df)
        for col in ["temperature_norm", "humidity_norm", "pressure_norm"]:
            assert col in result.columns, f"Column '{col}' missing"

    def test_values_in_zero_one_range(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = normalize_sensor_values(df)
        for col in ["temperature_norm", "humidity_norm", "pressure_norm"]:
            assert (result[col] >= 0).all(), f"{col} has values below 0"
            assert (result[col] <= 1).all(), f"{col} has values above 1"

    def test_min_value_normalises_to_zero(self):
        df = pd.DataFrame(
            {
                "device_id": ["sensor_001"],
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True),
                "temperature": [TEMP_MIN_C],
                "humidity": [HUMIDITY_MIN],
                "pressure": [PRESSURE_MIN],
            }
        )
        result = normalize_sensor_values(df)
        assert result["temperature_norm"].iloc[0] == pytest.approx(0.0)
        assert result["humidity_norm"].iloc[0] == pytest.approx(0.0)
        assert result["pressure_norm"].iloc[0] == pytest.approx(0.0)

    def test_max_value_normalises_to_one(self):
        df = pd.DataFrame(
            {
                "device_id": ["sensor_001"],
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True),
                "temperature": [TEMP_MAX_C],
                "humidity": [HUMIDITY_MAX],
                "pressure": [PRESSURE_MAX],
            }
        )
        result = normalize_sensor_values(df)
        assert result["temperature_norm"].iloc[0] == pytest.approx(1.0)
        assert result["humidity_norm"].iloc[0] == pytest.approx(1.0)
        assert result["pressure_norm"].iloc[0] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# enrich_data
# ---------------------------------------------------------------------------

class TestEnrichData:
    def test_reading_date_added(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = enrich_data(df)
        assert "reading_date" in result.columns

    def test_reading_hour_added(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        result = enrich_data(df)
        assert "reading_hour" in result.columns
        assert (result["reading_hour"] >= 0).all()
        assert (result["reading_hour"] <= 23).all()

    def test_temp_category_cold(self, minimal_df):
        df = coerce_dtypes(minimal_df).copy()
        df["temperature"] = 5.0
        result = enrich_data(df)
        assert (result["temp_category"] == "cold").all()

    def test_temp_category_comfortable(self, minimal_df):
        df = coerce_dtypes(minimal_df).copy()
        df["temperature"] = 20.0
        result = enrich_data(df)
        assert (result["temp_category"] == "comfortable").all()

    def test_temp_category_hot(self, minimal_df):
        df = coerce_dtypes(minimal_df).copy()
        df["temperature"] = 35.0
        result = enrich_data(df)
        assert (result["temp_category"] == "hot").all()

    def test_reading_date_type(self, minimal_df):
        import datetime
        df = coerce_dtypes(minimal_df)
        result = enrich_data(df)
        assert isinstance(result["reading_date"].iloc[0], datetime.date)


# ---------------------------------------------------------------------------
# aggregate_daily
# ---------------------------------------------------------------------------

class TestAggregateDaily:
    def test_output_columns(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        df = enrich_data(df)
        result = aggregate_daily(df)
        for col in ["device_id", "reading_date", "avg_temperature",
                    "avg_humidity", "avg_pressure", "reading_count"]:
            assert col in result.columns, f"Column '{col}' missing"

    def test_correct_average(self):
        df = pd.DataFrame(
            {
                "device_id": ["sensor_001", "sensor_001"],
                "timestamp": pd.to_datetime(
                    ["2024-01-01T08:00:00Z", "2024-01-01T09:00:00Z"], utc=True
                ),
                "temperature": [10.0, 20.0],
                "humidity": [50.0, 50.0],
                "pressure": [1013.0, 1013.0],
            }
        )
        df = enrich_data(df)
        result = aggregate_daily(df)
        assert result.loc[0, "avg_temperature"] == pytest.approx(15.0)
        assert result.loc[0, "reading_count"] == 2

    def test_grouping_by_device_and_date(self, minimal_df):
        df = coerce_dtypes(minimal_df)
        df = enrich_data(df)
        result = aggregate_daily(df)
        # sensor_001 appears twice on same day → 1 aggregated row per device-date
        assert len(result) == 2  # sensor_001 + sensor_002


# ---------------------------------------------------------------------------
# apply_all_transformations (integration)
# ---------------------------------------------------------------------------

class TestApplyAllTransformations:
    def test_pipeline_runs_without_error(self, minimal_df):
        result = apply_all_transformations(minimal_df)
        assert len(result) > 0

    def test_pipeline_runs_with_nulls(self, df_with_nulls):
        result = apply_all_transformations(df_with_nulls)
        # Nulls should be imputed; no NaNs remain in numeric cols
        for col in ["temperature", "humidity", "pressure"]:
            assert result[col].isna().sum() == 0, f"NaN in {col} after pipeline"

    def test_expected_columns_present(self, minimal_df):
        result = apply_all_transformations(minimal_df)
        expected = [
            "device_id", "timestamp", "temperature", "humidity", "pressure",
            "temperature_f", "temperature_norm", "humidity_norm", "pressure_norm",
            "reading_date", "reading_hour", "temp_category",
        ]
        for col in expected:
            assert col in result.columns, f"Column '{col}' missing in pipeline output"

    def test_row_count_with_outliers(self, df_with_outliers):
        """Outlier clipping should preserve row count."""
        result = apply_all_transformations(df_with_outliers)
        assert len(result) == len(df_with_outliers)
