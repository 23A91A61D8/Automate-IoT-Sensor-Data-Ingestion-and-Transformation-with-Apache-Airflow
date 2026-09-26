"""
generate_sensor_data.py
-----------------------
Simulates IoT sensor readings and writes them to incremental CSV files
in the data/ directory. Each invocation creates a new timestamped file
with 100-500 rows of sensor data.

Usage:
    python scripts/generate_sensor_data.py [--rows 250] [--output-dir data/]
"""

import argparse
import logging
import os
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEVICE_IDS = [f"sensor_{i:03d}" for i in range(1, 11)]  # sensor_001 … sensor_010

TEMP_MEAN = 22.0      # °C
TEMP_STD = 8.0
HUMIDITY_MEAN = 55.0  # %
HUMIDITY_STD = 15.0
PRESSURE_MEAN = 1013.25  # hPa
PRESSURE_STD = 10.0

# Probability of injecting a "bad" value (for realism)
ANOMALY_RATE = 0.02


# ---------------------------------------------------------------------------
# Core generator
# ---------------------------------------------------------------------------
def generate_sensor_batch(
    num_rows: int = 250,
    start_time: datetime | None = None,
    anomaly_rate: float = ANOMALY_RATE,
) -> pd.DataFrame:
    """
    Generate a batch of simulated IoT sensor readings.

    Parameters
    ----------
    num_rows : int
        Number of sensor readings to generate (100–500).
    start_time : datetime, optional
        Base datetime for the batch. Defaults to one hour ago (UTC).
    anomaly_rate : float
        Fraction of rows that contain intentional anomalies for DQ testing.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: device_id, timestamp, temperature,
        humidity, pressure.
    """
    num_rows = max(100, min(500, num_rows))  # clamp to [100, 500]

    if start_time is None:
        start_time = datetime.now(timezone.utc) - timedelta(hours=1)

    rng = np.random.default_rng(seed=None)

    # Timestamps spread over a 1-hour window
    offsets_seconds = sorted(rng.integers(0, 3600, size=num_rows).tolist())
    timestamps = [
        (start_time + timedelta(seconds=int(s))).strftime("%Y-%m-%dT%H:%M:%SZ")
        for s in offsets_seconds
    ]

    device_ids = rng.choice(DEVICE_IDS, size=num_rows).tolist()

    temperatures = rng.normal(TEMP_MEAN, TEMP_STD, size=num_rows).round(2).tolist()
    humidities = rng.normal(HUMIDITY_MEAN, HUMIDITY_STD, size=num_rows).round(2).tolist()
    pressures = rng.normal(PRESSURE_MEAN, PRESSURE_STD, size=num_rows).round(2).tolist()

    # Inject anomalies
    num_anomalies = max(1, int(num_rows * anomaly_rate))
    anomaly_indices = rng.choice(num_rows, size=num_anomalies, replace=False).tolist()

    for idx in anomaly_indices:
        anomaly_type = rng.integers(0, 4)
        if anomaly_type == 0:
            temperatures[idx] = None          # null temperature
        elif anomaly_type == 1:
            temperatures[idx] = round(float(rng.choice([-60.0, 200.0])), 2)  # out-of-range
        elif anomaly_type == 2:
            humidities[idx] = round(float(rng.choice([-5.0, 110.0])), 2)     # out-of-range
        else:
            pressures[idx] = None              # null pressure

    df = pd.DataFrame(
        {
            "device_id": device_ids,
            "timestamp": timestamps,
            "temperature": temperatures,
            "humidity": humidities,
            "pressure": pressures,
        }
    )

    logger.info(
        "Generated %d sensor readings (%d anomalies injected).",
        num_rows,
        num_anomalies,
    )
    return df


def write_sensor_csv(
    df: pd.DataFrame,
    output_dir: str = "data",
    run_time: datetime | None = None,
) -> str:
    """
    Write a sensor DataFrame to a timestamped CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        Sensor data to persist.
    output_dir : str
        Directory in which to save the file.
    run_time : datetime, optional
        Timestamp used in the filename. Defaults to now (UTC).

    Returns
    -------
    str
        Absolute path of the written file.
    """
    if run_time is None:
        run_time = datetime.now(timezone.utc)

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    filename = f"sensor_readings_{run_time.strftime('%Y%m%d%H%M')}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    logger.info("Sensor data written to '%s' (%d rows).", filepath, len(df))
    return filepath


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate IoT sensor data and write to a CSV file."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=250,
        help="Number of sensor readings to generate (100–500). Default: 250.",
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Directory to save the generated CSV. Default: data/",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=ANOMALY_RATE,
        help=f"Fraction of rows with injected anomalies. Default: {ANOMALY_RATE}.",
    )
    args = parser.parse_args()

    df = generate_sensor_batch(
        num_rows=args.rows,
        anomaly_rate=args.anomaly_rate,
    )
    filepath = write_sensor_csv(df, output_dir=args.output_dir)
    print(f"Generated: {filepath}")


if __name__ == "__main__":
    main()
