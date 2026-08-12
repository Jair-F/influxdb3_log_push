#!/usr/bin/env python3
"""
Reads a Parquet file and pushes its contents to InfluxDB 3.

Uses ``publish_time`` as the InfluxDB timestamp and keeps ``log_time``
as a regular field (int64 nanoseconds).

Alignment Strategy:
    Since every Parquet column has a unique name (e.g.
    ``sensor/sensor1/value_raw``, ``can/0x001/data[0]``), InfluxDB
    natively merges them into a single wide row per timestamp.
    No timestamp manipulation is needed.

Memory Strategy:
    Processes one Parquet row group at a time, then writes in sub-chunks
    of ``WRITE_CHUNK_SIZE`` rows to stay under InfluxDB's payload limit.
"""

import argparse
import time

import pandas as pd
import pyarrow.parquet as pq
from influxdb_client_3 import InfluxDBClient3

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum rows per InfluxDB write to stay under 10 MB payload limit
WRITE_CHUNK_SIZE = 10_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Push Parquet data to InfluxDB 3.",
    )
    parser.add_argument("parquet_file", help="Path to the input Parquet file.")
    parser.add_argument(
        "--host",
        default="http://localhost:8181",
        help="InfluxDB host URL (default: http://localhost:8181)",
    )
    parser.add_argument(
        "--token",
        default="",
        help="InfluxDB authentication token",
    )
    parser.add_argument(
        "--database",
        default="sensor_logs",
        help="Target database name (default: sensor_logs)",
    )
    parser.add_argument(
        "--measurement",
        default="sensor_data",
        help="Target measurement name (default: sensor_data)",
    )
    parser.add_argument(
        "--vehicle",
        default="",
        help="Optional vehicle name to add as a tag (e.g., drone1)",
    )
    return parser.parse_args()


def write_in_chunks(
    client: InfluxDBClient3,
    df: pd.DataFrame,
    measurement: str,
    vehicle: str = "",
) -> None:
    """
    Write a DataFrame to InfluxDB in sub-chunks.

    Splitting avoids hitting the 10 MB payload limit on large row groups.

    Args:
        client:      InfluxDB client.
        df:          DataFrame with a DatetimeIndex to write.
        measurement: Target measurement name in InfluxDB.
        vehicle:     Optional vehicle tag.
    """
    tag_columns = []
    if vehicle:
        df["vehicle"] = vehicle
        tag_columns = ["vehicle"]

    for start in range(0, len(df), WRITE_CHUNK_SIZE):
        chunk = df.iloc[start : start + WRITE_CHUNK_SIZE]
        client.write(
            record=chunk,
            data_frame_measurement_name=measurement,
            data_frame_tag_columns=tag_columns if tag_columns else None,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Read a Parquet file and push to InfluxDB."""
    args = parse_args()
    wall_start = time.time()

    # --- Open file ---
    print(f"Opening Parquet file: {args.parquet_file}")
    try:
        parquet_file = pq.ParquetFile(args.parquet_file)
    except Exception as e:
        print(f"Error opening Parquet file: {e}")
        return

    # Validate that publish_time exists
    schema = parquet_file.schema.to_arrow_schema()
    if "publish_time" not in schema.names:
        print("Error: Parquet file must contain a 'publish_time' column.")
        return

    # --- Connect ---
    try:
        client = InfluxDBClient3(
            host=args.host, token=args.token, database=args.database
        )
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")
        return

    total_row_groups = parquet_file.num_row_groups
    total_rows = 0

    print(f"Pushing to InfluxDB. Row groups: {total_row_groups}")

    # --- Process row groups ---
    for i in range(total_row_groups):
        try:
            df = parquet_file.read_row_group(i).to_pandas()
        except Exception as e:
            print(f"Error reading row group {i}: {e}")
            break

        if df.empty:
            continue

        # Set publish_time as the InfluxDB timestamp index
        df["publish_time"] = pd.to_datetime(df["publish_time"])
        df.set_index("publish_time", inplace=True)

        # Keep log_time as a regular field (int64 nanoseconds)
        if "log_time" in df.columns:
            df["log_time"] = pd.to_datetime(df["log_time"]).astype("int64")

        # Drop columns that are entirely NaN in this chunk
        df.dropna(axis=1, how="all", inplace=True)

        try:
            write_in_chunks(client, df, args.measurement, args.vehicle)
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")
            break

        total_rows += len(df)
        pct = (i + 1) / total_row_groups * 100
        print(f"  Row group {i + 1}/{total_row_groups} ({pct:.1f}%)")

    client.close()

    elapsed = time.time() - wall_start
    print("---")
    print(f"Done — pushed {total_rows:,} rows in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
