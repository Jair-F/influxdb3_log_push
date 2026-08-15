#!/usr/bin/env python3
"""
Reads a Parquet file and pushes its contents to InfluxDB 3.

Uses ``publish_time`` as the InfluxDB timestamp and keeps ``log_time``
as a regular field (int64 nanoseconds).

Measurement Strategy:
    The source Parquet file has one row per original MCAP message, so on
    any given row only that message's own columns are populated — every
    other column (every other sensor, every other CAN id, ...) is null.
    Writing everything into a single measurement means a query for one
    field spends most of its rows on other channels' nulls (92-99% null
    in this dataset, since CAN/sensor/RS232 sample at very different
    rates: 100 Hz / 10 Hz / 5 Hz). By default this script now splits
    columns by their top-level topic ("can", "sensor", "rs232") and
    writes each domain to its own measurement (``<domain>_data``),
    dropping rows that carry no data for that domain. InfluxDB still
    merges same-timestamp writes into one point server-side, so no data
    is lost — each domain's table is just dense instead of sparse.
    Pass --single-measurement to restore the old combined-table behavior.

Integer Preservation:
    Parquet stores the CAN/RS232 byte columns as uint8, but every row
    also has nulls (see above), and plain numpy integer arrays can't
    represent null. PyArrow's default Parquet->pandas conversion silently
    upcasts such columns to float64, so byte values were being written to
    InfluxDB as floats (`=5.0`) instead of integers (`=5i`). This script
    now reads row groups with a types_mapper that keeps them as pandas'
    nullable Int64 dtype, which round-trips through the write client as
    a proper integer field.

Memory Strategy:
    Processes one Parquet row group at a time, then writes in sub-chunks
    of ``WRITE_CHUNK_SIZE`` rows to stay under InfluxDB's payload limit.
"""

import argparse
import time

import pandas as pd
import pyarrow as pa
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
    parser.add_argument(
        "--single-measurement",
        action="store_true",
        help=(
            "Write every column into one combined measurement (named by "
            "--measurement), matching the old behavior. By default, columns "
            "are split by domain (can/sensor/rs232) into their own "
            "measurements (<domain>_data) since mixing sample rates that "
            "differ 20x in one wide table leaves most fields null on most "
            "rows."
        ),
    )
    return parser.parse_args()


def _types_mapper(arrow_type: pa.DataType):
    """
    Map Arrow integer types to pandas' nullable Int64 instead of the numpy
    int types pandas would otherwise silently fall back to float64 for.

    Every integer column in this dataset has nulls (each row is one MCAP
    message, so only that message's own columns are populated), and numpy's
    int dtypes can't hold null — pandas' default `to_pandas()` behavior is
    to upcast such columns to float64, which quietly changes CAN/RS232 byte
    fields from integers to floats on their way into InfluxDB. Returning
    pandas' extension Int64Dtype for integer Arrow types preserves the
    original integer semantics through the null-heavy conversion.
    """
    if pa.types.is_integer(arrow_type):
        return pd.Int64Dtype()
    return None


def split_by_domain(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Split a wide, multi-domain frame into one dense frame per domain.

    The source frame has one row per original MCAP message, so on any
    given row only that message's own columns are populated. The "domain"
    is the first path segment of a column name (e.g. "can" in
    "can/0x001/data_0"). For each domain, keep only that domain's columns
    (plus log_time) and drop rows that are null across all of them — those
    rows belong to a different domain and have nothing to contribute here.
    InfluxDB merges multiple writes that share the same series+timestamp
    into a single point, so splitting like this does not lose the
    same-timestamp alignment between domains, it just avoids writing rows
    that would otherwise be all-null.

    Args:
        df: Wide dataframe indexed by publish_time, as produced for one
            Parquet row group (log_time already cast, publish_time already
            the index, fully-empty columns already dropped).

    Returns:
        Dict mapping domain name (e.g. "can") to its own dataframe.
    """
    domain_cols: dict[str, list[str]] = {}
    for col in df.columns:
        if col == "log_time":
            continue
        domain_cols.setdefault(col.split("/", 1)[0], []).append(col)

    frames: dict[str, pd.DataFrame] = {}
    for domain, cols in domain_cols.items():
        keep = (["log_time"] if "log_time" in df.columns else []) + cols
        sub = df[keep].dropna(subset=cols, how="all").copy()
        if not sub.empty:
            frames[domain] = sub
    return frames


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

    if args.single_measurement:
        print(f"Mode: single measurement ({args.measurement!r})")
    else:
        print("Mode: split by domain (can_data / sensor_data / rs232_data / ...)")

    # --- Process row groups ---
    for i in range(total_row_groups):
        try:
            # types_mapper keeps nullable integer columns (the CAN/RS232
            # byte fields) as pandas Int64 instead of silently upcasting
            # them to float64, which is numpy's only way to hold a null.
            df = parquet_file.read_row_group(i).to_pandas(types_mapper=_types_mapper)
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
            if args.single_measurement:
                write_in_chunks(client, df, args.measurement, args.vehicle)
                total_rows += len(df)
            else:
                for domain, sub_df in split_by_domain(df).items():
                    write_in_chunks(client, sub_df, f"{domain}_data", args.vehicle)
                    total_rows += len(sub_df)
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")
            break

        pct = (i + 1) / total_row_groups * 100
        print(f"  Row group {i + 1}/{total_row_groups} ({pct:.1f}%)")

    client.close()

    elapsed = time.time() - wall_start
    print("---")
    print(f"Done — pushed {total_rows:,} rows in {elapsed:.1f}s")


if __name__ == "__main__":
    main()