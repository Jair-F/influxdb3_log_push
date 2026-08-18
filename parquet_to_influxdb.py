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
    rates: 100 Hz / 10 Hz / 5 Hz). This script splits columns by their
    top-level topic ("can", "sensor", "rs232") and writes each domain to
    its own measurement (``<domain>_data``), dropping rows that carry no
    data for that domain. InfluxDB still merges same-timestamp writes
    into one point server-side, so no data is lost — each domain's table
    is just dense instead of sparse.

Integer Preservation:
    Parquet stores the CAN/RS232 byte columns as uint8, but every row
    also has nulls (see above), and plain numpy integer arrays can't
    represent null. PyArrow's default Parquet->pandas conversion silently
    upcasts such columns to float64, so byte values were being written to
    InfluxDB as floats (`=5.0`) instead of integers (`=5i`). This script
    reads row groups with a types_mapper that keeps them as pandas'
    nullable Int64 dtype, which round-trips through the write client as
    a proper integer field.

Memory Strategy:
    Processes one Parquet row group at a time, then writes in sub-chunks
    of ``WRITE_CHUNK_SIZE`` rows to stay under InfluxDB's payload limit.
"""

import argparse
import time
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from influxdb_client_3 import InfluxDBClient3

import influx_cli

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum rows per InfluxDB write to stay under 10 MB payload limit
WRITE_CHUNK_SIZE = 10_000

DEFAULT_DATABASE = "sensor_logs"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Push Parquet data to InfluxDB 3.")
    parser.add_argument("parquet_file", help="Path to the input Parquet file.")
    influx_cli.add_connection_args(parser, default_database=DEFAULT_DATABASE)
    influx_cli.add_vehicle_arg(parser)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Parquet -> pandas, preserving integer columns
# ---------------------------------------------------------------------------


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
    return pd.Int64Dtype() if pa.types.is_integer(arrow_type) else None


def _read_row_group_as_frame(parquet_file: pq.ParquetFile, row_group_index: int) -> pd.DataFrame:
    """Read one row group and shape it for writing: indexed by publish_time, log_time as int64."""
    df = parquet_file.read_row_group(row_group_index).to_pandas(types_mapper=_types_mapper)

    df["publish_time"] = pd.to_datetime(df["publish_time"])
    df.set_index("publish_time", inplace=True)

    if "log_time" in df.columns:
        df["log_time"] = pd.to_datetime(df["log_time"]).astype("int64")

    df.dropna(axis=1, how="all", inplace=True)  # columns entirely empty in this row group
    return df


# ---------------------------------------------------------------------------
# Splitting by domain
# ---------------------------------------------------------------------------


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
        df: Wide dataframe indexed by publish_time, as produced by
            _read_row_group_as_frame().

    Returns:
        Dict mapping domain name (e.g. "can") to its own dataframe.
    """
    domain_columns: dict[str, list[str]] = {}
    for column in df.columns:
        if column == "log_time":
            continue
        domain_columns.setdefault(column.split("/", 1)[0], []).append(column)

    frames: dict[str, pd.DataFrame] = {}
    for domain, columns in domain_columns.items():
        keep = (["log_time"] if "log_time" in df.columns else []) + columns
        frame = df[keep].dropna(subset=columns, how="all").copy()
        if not frame.empty:
            frames[domain] = frame
    return frames


# ---------------------------------------------------------------------------
# Writing to InfluxDB
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WriteTarget:
    """Where a batch of rows should land in InfluxDB."""

    measurement: str
    vehicle: str = ""


def write_in_chunks(client: InfluxDBClient3, df: pd.DataFrame, target: WriteTarget) -> None:
    """
    Write a DataFrame to InfluxDB in sub-chunks.

    Splitting avoids hitting the 10 MB payload limit on large row groups.
    """
    tag_columns = None
    if target.vehicle:
        df["vehicle"] = target.vehicle
        tag_columns = ["vehicle"]

    for start in range(0, len(df), WRITE_CHUNK_SIZE):
        chunk = df.iloc[start : start + WRITE_CHUNK_SIZE]
        client.write(
            record=chunk,
            data_frame_measurement_name=target.measurement,
            data_frame_tag_columns=tag_columns,
        )


def _write_row_group(client: InfluxDBClient3, df: pd.DataFrame, vehicle: str) -> int:
    """Split one row group's frame by domain and write each to its own measurement.

    Returns the number of rows written.
    """
    rows_written = 0
    for domain, frame in split_by_domain(df).items():
        target = WriteTarget(measurement=f"{domain}_data", vehicle=vehicle)
        write_in_chunks(client, frame, target)
        rows_written += len(frame)
    return rows_written


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Read a Parquet file and push to InfluxDB, split by domain measurement."""
    args = parse_args()
    wall_start = time.time()

    print(f"Opening Parquet file: {args.parquet_file}")
    try:
        parquet_file = pq.ParquetFile(args.parquet_file)
    except Exception as e:
        print(f"Error opening Parquet file: {e}")
        return

    if "publish_time" not in parquet_file.schema.to_arrow_schema().names:
        print("Error: Parquet file must contain a 'publish_time' column.")
        return

    try:
        client = InfluxDBClient3(host=args.host, token=args.token, database=args.database)
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")
        return

    total_row_groups = parquet_file.num_row_groups
    total_rows = 0
    print(f"Pushing to InfluxDB. Row groups: {total_row_groups}")
    print("Measurements: can_data / sensor_data / rs232_data")

    for i in range(total_row_groups):
        try:
            df = _read_row_group_as_frame(parquet_file, i)
        except Exception as e:
            print(f"Error reading row group {i}: {e}")
            break

        if df.empty:
            continue

        try:
            total_rows += _write_row_group(client, df, args.vehicle)
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