#!/usr/bin/env python3
"""
Reads an ArduPilot .BIN log file and pushes data to InfluxDB 3.

Each log message type (GPS, IMU, ATT, etc.) becomes a separate
measurement/table in InfluxDB.

Deduplication Strategy:
    ArduPilot logs often have multiple messages at the same timestamp.
    InfluxDB deduplicates points with identical (measurement + tags + time).
    To prevent data loss, timestamps are made strictly increasing per
    measurement type by adding minimal 1ns offsets to collisions.
"""

import argparse
import collections
import os
import sys
from typing import Dict, List

try:
    from pymavlink.DFReader import DFReader_binary
except ImportError:
    print("Error: pymavlink is not installed. Run: pip install pymavlink")
    sys.exit(1)

try:
    from influxdb_client_3 import InfluxDBClient3, Point
except ImportError:
    print("Error: influxdb3-python is not installed. Run: pip install influxdb3-python")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Metadata-only message types — no sensor data, skip during ingestion
SKIP_MESSAGES = {"FMT", "FMTU", "MULT", "ISBD", "ISBH"}

# Number of points to buffer before flushing to InfluxDB
BATCH_SIZE = 5_000

# InfluxDB requires at least one field per point; used as a fallback
PLACEHOLDER_FIELD_NAME = "_placeholder"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Push ArduPilot BIN log to InfluxDB 3",
    )
    parser.add_argument("bin_file", help="Path to the ArduPilot .BIN log file")
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
        default="ardupilot",
        help="InfluxDB database name (default: ardupilot)",
    )
    parser.add_argument(
        "--vehicle",
        default="",
        help="Optional vehicle name to add as a tag (e.g., drone1)",
    )
    return parser.parse_args()


def flush_buffer(
    client: InfluxDBClient3,
    database: str,
    points: List[Point],
) -> None:
    """Write a batch of points to InfluxDB and handle errors gracefully."""
    if not points:
        return
    try:
        client.write(database=database, record=points)
    except Exception as e:
        print(f"\nError writing to InfluxDB: {e}")


def deduplicate_timestamp(
    time_ns: int,
    msg_type: str,
    last_timestamps: Dict[str, int],
) -> int:
    """
    Ensure the timestamp is unique for this measurement type.

    If a message has the same (or earlier) nanosecond timestamp as the
    previous one of the same type, bump it by 1ns.  This prevents
    InfluxDB from silently overwriting points that share
    measurement + tags + timestamp.

    Args:
        time_ns:         Raw timestamp in nanoseconds.
        msg_type:        Measurement / message type (e.g. "IMU", "GPS").
        last_timestamps: Mutable dict tracking the last used ns per type.

    Returns:
        A guaranteed-unique nanosecond timestamp for *msg_type*.
    """
    if msg_type in last_timestamps and time_ns <= last_timestamps[msg_type]:
        time_ns = last_timestamps[msg_type] + 1
    last_timestamps[msg_type] = time_ns
    return time_ns


def build_point(msg_type: str, time_ns: int, fields: dict, vehicle: str = "") -> Point:
    """
    Build an InfluxDB Point from a parsed BIN log message.

    All values are stored as fields (never tags) to avoid schema
    conflicts where the same key appears as both tag and field.

    Args:
        msg_type: Measurement name (e.g. "IMU", "GPS", "ATT").
        time_ns:  Timestamp in nanoseconds.
        fields:   Dict of field_name → value from the BIN message.
        vehicle:  Optional vehicle tag.

    Returns:
        A populated InfluxDB Point ready for writing.
    """
    point = Point(msg_type).time(time_ns)
    if vehicle:
        point.tag("vehicle", vehicle)
    has_fields = False

    for field_name, value in fields.items():
        if value is None:
            continue
        if isinstance(value, (int, float)):
            point.field(field_name, value)
            has_fields = True
        elif isinstance(value, str):
            point.field(field_name, value)
            has_fields = True

    # InfluxDB requires at least one field per point
    if not has_fields:
        point.field(PLACEHOLDER_FIELD_NAME, 1)

    return point


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Read a BIN file, parse messages, and push to InfluxDB."""
    args = parse_args()

    if not os.path.exists(args.bin_file):
        print(f"Error: BIN file '{args.bin_file}' not found.")
        sys.exit(1)

    # --- Connect ---
    print(f"Connecting to InfluxDB at {args.host} (database: {args.database})…")
    client = InfluxDBClient3(host=args.host, token=args.token, database=args.database)

    # --- Open log ---
    print(f"Opening {args.bin_file}…")
    try:
        log = DFReader_binary(args.bin_file)
    except Exception as e:
        print(f"Error opening BIN file: {e}")
        sys.exit(1)

    file_size = os.path.getsize(args.bin_file)

    # Per-type point buffers, flushed at BATCH_SIZE
    buffers: Dict[str, List[Point]] = collections.defaultdict(list)
    counts: Dict[str, int] = collections.defaultdict(int)
    last_timestamps: Dict[str, int] = {}
    total_points = 0
    last_percent = -1

    # --- Parse & push ---
    print("Parsing messages…")

    while True:
        msg = log.recv_msg()
        if msg is None:
            break

        msg_type = msg.get_type()
        if msg_type in SKIP_MESSAGES:
            continue

        timestamp_s = getattr(msg, "_timestamp", None)
        if timestamp_s is None:
            continue

        # POSIX seconds (float, ~µs precision) → nanoseconds
        time_ns = int(timestamp_s * 1_000_000_000)

        # Bump collisions by 1ns to keep every point unique
        time_ns = deduplicate_timestamp(time_ns, msg_type, last_timestamps)

        # Build and buffer the point
        fields = msg.to_dict()
        fields.pop("mavpackettype", None)
        point = build_point(msg_type, time_ns, fields, args.vehicle)

        buffers[msg_type].append(point)
        counts[msg_type] += 1
        total_points += 1

        # Flush full buffers
        if len(buffers[msg_type]) >= BATCH_SIZE:
            flush_buffer(client, args.database, buffers[msg_type])
            buffers[msg_type].clear()

        # Progress based on file position
        try:
            percent = int((log.filehandle.tell() / file_size) * 100)
            if percent % 5 == 0 and percent != last_percent:
                print(f"  Progress: {percent}%")
                last_percent = percent
        except AttributeError:
            pass

    # Flush remaining buffers
    for msg_type, points in buffers.items():
        if points:
            flush_buffer(client, args.database, points)

    # --- Summary ---
    print(f"\nDone — pushed {total_points} points.")
    print("\nPoints per measurement:")
    for msg_type, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {msg_type}: {count}")


if __name__ == "__main__":
    main()
