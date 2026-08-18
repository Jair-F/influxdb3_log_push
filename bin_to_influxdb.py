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
from dataclasses import dataclass, field
from typing import Optional

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

import influx_cli

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Metadata-only message types — no sensor data, skip during ingestion
SKIP_MESSAGES = {"FMT", "FMTU", "MULT", "ISBD", "ISBH"}

# Number of points to buffer (per message type) before flushing to InfluxDB
BATCH_SIZE = 5_000

# InfluxDB requires at least one field per point; used as a fallback
PLACEHOLDER_FIELD_NAME = "_placeholder"

NANOSECONDS_PER_SECOND = 1_000_000_000

DEFAULT_DATABASE = "ardupilot"

# Point field values must be one of these types; anything else (e.g. bytes,
# lists, enums) is dropped rather than guessed at.
FIELD_VALUE_TYPES = (int, float, str)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Push ArduPilot BIN log to InfluxDB 3")
    parser.add_argument("bin_file", help="Path to the ArduPilot .BIN log file")
    influx_cli.add_connection_args(parser, default_database=DEFAULT_DATABASE)
    influx_cli.add_vehicle_arg(parser)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Timestamp deduplication
# ---------------------------------------------------------------------------


def deduplicate_timestamp(time_ns: int, msg_type: str, last_timestamps: dict[str, int]) -> int:
    """
    Ensure the timestamp is unique for this measurement type.

    If a message has the same (or earlier) nanosecond timestamp as the
    previous one of the same type, bump it by 1ns. This prevents
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


# ---------------------------------------------------------------------------
# Building points
# ---------------------------------------------------------------------------


@dataclass
class LogMessage:
    """One parsed BIN log message, ready to become an InfluxDB Point."""

    msg_type: str
    time_ns: int
    fields: dict = field(default_factory=dict)
    vehicle: str = ""


def build_point(message: LogMessage) -> Point:
    """
    Build an InfluxDB Point from a parsed BIN log message.

    All values are stored as fields (never tags) to avoid schema
    conflicts where the same key appears as both tag and field.
    """
    point = Point(message.msg_type).time(message.time_ns)
    if message.vehicle:
        point.tag("vehicle", message.vehicle)

    has_fields = False
    for field_name, value in message.fields.items():
        if isinstance(value, FIELD_VALUE_TYPES):
            point.field(field_name, value)
            has_fields = True

    if not has_fields:
        point.field(PLACEHOLDER_FIELD_NAME, 1)

    return point


def _message_to_point(msg, last_timestamps: dict[str, int], vehicle: str) -> Optional[tuple[str, Point]]:
    """Convert one raw pymavlink message into (msg_type, Point), or None to skip it."""
    msg_type = msg.get_type()
    if msg_type in SKIP_MESSAGES:
        return None

    timestamp_s = getattr(msg, "_timestamp", None)
    if timestamp_s is None:
        return None

    time_ns = int(timestamp_s * NANOSECONDS_PER_SECOND)
    time_ns = deduplicate_timestamp(time_ns, msg_type, last_timestamps)

    fields = msg.to_dict()
    fields.pop("mavpackettype", None)

    point = build_point(LogMessage(msg_type=msg_type, time_ns=time_ns, fields=fields, vehicle=vehicle))
    return msg_type, point


# ---------------------------------------------------------------------------
# Buffering and writing
# ---------------------------------------------------------------------------


class PointBuffers:
    """Per-message-type point buffers, flushed once BATCH_SIZE is reached."""

    def __init__(self, batch_size: int):
        self._batch_size = batch_size
        self._pending: dict[str, list[Point]] = collections.defaultdict(list)
        self.counts: dict[str, int] = collections.defaultdict(int)

    def add(self, msg_type: str, point: Point) -> Optional[list[Point]]:
        """Buffer a point. Returns a full batch ready to flush, or None."""
        self._pending[msg_type].append(point)
        self.counts[msg_type] += 1
        if len(self._pending[msg_type]) >= self._batch_size:
            batch, self._pending[msg_type] = self._pending[msg_type], []
            return batch
        return None

    def drain(self) -> dict[str, list[Point]]:
        """Return and clear everything still buffered (call once at the end)."""
        remaining = {msg_type: points for msg_type, points in self._pending.items() if points}
        self._pending.clear()
        return remaining


def flush_buffer(client: InfluxDBClient3, database: str, points: list[Point]) -> None:
    """Write a batch of points to InfluxDB and handle errors gracefully."""
    if not points:
        return
    try:
        client.write(database=database, record=points)
    except Exception as e:
        print(f"\nError writing to InfluxDB: {e}")


# ---------------------------------------------------------------------------
# Progress reporting
# ---------------------------------------------------------------------------


class FileProgressTracker:
    """Prints '  Progress: N%' at STEP_PERCENT increments based on file position."""

    STEP_PERCENT = 5

    def __init__(self, file_size: int):
        self._file_size = file_size
        self._last_percent = -1

    def maybe_print(self, file_position: int) -> None:
        if self._file_size <= 0:
            return
        percent = int((file_position / self._file_size) * 100)
        if percent % self.STEP_PERCENT == 0 and percent != self._last_percent:
            print(f"  Progress: {percent}%")
            self._last_percent = percent


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _open_bin_log(bin_file: str) -> DFReader_binary:
    return DFReader_binary(bin_file)


def main() -> None:
    """Read a BIN file, parse messages, and push to InfluxDB."""
    args = parse_args()

    if not os.path.exists(args.bin_file):
        print(f"Error: BIN file '{args.bin_file}' not found.")
        sys.exit(1)

    print(f"Connecting to InfluxDB at {args.host} (database: {args.database})…")
    client = InfluxDBClient3(host=args.host, token=args.token, database=args.database)

    print(f"Opening {args.bin_file}…")
    try:
        log = _open_bin_log(args.bin_file)
    except Exception as e:
        print(f"Error opening BIN file: {e}")
        sys.exit(1)

    buffers = PointBuffers(BATCH_SIZE)
    last_timestamps: dict[str, int] = {}
    progress = FileProgressTracker(os.path.getsize(args.bin_file))
    total_points = 0

    print("Parsing messages…")
    while (msg := log.recv_msg()) is not None:
        result = _message_to_point(msg, last_timestamps, args.vehicle)
        if result is None:
            continue

        msg_type, point = result
        total_points += 1
        full_batch = buffers.add(msg_type, point)
        if full_batch is not None:
            flush_buffer(client, args.database, full_batch)

        try:
            progress.maybe_print(log.filehandle.tell())
        except AttributeError:
            pass

    for points in buffers.drain().values():
        flush_buffer(client, args.database, points)

    print(f"\nDone — pushed {total_points} points.")
    print("\nPoints per measurement:")
    for msg_type, count in sorted(buffers.counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {msg_type}: {count}")


if __name__ == "__main__":
    main()