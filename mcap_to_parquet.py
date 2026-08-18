#!/usr/bin/env python3
"""
Reads an MCAP file and converts it to a flattened Parquet file.

Each MCAP message is flattened (nested dictionaries/arrays become
separate columns like `sensor/sensor1/value_raw`).
The Parquet file uses `publish_time` and `log_time` as its timestamps.

Memory Strategy:
    Scans the file once to build a comprehensive schema, then streams
    the messages in chunks (default 50,000) to keep memory usage bounded.
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from mcap.reader import make_reader
from mcap.records import Channel

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHUNK_SIZE = 50_000
LOG_TIME_FIELD = pa.field("log_time", pa.timestamp("ns"))
PUBLISH_TIME_FIELD = pa.field("publish_time", pa.timestamp("ns"))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Convert MCAP to Parquet")
    parser.add_argument(
        "--input",
        default="simulated_log.mcap",
        help="Path to the input MCAP file (default: simulated_log.mcap)",
    )
    parser.add_argument(
        "--output",
        default="simulated_log.parquet",
        help="Path to the output Parquet file (default: simulated_log.parquet)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Column naming
# ---------------------------------------------------------------------------


def sanitize_column_name(name: str) -> str:
    """
    Make a flattened column name safe to query.

    Two separate problems have shown up in practice, both originating from
    the MCAP topic names (e.g. "can/0x001", "rs232/0x41") flowing straight
    through into column names:

    1. Array indices flattened as "[0]" collide with the array/map subscript
       operator in the SQL dialect InfluxDB 3 uses, so a column named
       "...data[0]" fails to SELECT even quoted. flatten_dict() already
       avoids this by using "_0" instead of "[0]".
    2. The literal substring "0x" (from the hex-style topic names, e.g.
       "can/0x00A") is rejected outright by the query tool — "Hex-encoded
       values (0x...) are not allowed" — even inside a quoted identifier.
       That's a plain text-level guard, not one that understands SQL
       quoting, so the only reliable fix is to never emit "0x" at all.

    "0x001" -> "h001" keeps the value visually hex-like (and still unique
    per CAN id) without containing the blocked substring.
    """
    return re.sub(r"0[xX]", "h", name)


def flatten_dict(d: dict[str, Any], parent_key: str = "", sep: str = "/") -> dict[str, Any]:
    """
    Flatten a nested dictionary (and lists) into a single-level dictionary.

    Example:
        {"a": {"b": 1}, "c": [2, 3]} -> {"a/b": 1, "c_0": 2, "c_1": 3}

    Note: list items are joined with "_N" rather than "[N]" — see
    sanitize_column_name() for why square brackets are unsafe here.
    """
    items: list[tuple[str, Any]] = []
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            items.extend((f"{new_key}_{i}", item) for i, item in enumerate(value))
        else:
            items.append((new_key, value))
    return dict(items)


def _is_flattened_list_item(key: str) -> bool:
    """True for keys like "data_0" that came from a list index, not a scalar field like "id"."""
    return re.search(r"_\d+$", key) is not None


# ---------------------------------------------------------------------------
# Reading MCAP messages
# ---------------------------------------------------------------------------


@dataclass
class FlatMessage:
    """One MCAP message, JSON-decoded and flattened to sanitized column names."""

    channel: Channel
    log_time: int
    publish_time: int
    fields: dict[str, Any]


def iter_flat_messages(input_path: str) -> Iterator[FlatMessage]:
    """
    Yield every MCAP message as a FlatMessage.

    Messages whose payload isn't valid JSON are silently skipped, matching
    the original behavior of tolerating malformed entries rather than
    aborting the whole conversion.

    This is the single source of truth for "how does a raw MCAP message
    become column data" — both the schema-scanning pass and the row-writing
    pass consume it, so the two passes can't drift out of sync with each
    other.
    """
    with open(input_path, "rb") as f:
        reader = make_reader(f)
        for _, channel, message in reader.iter_messages():
            try:
                payload = json.loads(message.data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue

            fields = {
                sanitize_column_name(f"{channel.topic}/{key}"): value
                for key, value in flatten_dict(payload).items()
            }
            yield FlatMessage(
                channel=channel, log_time=message.log_time, publish_time=message.publish_time, fields=fields
            )


def count_messages(input_path: str) -> int:
    """Return the total message count from the MCAP summary block, or 0 if unavailable."""
    with open(input_path, "rb") as f:
        summary = make_reader(f).get_summary()
    if not summary or not summary.statistics:
        return 0
    return sum(summary.statistics.channel_message_counts.values())


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


def _infer_arrow_type(col_name: str, value: Any) -> pa.DataType:
    """Pick a Parquet column type for one flattened (column name, value) pair."""
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        # Flattened list items (column names ending "_<digits>", e.g.
        # ".../data_0") are byte-range values in this dataset (CAN/RS232
        # payload bytes); plain scalar integers like ".../id" are not
        # assumed to fit a byte.
        return pa.uint8() if _is_flattened_list_item(col_name) else pa.int32()
    return pa.string()


def build_pyarrow_schema(input_path: str) -> pa.Schema:
    """Scan the MCAP file once to determine every column and its type."""
    col_types: dict[str, pa.DataType] = {}

    for message in iter_flat_messages(input_path):
        for col_name, value in message.fields.items():
            if col_name not in col_types:
                col_types[col_name] = _infer_arrow_type(col_name, value)

    fields = [LOG_TIME_FIELD, PUBLISH_TIME_FIELD]
    fields.extend(pa.field(name, col_types[name]) for name in sorted(col_types))
    return pa.schema(fields)


# ---------------------------------------------------------------------------
# Writing Parquet
# ---------------------------------------------------------------------------


class ChunkedParquetWriter:
    """Accumulates rows in column-oriented form and flushes full chunks to disk."""

    def __init__(self, schema: pa.Schema, output_path: str, chunk_size: int = CHUNK_SIZE):
        self._schema = schema
        self._output_path = output_path
        self._chunk_size = chunk_size
        self._writer: Optional[pq.ParquetWriter] = None
        self._columns: dict[str, list] = {field.name: [] for field in schema}
        self._buffered_rows = 0

    def add_row(self, row: dict[str, Any]) -> None:
        """Buffer one row (dict of column name -> value), flushing if the chunk is full."""
        for field in self._schema:
            self._columns[field.name].append(row.get(field.name))
        self._buffered_rows += 1
        if self._buffered_rows >= self._chunk_size:
            self._flush()

    def close(self) -> None:
        """Flush any remaining buffered rows and close the underlying writer."""
        self._flush()
        if self._writer is not None:
            self._writer.close()

    def _flush(self) -> None:
        if self._buffered_rows == 0:
            return
        arrays = [pa.array(self._columns[field.name], type=field.type) for field in self._schema]
        table = pa.Table.from_batches([pa.RecordBatch.from_arrays(arrays, schema=self._schema)])

        if self._writer is None:
            self._writer = pq.ParquetWriter(self._output_path, self._schema)
        self._writer.write_table(table)

        self._columns = {field.name: [] for field in self._schema}
        self._buffered_rows = 0


def _row_from_message(message: FlatMessage, schema: pa.Schema) -> dict[str, Any]:
    """Build one output row: timestamps plus whichever schema columns this message populates."""
    row: dict[str, Any] = {"log_time": message.log_time, "publish_time": message.publish_time}
    field_names = {field.name for field in schema}
    row.update((name, value) for name, value in message.fields.items() if name in field_names)
    return row


def _print_progress(processed: int, total: int) -> None:
    if total > 0:
        pct = (processed / total) * 100
        print(f"  Progress: {pct:.1f}% ({processed}/{total})")
    else:
        print(f"  Progress: {processed} messages")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entrypoint for MCAP to Parquet conversion."""
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file '{args.input}' not found.")
        sys.exit(1)

    print("Pass 1: Scanning all messages to build the schema…")
    schema = build_pyarrow_schema(args.input)
    total_messages = count_messages(args.input)
    print(f"  Schema built with {len(schema)} columns.")

    print("Pass 2: Converting messages in chunks…")
    writer = ChunkedParquetWriter(schema, args.output)

    messages_processed = 0
    for message in iter_flat_messages(args.input):
        writer.add_row(_row_from_message(message, schema))
        messages_processed += 1
        if messages_processed % CHUNK_SIZE == 0:
            _print_progress(messages_processed, total_messages)

    writer.close()
    if messages_processed % CHUNK_SIZE != 0:
        _print_progress(messages_processed, total_messages)

    print(f"\nDone — saved Parquet to {args.output}")


if __name__ == "__main__":
    main()