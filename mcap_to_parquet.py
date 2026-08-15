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
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from mcap.reader import make_reader

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHUNK_SIZE = 50_000


# ---------------------------------------------------------------------------
# Helpers
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


def flatten_dict(
    d: Dict[str, Any],
    parent_key: str = "",
    sep: str = "/",
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary (and lists) into a single-level dictionary.

    Example:
        {"a": {"b": 1}, "c": [2, 3]} -> {"a/b": 1, "c_0": 2, "c_1": 3}

    Note: list items are joined with "_N" rather than "[N]". Square brackets
    are a reserved array/map-subscript operator in the SQL dialect InfluxDB 3
    uses (DataFusion), so a column literally named "data[0]" can fail to
    query even when quoted — the write succeeds (line protocol doesn't
    escape brackets) but SELECT on that field errors out. Underscore is safe
    everywhere: InfluxDB, plain SQL, Parquet, pandas.
    """
    items: List[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.append((f"{new_key}_{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)


def build_pyarrow_schema(input_path: str) -> tuple[pa.Schema, int]:
    """
    Scan the MCAP file to determine all possible columns and their types.

    Args:
        input_path: Path to the MCAP file.

    Returns:
        (PyArrow Schema, Total message count)
    """
    col_types: Dict[str, pa.DataType] = {}
    total_messages = 0

    with open(input_path, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()

        # Try to get total messages from summary block
        if summary and summary.statistics:
            for count in summary.statistics.channel_message_counts.values():
                total_messages += count

        for _, channel, message in reader.iter_messages():
            try:
                data = json.loads(message.data.decode("utf-8"))
            except Exception:
                continue

            flat = flatten_dict(data)
            for k, v in flat.items():
                col_name = sanitize_column_name(f"{channel.topic}/{k}")
                if col_name not in col_types:
                    if isinstance(v, float):
                        col_types[col_name] = pa.float64()
                    elif isinstance(v, bool):
                        col_types[col_name] = pa.bool_()
                    elif isinstance(v, int):
                        # Heuristic: byte arrays (flattened list items, which
                        # now end in "_<index>", e.g. "data_0") vs plain
                        # scalar integers like "id". A trailing-digit regex
                        # is used instead of a literal "data[" substring
                        # check so this isn't coupled to one field name.
                        if re.search(r"_\d+$", k):
                            col_types[col_name] = pa.uint8()
                        else:
                            col_types[col_name] = pa.int32()
                    else:
                        col_types[col_name] = pa.string()

    # Core timestamp fields
    fields = [
        pa.field("log_time", pa.timestamp("ns")),
        pa.field("publish_time", pa.timestamp("ns")),
    ]

    # Append discovered fields deterministically
    for col_name in sorted(col_types.keys()):
        fields.append(pa.field(col_name, col_types[col_name]))

    return pa.schema(fields), total_messages


def write_chunk(
    writer: Optional[pq.ParquetWriter],
    rows: Dict[str, list],
    schema: pa.Schema,
    output_path: str,
) -> pq.ParquetWriter:
    """
    Write a chunk of row data to the Parquet file.

    Args:
        writer:      The current ParquetWriter (or None if first chunk).
        rows:        Dict of column_name -> list of values.
        schema:      The full PyArrow schema.
        output_path: Destination file path (used only if writer is None).

    Returns:
        The ParquetWriter instance.
    """
    arrays = [pa.array(rows[f.name], type=f.type) for f in schema]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
    table = pa.Table.from_batches([batch])

    if writer is None:
        writer = pq.ParquetWriter(output_path, schema)

    writer.write_table(table)
    return writer


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
    pq_schema, total_messages = build_pyarrow_schema(args.input)
    fields = list(pq_schema)
    print(f"  Schema built with {len(fields)} columns.")

    print("Pass 2: Converting messages in chunks…")
    rows: Dict[str, list] = {f.name: [] for f in fields}
    messages_processed = 0
    writer: Optional[pq.ParquetWriter] = None

    with open(args.input, "rb") as f:
        reader = make_reader(f)

        for _, channel, message in reader.iter_messages():
            # Initialize empty row
            row_data: Dict[str, Any] = {f.name: None for f in fields}
            row_data["log_time"] = message.log_time
            row_data["publish_time"] = message.publish_time

            # Parse and map JSON payload
            try:
                data = json.loads(message.data.decode("utf-8"))
                flat = flatten_dict(data)
                for k, v in flat.items():
                    col_name = sanitize_column_name(f"{channel.topic}/{k}")
                    if col_name in row_data:
                        row_data[col_name] = v
            except Exception:
                pass

            # Append to column arrays
            for field in fields:
                rows[field.name].append(row_data[field.name])

            messages_processed += 1

            # Write full chunks
            if messages_processed % CHUNK_SIZE == 0:
                writer = write_chunk(writer, rows, pq_schema, args.output)
                # Clear buffer
                rows = {f.name: [] for f in fields}

                if total_messages > 0:
                    pct = (messages_processed / total_messages) * 100
                    print(
                        f"  Progress: {pct:.1f}% ({messages_processed}/{total_messages})"
                    )
                else:
                    print(f"  Progress: {messages_processed} messages")

        # Write remaining partial chunk
        if messages_processed % CHUNK_SIZE != 0:
            writer = write_chunk(writer, rows, pq_schema, args.output)
            if total_messages > 0:
                print(f"  Progress: 100.0% ({messages_processed}/{total_messages})")

    if writer:
        writer.close()

    print(f"\nDone — saved Parquet to {args.output}")


if __name__ == "__main__":
    main()