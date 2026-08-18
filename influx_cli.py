#!/usr/bin/env python3
"""
Shared argparse helpers for the InfluxDB ingestion and verification scripts.

Every script here (bin_to_influxdb.py, parquet_to_influxdb.py,
verify_data.py) needs the same --host/--token/--database options. Defining
them once avoids the three copies drifting out of sync, and lets the token
default come from an environment variable instead of shell history or,
worse, a hardcoded value in source code.
"""

import argparse
import os

DEFAULT_HOST = "http://localhost:8181"
TOKEN_ENV_VAR = "INFLUXDB3_TOKEN"  # matches the env var name already used in docker-compose.yml


def add_host_token_args(parser: argparse.ArgumentParser) -> None:
    """Add the --host/--token options every script here needs."""
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"InfluxDB host URL (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get(TOKEN_ENV_VAR, ""),
        help=f"InfluxDB authentication token (default: ${TOKEN_ENV_VAR})",
    )


def add_connection_args(parser: argparse.ArgumentParser, *, default_database: str) -> None:
    """Add --host/--token plus a single target --database (for scripts that write to one database)."""
    add_host_token_args(parser)
    parser.add_argument(
        "--database",
        default=default_database,
        help=f"Target database name (default: {default_database})",
    )


def add_vehicle_arg(parser: argparse.ArgumentParser) -> None:
    """Add the optional --vehicle tag shared by the ingestion scripts."""
    parser.add_argument(
        "--vehicle",
        default="",
        help="Optional vehicle name to add as a tag (e.g., drone1)",
    )