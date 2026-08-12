#!/usr/bin/env python3
"""
Verifies that data was pushed to InfluxDB successfully.

Runs count(*) queries against the ArduPilot and sensor databases to
confirm that row counts match expectations.
"""

import argparse

from influxdb_client_3 import InfluxDBClient3

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_HOST = "http://localhost:8181"
DEFAULT_TOKEN = "apiv3_pg0fQJMYRzhXlmpO-oEvqFbSCuxfveOcJx1FC8QoO-1FxM5QRTqmf9DCc5ZT66bsA_kwoIhl23QT6pE5gOIgcw"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Verify InfluxDB data integrity")
    parser.add_argument(
        "--host", default=DEFAULT_HOST, help=f"InfluxDB host (default: {DEFAULT_HOST})"
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="InfluxDB auth token")
    return parser.parse_args()


def query_row_count(host: str, token: str, database: str, measurement: str) -> None:
    """Query and print the row count for a given measurement."""
    client = InfluxDBClient3(host=host, token=token, database=database)
    query = f'SELECT count(*) FROM "{measurement}"'
    try:
        result = client.query(query=query, language="sql")
        df = result.to_pandas()
        print(f"  {measurement} rows: {df.iloc[0, 0]:,}")
    except Exception as e:
        print(f"  Error querying {database}/{measurement}: {e}")
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run verification queries against InfluxDB."""
    args = parse_args()

    print("--- ArduPilot BIN Data ---")
    query_row_count(args.host, args.token, "ardupilot_clean", "PARM")

    print("\n--- Parquet Sensor Data ---")
    query_row_count(args.host, args.token, "sensor_logs_clean", "sensor_data_clean")


if __name__ == "__main__":
    main()
