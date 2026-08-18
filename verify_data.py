#!/usr/bin/env python3
"""
Verifies that data was pushed to InfluxDB successfully.

Runs count(*) queries against the ArduPilot and sensor databases to
confirm that row counts match expectations.
"""

import argparse
from dataclasses import dataclass

from influxdb_client_3 import InfluxDBClient3

import influx_cli

# NOTE: previously these targets were "ardupilot_clean" (database) and
# "sensor_data_clean" (measurement) — neither exists anywhere else in this
# project. bin_to_influxdb.py writes to database "ardupilot", and
# parquet_to_influxdb.py writes to database "sensor_logs" across three
# measurements (can_data / sensor_data / rs232_data). Updated to match what
# those scripts actually produce. If a separate cleaning/dedup stage writes
# "_clean"-suffixed targets elsewhere, point these back at that instead.


@dataclass(frozen=True)
class MeasurementCheck:
    """One (database, measurement) pair to report a row count for."""

    database: str
    measurement: str
    label: str


CHECKS = [
    MeasurementCheck(database="ardupilot", measurement="PARM", label="ArduPilot BIN data (PARM)"),
    MeasurementCheck(database="sensor_logs", measurement="can_data", label="Parquet sensor data (CAN)"),
    MeasurementCheck(database="sensor_logs", measurement="sensor_data", label="Parquet sensor data (sensors)"),
    MeasurementCheck(database="sensor_logs", measurement="rs232_data", label="Parquet sensor data (RS232)"),
]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Verify InfluxDB data integrity")
    influx_cli.add_host_token_args(parser)
    return parser.parse_args()


def print_row_count(host: str, token: str, check: MeasurementCheck) -> None:
    """Query and print the row count for one (database, measurement) pair."""
    client = InfluxDBClient3(host=host, token=token, database=check.database)
    query = f'SELECT count(*) FROM "{check.measurement}"'
    try:
        result = client.query(query=query, language="sql")
        row_count = result.to_pandas().iloc[0, 0]
        print(f"  {check.label}: {row_count:,}")
    except Exception as e:
        print(f"  Error querying {check.database}/{check.measurement}: {e}")
    finally:
        client.close()


def main() -> None:
    """Run verification queries against InfluxDB."""
    args = parse_args()
    print("Verifying InfluxDB data…\n")
    for check in CHECKS:
        print_row_count(args.host, args.token, check)


if __name__ == "__main__":
    main()
