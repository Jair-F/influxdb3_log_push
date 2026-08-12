# InfluxDB 3 Telemetry Pipeline

This repository contains a full end-to-end data ingestion pipeline built around **InfluxDB 3 Core**. It is designed to handle high-frequency telemetry data, specifically ArduPilot `.BIN` logs and robotic `.MCAP` / `.parquet` datasets.

## Architecture & Techniques
- **InfluxDB 3 Core**: The high-performance time-series database. We are using the open-source Core edition via Docker.
- **InfluxDB 3 Explorer**: A web UI for querying and managing databases and tokens.
- **InfluxDB 3 MCP Server**: Provides a Model Context Protocol (MCP) server so that AI models (like OpenWebUI agents) can securely query the database.
- **Python Ingestion Scripts**: Uses `influxdb-client-3` and PyArrow to ingest massive amounts of data in chunks.
- **Wide Table Merging**: Parquet data is ingested into a flattened "wide table" schema. InfluxDB 3 handles missing columns natively.
- **Vehicle Tagging**: Every ingestion script supports a `--vehicle` tag to uniquely identify different agents (e.g., `--vehicle drone1`).

## Important Tricks & Limitations

### 1. Hardcoded Limits Unlocked
By default, InfluxDB 3 Core strictly limits databases (2), tables (250), columns per table (250), and the historical query window (72 hours) because it lacks a background compactor. We have **bypassed these limits** in `docker-compose.yml` by setting the following environment variables to `9999`:
- `INFLUXDB3_NUM_DATABASE_LIMIT`
- `INFLUXDB3_NUM_TABLE_LIMIT`
- `INFLUXDB3_NUM_TOTAL_COLUMNS_PER_TABLE_LIMIT`
- `INFLUXDB3_QUERY_FILE_LIMIT`

### 2. Token Generation
InfluxDB 3 Core **does not support pre-provisioning admin tokens** via environment variables (unlike v2). If you destroy the database volume (e.g., `docker compose down -v`), your token is permanently lost. You must generate a new one manually via the CLI (see setup instructions).

### 3. Timestamp Deduplication
ArduPilot MAVLink logs often record multiple events at the exact same microsecond. InfluxDB overwrites points with identical timestamps in the same table. To prevent data loss, `bin_to_influxdb.py` increments duplicate timestamps by exactly `1ns` using a `while` loop collision check.

---

## Setup & Running

### 1. Spin up the Database
Start the database, explorer, and MCP server in the background:
```bash
docker compose up -d
```

### 2. Generate the Admin Token
Once the core database is running, you must generate the initial admin token:
```bash
docker exec influxdb3-core influxdb3 create token --admin
```
> **⚠️ CRITICAL**: Copy the generated token immediately! It will only be shown once.

### 3. Update the Configuration
Open `docker-compose.yml` and paste the new token into the `INFLUX_DB_TOKEN` environment variable under the `influxdb3-mcp` service.

Then, restart the MCP server to apply the token:
```bash
docker compose up -d influxdb3-mcp
```

### 4. Setup Python Environment
Create a virtual environment and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Run the Ingestion Pipeline
*(Note: Replace `<YOUR_TOKEN>` with your generated admin token)*

**Generate mock data:**
```bash
python create_mcap_log.py
python mcap_to_parquet.py
```

**Ingest ArduPilot data:**
```bash
python bin_to_influxdb.py test_ardupilot.bin --database ardupilot_clean --vehicle drone1 --token <YOUR_TOKEN>
```

**Ingest Parquet data:**
```bash
python parquet_to_influxdb.py simulated_log.parquet --database sensor_logs_clean --measurement sensor_data_clean --vehicle drone1 --token <YOUR_TOKEN>
```

**Verify data counts:**
```bash
# Update DEFAULT_TOKEN in verify_data.py first, or pass it via CLI
python verify_data.py --token <YOUR_TOKEN>
```

---

## Web Interfaces

* **InfluxDB 3 Explorer**: [http://localhost:8888](http://localhost:8888)
* **InfluxDB 3 Database API**: [http://localhost:8181](http://localhost:8181)

---

## OpenWebUI & MCP Integration

The **InfluxDB 3 MCP Server** runs on port `8000` via Streamable HTTP. This allows OpenWebUI to connect to it and query your databases naturally via an LLM.

**To configure OpenWebUI:**
1. Open your OpenWebUI application.
2. Navigate to **Admin Settings > Integrations** or the **MCP Servers** section.
3. Click **Add Server**.
4. Configure the server with the following settings:
   - **Type**: `MCP (Streamable HTTP)` or `SSE (Server-Sent Events)` depending on the exact UI wording.
   - **URL**: `http://<YOUR_DOCKER_HOST_IP>:8000/mcp` *(Note: If OpenWebUI runs in Docker on the same network, use `http://influxdb3-mcp:8000/mcp`. If it runs locally on the host, use `http://localhost:8000/mcp`)*
5. Save the configuration.

Your LLM inside OpenWebUI will now have access to read-only tools to query the InfluxDB 3 tables and schemas directly!
