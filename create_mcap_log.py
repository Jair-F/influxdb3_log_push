#!/usr/bin/env python3
"""
Creates a 30-minute MCAP log file with simulated, pattern-based data.

Three data domains are generated:

* **Sensor** — 30 channels at 10 Hz (sine waves + EMA filter)
* **CAN**    — 10 channels at 100 Hz (counters, RPM, sawtooth, …)
* **RS232**  — 1 channel  at  5 Hz (serial protocol simulation)

All patterns are deterministic (no randomness) so the file is
reproducible across runs.
"""

import json
import math
from pathlib import Path

from mcap.writer import Writer

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DURATION_SEC = 1_800  # 30 minutes
TICK_HZ = 100  # Main loop frequency (= fastest channel, CAN)
TOTAL_STEPS = DURATION_SEC * TICK_HZ

# Fixed base: 2024-01-15 10:00:00 UTC  →  nanoseconds since Unix epoch
START_TIME_NS = 1_705_312_800_000_000_000

NUM_SENSORS = 30
NUM_CAN_IDS = 10
RS232_DEVICE_ID = 0x41
EMA_ALPHA = 0.1  # Exponential moving average smoothing factor

# JSON schemas for the three domains
SENSOR_SCHEMA = {
    "type": "object",
    "properties": {
        "value_raw": {"type": "number"},
        "value_filtered": {"type": "number"},
    },
    "required": ["value_raw", "value_filtered"],
}

CAN_SCHEMA = {
    "type": "object",
    "properties": {
        "data": {
            "type": "array",
            "items": {"type": "integer", "minimum": 0, "maximum": 255},
            "minItems": 8,
            "maxItems": 8,
        }
    },
    "required": ["data"],
}

RS232_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "data": {
            "type": "array",
            "items": {"type": "integer", "minimum": 0, "maximum": 255},
        },
    },
    "required": ["id", "data"],
}


# ---------------------------------------------------------------------------
# Pattern generators — one function per CAN ID for readability
# ---------------------------------------------------------------------------


def generate_can_data(can_index: int, step: int, time_sec: float) -> list[int]:
    """
    Generate 8 bytes of deterministic CAN data.

    Each CAN ID uses a different pattern so they are visually
    distinguishable when plotted.

    Args:
        can_index: 0-based index of the CAN channel (0 = 0x001).
        step:      Current time-step counter.
        time_sec:  Current time in seconds since start.

    Returns:
        List of 8 integers in [0, 255].
    """
    data = [0] * 8

    if can_index == 0:
        # 0x001: 16-bit counter + temperature ramp + status cycle
        counter = step % 65536
        temp = int(50 + 50 * math.sin(time_sec * 0.1)) % 256
        data[0] = (counter >> 8) & 0xFF
        data[1] = counter & 0xFF
        data[2] = temp
        data[4] = step % 4

    elif can_index == 1:
        # 0x002: sinusoidal RPM (16-bit) + load percentage
        rpm = int(3000 + 2000 * math.sin(time_sec * 0.5))
        rpm = max(0, min(65535, rpm))
        load = int(50 + 50 * math.sin(time_sec * 0.2)) % 256
        data[0] = (rpm >> 8) & 0xFF
        data[1] = rpm & 0xFF
        data[2] = load

    else:
        # 0x003–0x00A: sawtooth + triangle + stepped counter + flag toggle
        data[0] = int((time_sec * (can_index * 10)) % 256)
        data[1] = int(abs((time_sec * (can_index * 5)) % 512 - 256)) % 256
        data[2] = (step // (can_index + 1)) % 256
        data[3] = can_index * 10
        data[4] = 0xAA if step % 2 == 0 else 0x55

    return data


def generate_sensor_reading(
    sensor_index: int,
    time_sec: float,
    filtered_state: float,
) -> tuple[float, float]:
    """
    Generate raw and filtered sensor readings.

    Each sensor has a unique sine frequency / amplitude / DC offset.
    A slow drift simulates environmental change.  The filtered value
    is an exponential moving average (α = 0.1) of the raw value.

    Args:
        sensor_index:   0-based sensor number.
        time_sec:       Current time in seconds since start.
        filtered_state: Previous filtered value (EMA state).

    Returns:
        (value_raw, value_filtered)
    """
    freq = 0.01 + (sensor_index / NUM_SENSORS) * 0.49
    amplitude = 10 + (sensor_index / NUM_SENSORS) * 90
    dc_offset = sensor_index * (500.0 / NUM_SENSORS)
    drift = 20.0 * math.sin(time_sec * 0.001)

    raw = dc_offset + drift + amplitude * math.sin(2 * math.pi * freq * time_sec)

    if time_sec == 0:
        filtered = raw
    else:
        filtered = EMA_ALPHA * raw + (1 - EMA_ALPHA) * filtered_state

    return raw, filtered


def generate_rs232_message(step: int) -> dict:
    """
    Generate an RS232 serial-protocol message.

    Simulates rotating command bytes with a 4-byte payload and checksum.

    Args:
        step: Current time-step counter.

    Returns:
        Dict with 'id' (always 0x41) and 'data' (list of bytes).
    """
    seq = step // 20  # RS232 fires every 20 steps
    cmd = seq % 256
    payload = [(seq + j) % 256 for j in range(4)]
    checksum = (cmd + sum(payload)) % 256
    return {"id": RS232_DEVICE_ID, "data": [cmd] + payload + [checksum]}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Generate the MCAP file with all three domains."""
    output_file = Path(__file__).parent / "simulated_log.mcap"
    print(f"Writing MCAP file to {output_file}…")

    with open(output_file, "wb") as f:
        writer = Writer(f)
        writer.start()

        # --- Register schemas ---
        sensor_schema_id = writer.register_schema(
            name="sensor_schema",
            encoding="jsonschema",
            data=json.dumps(SENSOR_SCHEMA).encode(),
        )
        can_schema_id = writer.register_schema(
            name="can_schema",
            encoding="jsonschema",
            data=json.dumps(CAN_SCHEMA).encode(),
        )
        rs232_schema_id = writer.register_schema(
            name="rs232_schema",
            encoding="jsonschema",
            data=json.dumps(RS232_SCHEMA).encode(),
        )

        # --- Register channels ---
        sensor_channels = [
            writer.register_channel(
                topic=f"sensor/sensor{i}",
                message_encoding="json",
                schema_id=sensor_schema_id,
            )
            for i in range(1, NUM_SENSORS + 1)
        ]

        can_channels = [
            writer.register_channel(
                topic=f"can/0x{i:03X}",
                message_encoding="json",
                schema_id=can_schema_id,
            )
            for i in range(1, NUM_CAN_IDS + 1)
        ]

        rs232_channel = writer.register_channel(
            topic="rs232/0x41",
            message_encoding="json",
            schema_id=rs232_schema_id,
        )

        # EMA filter state for each sensor
        sensor_filtered = [0.0] * NUM_SENSORS

        # --- Main time-stepping loop ---
        for step in range(TOTAL_STEPS):
            if step % (TOTAL_STEPS // 10) == 0:
                print(f"  Progress: {step * 100 // TOTAL_STEPS}%")

            time_sec = step * (1.0 / TICK_HZ)
            log_time_ns = START_TIME_NS + int(time_sec * 1e9)

            # Deterministic jitter: 1–5 ms based on step
            jitter_ns = 1_000_000 + (step % 5) * 1_000_000
            publish_time_ns = log_time_ns - jitter_ns

            # --- CAN: 100 Hz (every step) ---
            for ci in range(NUM_CAN_IDS):
                data = generate_can_data(ci, step, time_sec)
                writer.add_message(
                    channel_id=can_channels[ci],
                    log_time=log_time_ns,
                    data=json.dumps({"data": data}).encode(),
                    publish_time=publish_time_ns,
                )

            # --- Sensor: 10 Hz (every 10th step) ---
            if step % 10 == 0:
                for si in range(NUM_SENSORS):
                    raw, filtered = generate_sensor_reading(
                        si, time_sec, sensor_filtered[si]
                    )
                    sensor_filtered[si] = filtered

                    writer.add_message(
                        channel_id=sensor_channels[si],
                        log_time=log_time_ns,
                        data=json.dumps(
                            {"value_raw": raw, "value_filtered": filtered}
                        ).encode(),
                        publish_time=publish_time_ns,
                    )

            # --- RS232: 5 Hz (every 20th step) ---
            if step % 20 == 0:
                msg = generate_rs232_message(step)
                writer.add_message(
                    channel_id=rs232_channel,
                    log_time=log_time_ns,
                    data=json.dumps(msg).encode(),
                    publish_time=publish_time_ns,
                )

        writer.finish()
        print("  Progress: 100%")
        print(f"Finished writing {output_file}")


if __name__ == "__main__":
    main()
