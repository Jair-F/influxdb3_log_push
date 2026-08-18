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
from dataclasses import dataclass
from pathlib import Path

from mcap.writer import Writer

# ---------------------------------------------------------------------------
# Simulation timing
# ---------------------------------------------------------------------------

DURATION_SEC = 1_800  # 30 minutes
TICK_HZ = 100  # Main loop frequency (= fastest channel, CAN)
TOTAL_STEPS = DURATION_SEC * TICK_HZ

SENSOR_PERIOD_STEPS = 10  # Sensors sample every 10th step -> 10 Hz
RS232_PERIOD_STEPS = 20  # RS232 fires every 20th step -> 5 Hz

# Fixed base: 2024-01-15 10:00:00 UTC -> nanoseconds since Unix epoch
START_TIME_NS = 1_705_312_800_000_000_000

# publish_time trails log_time by a small, deterministic amount that cycles
# every JITTER_CYCLE_STEPS steps, simulating realistic transport delay
# without introducing randomness.
JITTER_CYCLE_STEPS = 5
JITTER_STEP_NS = 1_000_000  # 1 ms
JITTER_MIN_NS = JITTER_STEP_NS

NUM_SENSORS = 30
NUM_CAN_IDS = 10
RS232_DEVICE_ID = 0x41
EMA_ALPHA = 0.1  # Exponential moving average smoothing factor

BYTE_MODULUS = 256  # Values wrap into a single byte: [0, 255]
BYTE_MASK = 0xFF
CAN_FRAME_LENGTH_BYTES = 8

PROGRESS_PRINT_INTERVAL_STEPS = TOTAL_STEPS // 10

# ---------------------------------------------------------------------------
# JSON schemas for the three domains
# ---------------------------------------------------------------------------

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
            "items": {"type": "integer", "minimum": 0, "maximum": BYTE_MASK},
            "minItems": CAN_FRAME_LENGTH_BYTES,
            "maxItems": CAN_FRAME_LENGTH_BYTES,
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
            "items": {"type": "integer", "minimum": 0, "maximum": BYTE_MASK},
        },
    },
    "required": ["id", "data"],
}


# ---------------------------------------------------------------------------
# CAN pattern generators — one function per CAN channel "personality"
# ---------------------------------------------------------------------------

# CAN 0x001: 16-bit counter + temperature ramp + status cycle
COUNTER_16BIT_MODULUS = 1 << 16
TEMP_BASE_C = 50
TEMP_AMPLITUDE_C = 50
TEMP_OSCILLATION_RATE = 0.1
STATUS_CYCLE_LENGTH = 4

# CAN 0x002: sinusoidal RPM (16-bit) + load percentage
RPM_BASE = 3_000
RPM_AMPLITUDE = 2_000
RPM_OSCILLATION_RATE = 0.5
RPM_MAX = 0xFFFF
LOAD_BASE_PERCENT = 50
LOAD_AMPLITUDE_PERCENT = 50
LOAD_OSCILLATION_RATE = 0.2

# CAN 0x003-0x00A: sawtooth + triangle + stepped counter + flag toggle
SAWTOOTH_FREQ_SCALE = 10
TRIANGLE_FREQ_SCALE = 5
TRIANGLE_PERIOD = 512
DATA3_INDEX_SCALE = 10
FLAG_TOGGLE_HIGH = 0xAA
FLAG_TOGGLE_LOW = 0x55


def _can_counter_temp_status(step: int, time_sec: float) -> list[int]:
    """CAN 0x001 pattern: 16-bit counter, a slow temperature ramp, and a status cycle."""
    data = [0] * CAN_FRAME_LENGTH_BYTES
    counter = step % COUNTER_16BIT_MODULUS
    temp = int(TEMP_BASE_C + TEMP_AMPLITUDE_C * math.sin(time_sec * TEMP_OSCILLATION_RATE)) % BYTE_MODULUS
    data[0] = (counter >> 8) & BYTE_MASK
    data[1] = counter & BYTE_MASK
    data[2] = temp
    data[4] = step % STATUS_CYCLE_LENGTH
    return data


def _can_rpm_load(time_sec: float) -> list[int]:
    """CAN 0x002 pattern: sinusoidal RPM (clamped to 16 bits) and a load percentage."""
    data = [0] * CAN_FRAME_LENGTH_BYTES
    rpm = int(RPM_BASE + RPM_AMPLITUDE * math.sin(time_sec * RPM_OSCILLATION_RATE))
    rpm = max(0, min(RPM_MAX, rpm))
    load = int(LOAD_BASE_PERCENT + LOAD_AMPLITUDE_PERCENT * math.sin(time_sec * LOAD_OSCILLATION_RATE)) % BYTE_MODULUS
    data[0] = (rpm >> 8) & BYTE_MASK
    data[1] = rpm & BYTE_MASK
    data[2] = load
    return data


def _can_sawtooth_pattern(can_index: int, step: int, time_sec: float) -> list[int]:
    """CAN 0x003-0x00A pattern: sawtooth + triangle + stepped counter + alternating flag."""
    data = [0] * CAN_FRAME_LENGTH_BYTES
    sawtooth = time_sec * (can_index * SAWTOOTH_FREQ_SCALE)
    triangle = abs((time_sec * (can_index * TRIANGLE_FREQ_SCALE)) % TRIANGLE_PERIOD - TRIANGLE_PERIOD // 2)
    data[0] = int(sawtooth % BYTE_MODULUS)
    data[1] = int(triangle) % BYTE_MODULUS
    data[2] = (step // (can_index + 1)) % BYTE_MODULUS
    data[3] = can_index * DATA3_INDEX_SCALE
    data[4] = FLAG_TOGGLE_HIGH if step % 2 == 0 else FLAG_TOGGLE_LOW
    return data


def generate_can_data(can_index: int, step: int, time_sec: float) -> list[int]:
    """
    Generate 8 bytes of deterministic CAN data for one channel.

    Each CAN ID has its own pattern (see the three helpers above) so
    channels are visually distinguishable when plotted.

    Args:
        can_index: 0-based index of the CAN channel (0 = 0x001).
        step:      Current time-step counter.
        time_sec:  Current time in seconds since start.

    Returns:
        List of 8 integers in [0, 255].
    """
    if can_index == 0:
        return _can_counter_temp_status(step, time_sec)
    if can_index == 1:
        return _can_rpm_load(time_sec)
    return _can_sawtooth_pattern(can_index, step, time_sec)


# ---------------------------------------------------------------------------
# Sensor pattern generator
# ---------------------------------------------------------------------------

SENSOR_FREQ_MIN_HZ = 0.01
SENSOR_FREQ_RANGE_HZ = 0.49
SENSOR_AMPLITUDE_BASE = 10
SENSOR_AMPLITUDE_RANGE = 90
SENSOR_DC_OFFSET_RANGE = 500.0
SENSOR_DRIFT_AMPLITUDE = 20.0
SENSOR_DRIFT_RATE = 0.001


def generate_sensor_reading(
    sensor_index: int,
    time_sec: float,
    filtered_state: float,
) -> tuple[float, float]:
    """
    Generate raw and filtered sensor readings.

    Each sensor has a unique sine frequency / amplitude / DC offset.
    A slow drift simulates environmental change. The filtered value
    is an exponential moving average (α = 0.1) of the raw value.

    Args:
        sensor_index:   0-based sensor number.
        time_sec:       Current time in seconds since start.
        filtered_state: Previous filtered value (EMA state).

    Returns:
        (value_raw, value_filtered)
    """
    sensor_fraction = sensor_index / NUM_SENSORS
    freq = SENSOR_FREQ_MIN_HZ + sensor_fraction * SENSOR_FREQ_RANGE_HZ
    amplitude = SENSOR_AMPLITUDE_BASE + sensor_fraction * SENSOR_AMPLITUDE_RANGE
    dc_offset = sensor_index * (SENSOR_DC_OFFSET_RANGE / NUM_SENSORS)
    drift = SENSOR_DRIFT_AMPLITUDE * math.sin(time_sec * SENSOR_DRIFT_RATE)

    raw = dc_offset + drift + amplitude * math.sin(2 * math.pi * freq * time_sec)

    if time_sec == 0:
        filtered = raw
    else:
        filtered = EMA_ALPHA * raw + (1 - EMA_ALPHA) * filtered_state

    return raw, filtered


# ---------------------------------------------------------------------------
# RS232 pattern generator
# ---------------------------------------------------------------------------

RS232_PAYLOAD_LENGTH = 4


def generate_rs232_message(step: int) -> dict:
    """
    Generate an RS232 serial-protocol message.

    Simulates rotating command bytes with a 4-byte payload and checksum.

    Args:
        step: Current time-step counter.

    Returns:
        Dict with 'id' (always 0x41) and 'data' (list of bytes).
    """
    seq = step // RS232_PERIOD_STEPS
    cmd = seq % BYTE_MODULUS
    payload = [(seq + offset) % BYTE_MODULUS for offset in range(RS232_PAYLOAD_LENGTH)]
    checksum = (cmd + sum(payload)) % BYTE_MODULUS
    return {"id": RS232_DEVICE_ID, "data": [cmd] + payload + [checksum]}


# ---------------------------------------------------------------------------
# MCAP writing
# ---------------------------------------------------------------------------


@dataclass
class SensorState:
    """Channel ids for each sensor, paired with the running EMA filter state."""

    channel_ids: list[int]
    filtered_values: list[float]


@dataclass
class SimChannels:
    """Every registered MCAP channel, grouped by domain."""

    writer: Writer
    sensors: SensorState
    can_ids: list[int]
    rs232_id: int


@dataclass(frozen=True)
class StepTiming:
    """Timing for one simulation step, computed once and shared by all three domains."""

    step: int
    time_sec: float
    log_time_ns: int
    publish_time_ns: int


def _jitter_ns(step: int) -> int:
    """Deterministic publish-time jitter, cycling every JITTER_CYCLE_STEPS steps."""
    return JITTER_MIN_NS + (step % JITTER_CYCLE_STEPS) * JITTER_STEP_NS


def _step_timing(step: int) -> StepTiming:
    """Compute the log/publish timestamps for one simulation step."""
    time_sec = step * (1.0 / TICK_HZ)
    log_time_ns = START_TIME_NS + int(time_sec * 1e9)
    publish_time_ns = log_time_ns - _jitter_ns(step)
    return StepTiming(step=step, time_sec=time_sec, log_time_ns=log_time_ns, publish_time_ns=publish_time_ns)


def _register_schemas(writer: Writer) -> dict[str, int]:
    """Register the three JSON schemas and return their ids by domain name."""
    return {
        "sensor": writer.register_schema(
            name="sensor_schema", encoding="jsonschema", data=json.dumps(SENSOR_SCHEMA).encode()
        ),
        "can": writer.register_schema(
            name="can_schema", encoding="jsonschema", data=json.dumps(CAN_SCHEMA).encode()
        ),
        "rs232": writer.register_schema(
            name="rs232_schema", encoding="jsonschema", data=json.dumps(RS232_SCHEMA).encode()
        ),
    }


def _register_channels(writer: Writer, schema_ids: dict[str, int]) -> SimChannels:
    """Register one MCAP channel per sensor and CAN id, plus the RS232 channel."""
    sensor_channel_ids = [
        writer.register_channel(topic=f"sensor/sensor{i}", message_encoding="json", schema_id=schema_ids["sensor"])
        for i in range(1, NUM_SENSORS + 1)
    ]
    can_channel_ids = [
        writer.register_channel(topic=f"can/0x{i:03X}", message_encoding="json", schema_id=schema_ids["can"])
        for i in range(1, NUM_CAN_IDS + 1)
    ]
    rs232_channel_id = writer.register_channel(
        topic=f"rs232/0x{RS232_DEVICE_ID:02X}", message_encoding="json", schema_id=schema_ids["rs232"]
    )
    sensors = SensorState(channel_ids=sensor_channel_ids, filtered_values=[0.0] * NUM_SENSORS)
    return SimChannels(writer=writer, sensors=sensors, can_ids=can_channel_ids, rs232_id=rs232_channel_id)


def _write_can_messages(channels: SimChannels, timing: StepTiming) -> None:
    """Write one CAN frame for every channel at this step (100 Hz)."""
    for can_index, channel_id in enumerate(channels.can_ids):
        data = generate_can_data(can_index, timing.step, timing.time_sec)
        channels.writer.add_message(
            channel_id=channel_id,
            log_time=timing.log_time_ns,
            data=json.dumps({"data": data}).encode(),
            publish_time=timing.publish_time_ns,
        )


def _write_sensor_messages(channels: SimChannels, timing: StepTiming) -> None:
    """Write one reading per sensor (10 Hz), updating the EMA state in place."""
    sensors = channels.sensors
    for sensor_index, channel_id in enumerate(sensors.channel_ids):
        raw, filtered = generate_sensor_reading(sensor_index, timing.time_sec, sensors.filtered_values[sensor_index])
        sensors.filtered_values[sensor_index] = filtered
        channels.writer.add_message(
            channel_id=channel_id,
            log_time=timing.log_time_ns,
            data=json.dumps({"value_raw": raw, "value_filtered": filtered}).encode(),
            publish_time=timing.publish_time_ns,
        )


def _write_rs232_message(channels: SimChannels, timing: StepTiming) -> None:
    """Write one RS232 message (5 Hz)."""
    msg = generate_rs232_message(timing.step)
    channels.writer.add_message(
        channel_id=channels.rs232_id,
        log_time=timing.log_time_ns,
        data=json.dumps(msg).encode(),
        publish_time=timing.publish_time_ns,
    )


def _maybe_print_progress(step: int) -> None:
    """Print progress every PROGRESS_PRINT_INTERVAL_STEPS steps."""
    if step % PROGRESS_PRINT_INTERVAL_STEPS == 0:
        print(f"  Progress: {step * 100 // TOTAL_STEPS}%")


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

        schema_ids = _register_schemas(writer)
        channels = _register_channels(writer, schema_ids)

        for step in range(TOTAL_STEPS):
            _maybe_print_progress(step)
            timing = _step_timing(step)

            _write_can_messages(channels, timing)
            if step % SENSOR_PERIOD_STEPS == 0:
                _write_sensor_messages(channels, timing)
            if step % RS232_PERIOD_STEPS == 0:
                _write_rs232_message(channels, timing)

        writer.finish()
        print("  Progress: 100%")
        print(f"Finished writing {output_file}")


if __name__ == "__main__":
    main()