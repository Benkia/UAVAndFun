#!/usr/bin/env python
"""
Parse ArduPilot DataFlash logs and emit InfluxDB line protocol focused on
accelerometer (IMU) and vibration (VIBE) records.

Usage:
  python scripts/dataflash_to_influx.py --input input-files/log.bin --output out.lp
  python scripts/dataflash_to_influx.py --input input-files/log.bin --output - \
    | docker exec -i mav-influx influx write \
        --org mav-org --bucket sample-bucket --precision ns --file -

Requirements: pymavlink (already present in the repo's venv).
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Dict, Iterable, Optional

from pymavlink import DFReader


def _format_line(measurement: str, tags: Dict[str, object], fields: Dict[str, object], ts_ns: int) -> Optional[str]:
    """Return a line protocol string or None if no fields."""
    clean_fields = {k: v for k, v in fields.items() if v is not None}
    if not clean_fields:
        return None

    tag_str = ",".join(f"{k}={v}" for k, v in tags.items() if v is not None)
    field_parts = []
    for k, v in clean_fields.items():
        if isinstance(v, bool):
            field_parts.append(f"{k}={str(v).lower()}")
        elif isinstance(v, int):
            field_parts.append(f"{k}={v}i")
        elif isinstance(v, float):
            field_parts.append(f"{k}={v}")
        else:
            field_parts.append(f'{k}="{v}"')
    field_str = ",".join(field_parts)
    return f"{measurement}{',' + tag_str if tag_str else ''} {field_str} {ts_ns}"


def generate_lines(path: str, precision: str = "ns") -> Iterable[str]:
    """Yield line protocol strings for IMU and VIBE records.

    DataFlash TimeUS is microseconds since boot. To keep writes in a sane
    time range (and within retention), we anchor the first TimeUS to the
    current wall-clock and preserve relative offsets.
    """
    reader = DFReader.DFReader_binary(path)

    start_us = None
    base_ns = None

    while True:
        msg = reader.recv_msg()
        if msg is None:
            break
        mtype = msg.get_type()
        data = msg.to_dict()

        if "TimeUS" not in data:
            continue
        time_us = data["TimeUS"]
        if start_us is None:
            start_us = time_us
            base_ns = time.time_ns() - int(start_us * 1000)
        ts_ns = base_ns + int(time_us * 1000)

        # Adjust precision if desired (default ns). Influx write must use the
        # same precision flag.
        if precision == "ns":
            ts_out = ts_ns
        elif precision == "ms":
            ts_out = ts_ns // 1_000_000
        elif precision == "s":
            ts_out = ts_ns // 1_000_000_000
        else:
            raise ValueError(f"Unsupported precision: {precision}")

        if mtype == "IMU":
            tags = {"imu": data.get("I")}
            fields = {
                "AccX": data.get("AccX"),
                "AccY": data.get("AccY"),
                "AccZ": data.get("AccZ"),
                "GyrX": data.get("GyrX"),
                "GyrY": data.get("GyrY"),
                "GyrZ": data.get("GyrZ"),
                "TempC": data.get("T"),
            }
            line = _format_line("imu", tags, fields, ts_out)
            if line:
                yield line

        elif mtype == "VIBE":
            tags = {"imu": data.get("IMU")}
            fields = {
                "VibeX": data.get("VibeX"),
                "VibeY": data.get("VibeY"),
                "VibeZ": data.get("VibeZ"),
                "Clip": data.get("Clip"),
            }
            line = _format_line("vibe", tags, fields, ts_out)
            if line:
                yield line


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert DataFlash log to InfluxDB line protocol (IMU + VIBE).")
    parser.add_argument("--input", "-i", required=True, help="Path to DataFlash .bin or .log")
    parser.add_argument("--output", "-o", default="-", help="Output file path or '-' for stdout")
    parser.add_argument(
        "--precision",
        choices=["ns", "ms", "s"],
        default="ms",
        help="Timestamp precision for output; must match influx write --precision.",
    )
    args = parser.parse_args()

    out_fh = sys.stdout if args.output == "-" else open(args.output, "w", encoding="utf-8")
    try:
        for line in generate_lines(args.input, precision=args.precision):
            out_fh.write(line + "\n")
    finally:
        if out_fh is not sys.stdout:
            out_fh.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

