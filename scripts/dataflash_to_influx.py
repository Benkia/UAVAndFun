#!/usr/bin/env python
"""
Parse ArduPilot DataFlash logs and emit InfluxDB line protocol focused on
accelerometer (IMU), vibration (VIBE), magnetometer (MAG), battery (BAT),
and motor output (RCOU) records.

Usage:
  python scripts/dataflash_to_influx.py --input input-files/log.bin --output out.lp
  python scripts/dataflash_to_influx.py --input input-files/log.bin --output - \
    | docker exec -i mav-influx influx write \
        --db sample-bucket --file -

Requirements: pymavlink (already present in the repo's venv).
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Dict, Iterable, Optional

from pymavlink import DFReader


def _format_line(measurement: str, tags: Dict[str, object], fields: Dict[str, object], ts_us: int) -> Optional[str]:
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
    return f"{measurement}{',' + tag_str if tag_str else ''} {field_str} {ts_us}"


def generate_lines(path: str, precision: str = "ns") -> Iterable[str]:
    """Yield line protocol strings for IMU, VIBE, MAG, BAT, and RCOU records.

    DataFlash TimeUS is microseconds since boot. To keep writes in a sane
    time range (and within retention), we anchor the first TimeUS to the
    current wall-clock and preserve relative offsets.
    """
    reader = DFReader.DFReader_binary(path)
    timebase = reader.clock.timebase * 1000000
    
    while True:
        msg = reader.recv_msg()
        if msg is None:
            break
        mtype = msg.get_type()
        data = msg.to_dict()

        if "TimeUS" not in data:
            continue
        time_us = int(timebase + data["TimeUS"])


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
            line = _format_line("imu", tags, fields, time_us)
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
            line = _format_line("vibe", tags, fields, time_us)
            if line:
                yield line

        elif mtype == "MAG":
            tags = {"instance": data.get("I")}
            fields = {
                "MagX": data.get("MagX"),
                "MagY": data.get("MagY"),
                "MagZ": data.get("MagZ"),
            }
            line = _format_line("mag", tags, fields, time_us)
            if line:
                yield line

        elif mtype == "BAT":
            tags = {"instance": data.get("I")}
            fields = {
                "Volt": data.get("Volt"),
                "Curr": data.get("Curr"),
                "Res": data.get("Res"),
            }
            line = _format_line("bat", tags, fields, time_us)
            if line:
                yield line

        elif mtype == "RCOU":
            tags = {}
            fields = {
                "C1": data.get("C1"),
                "C2": data.get("C2"),
                "C3": data.get("C3"),
                "C4": data.get("C4"),
                "C5": data.get("C5"),
                "C6": data.get("C6"),
                "C7": data.get("C7"),
                "C8": data.get("C8"),
                "C9": data.get("C9"),
                "C10": data.get("C10"),
                "C11": data.get("C11"),
                "C12": data.get("C12"),
                "C13": data.get("C13"),
                "C14": data.get("C14"),
            }
            line = _format_line("rcou", tags, fields, time_us)
            if line:
                yield line


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert DataFlash log to InfluxDB line protocol (IMU, VIBE, MAG, BAT, RCOU).")
    parser.add_argument("--input", "-i", required=True, help="Path to DataFlash .bin or .log")
    parser.add_argument("--output", "-o", default="-", help="Output file path or '-' for stdout")
    args = parser.parse_args()

    out_fh = sys.stdout if args.output == "-" else open(args.output, "w", encoding="utf-8")
    try:
        for line in generate_lines(args.input):
            out_fh.write(line + "\n")
    finally:
        if out_fh is not sys.stdout:
            out_fh.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

