#!/usr/bin/env python
"""
Parse ArduPilot DataFlash logs and write to InfluxDB 3 or files focused on
accelerometer (IMU), vibration (VIBE), magnetometer (MAG), battery (BAT),
and motor output (RCOU) records.

Usage (InfluxDB mode):
  python scripts/dataflash_to_influx.py \
    --input-dir input-files/ \
    --influx-url http://localhost:8181 \
    --influx-token supersecrettoken

Usage (File mode):
  python scripts/dataflash_to_influx.py \
    --input-dir input-files/ \
    --output-dir output-files/

Requirements: pymavlink, influxdb3-python (optional), deep-translator (already present in the repo's venv).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional

from deep_translator import GoogleTranslator
from pymavlink import DFReader

try:
    from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision, InfluxDBError, write_client_options, WriteOptions
    INFLUXDB_AVAILABLE = True
except ImportError:
    INFLUXDB_AVAILABLE = False


def _hebrew_to_english(text: str) -> str:
    """Convert Hebrew text to English using Google Translate."""
    try:
        # Check if text contains Hebrew characters
        if any('\u0590' <= char <= '\u05FF' for char in text):
            translator = GoogleTranslator(source='hebrew', target='english')
            translated = translator.translate(text)
            return translated
        else:
            # Not Hebrew, return as-is
            return text
    except Exception as e:
        # Fallback: if translation fails, return original text
        print(f"Warning: Translation failed for '{text}': {e}", file=sys.stderr)
        return text


def _generate_bucket_name(filepath: Path) -> str:
    """Generate bucket name from filepath using last 3 directories, converting Hebrew to English."""
    parts = []
    current = filepath.parent
    
    # Collect last 3 directory names
    for _ in range(3):
        if current == current.parent:  # Reached root
            break
        parts.insert(0, current.name)
        current = current.parent
    
    # If we have fewer than 3 directories, use what we have
    if not parts:
        parts = [filepath.stem]
    
    # Convert Hebrew to English and join with underscores
    translated = [_hebrew_to_english(part) for part in parts]
    bucket_name = "_".join(translated)
    bucket_name += "_" + filepath.stem
    
    # Sanitize bucket name (InfluxDB bucket names should be alphanumeric, hyphens, underscores)
    bucket_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in bucket_name).lower()
    
    return bucket_name or "default_bucket"


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


def _create_point(measurement: str, tags: Dict[str, object], fields: Dict[str, object], ts_us: int, bucket_name: str) -> Optional[Point]:
    """Create an InfluxDB Point or None if no fields."""
    if not INFLUXDB_AVAILABLE:
        raise RuntimeError("influxdb3-python not available")
    
    clean_fields = {k: v for k, v in fields.items() if v is not None}
    if not clean_fields:
        return None
    
    point = Point(measurement)
    
    # Add bucket name as a tag
    point = point.tag("bucket", bucket_name)
    
    # Add tags
    for k, v in tags.items():
        if v is not None:
            point = point.tag(k, str(v))
    
    # Add fields
    for k, v in clean_fields.items():
        if isinstance(v, bool):
            point = point.field(k, v)
        elif isinstance(v, int):
            point = point.field(k, v)
        elif isinstance(v, float):
            point = point.field(k, v)
        else:
            point = point.field(k, str(v))
    
    # Set timestamp (convert microseconds to nanoseconds)
    point = point.time(ts_us, write_precision=WritePrecision.US)  # Convert microseconds to nanoseconds
    
    return point


def generate_lines(path: str) -> Iterable[str]:
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

        tags = {}
        if data.get("imu") or data.get("instance") is not None:
            tags["imu"] = data.get("imu")
            tags["instance"] = data.get("instance")
        line = _format_line(mtype, tags, data, time_us)
        if line:
            yield line


def generate_points(path: str, bucket_name: str) -> Iterable[Point]:
    """Yield InfluxDB Points for IMU, VIBE, MAG, BAT, and RCOU records.

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

        tags = {}
        if data.get("imu") or data.get("instance") is not None:
            tags["imu"] = data.get("imu")
            tags["instance"] = data.get("instance")
        line = _create_point(mtype, tags, data, time_us, bucket_name)
        if line:
            yield line


def find_bin_files(directory: str) -> list[Path]:
    """Find all .bin and .BIN files recursively in the given directory."""
    dir_path = Path(directory)
    if not dir_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    
    # Find both .bin and .BIN files, avoiding duplicates
    bin_files = set(dir_path.rglob("*.bin"))
    bin_files.update(dir_path.rglob("*.BIN"))
    
    return sorted(bin_files)


def success(self, data: str):
    print(f"Successfully wrote batch: data: {data}")

def error(self, data: str, exception: InfluxDBError):
    print(f"Failed writing batch: config: {self}, data: {data} due: {exception}")

def retry(self, data: str, exception: InfluxDBError):
    print(f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}")

def process_file_to_influx(filepath: Path, influx_url: str, influx_token: str, org: str = "mav-org") -> None:
    """Process a single .bin file and write to InfluxDB 3."""
    if not INFLUXDB_AVAILABLE:
        raise RuntimeError("influxdb3-python not available. Install with: pip install influxdb3-python")
    
    # Fixed database name
    database_name = "AeroSentinal"
    # Generate bucket name for tagging points
    bucket_name = _generate_bucket_name(filepath)
    
    print(f"Processing: {filepath}")
    print(f"Database name: {database_name}")
    print(f"Bucket tag: {bucket_name}")
    
    write_options = WriteOptions(batch_size=1000, flush_interval=1000, jitter_interval=0, retry_interval=5000, max_retries=5, max_retry_delay=125000, exponential_base=2)

    client_options = write_client_options(
                          error_callback=error,
                          retry_callback=retry,
                          write_options=write_options,
                          write_precision=WritePrecision.US)
    
    # Create InfluxDB 3 client with fixed database name
    # Note: InfluxDB 3 uses 'database' instead of 'bucket' and 'org'
    client = InfluxDBClient3(host=influx_url, token=influx_token, database=database_name, write_client_options=client_options)
    
    try:
        # Generate points and write in batches
        batch = []
        batch_size = 1000
        point_count = 0
        
        for point in generate_points(str(filepath), bucket_name):
            batch.append(point)
            point_count += 1
            
            if len(batch) >= batch_size:
                client.write(batch, write_precision='us')
                batch = []
        
        # Write remaining points
        if batch:
            client.write(batch, write_precision='us')
        
        print(f"Successfully wrote {point_count} points to database '{database_name}' with bucket tag '{bucket_name}'")
        
    except Exception as e:
        print(f"Error processing {filepath}: {e}", file=sys.stderr)
        raise
    finally:
        client.close()


def process_file_to_file(filepath: Path, output_dir: Path) -> None:
    """Process a single .bin file and write to a file."""
    bucket_name = _generate_bucket_name(filepath)
    output_filename = f"{bucket_name}.lp"
    output_path = output_dir / output_filename
    
    print(f"Processing: {filepath}")
    print(f"Output file: {output_path}")
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        line_count = 0
        with open(output_path, "w", encoding="utf-8") as out_fh:
            for line in generate_lines(str(filepath)):
                out_fh.write(line + "\n")
                line_count += 1
        
        print(f"Successfully wrote {line_count} lines to '{output_path}'")
        
    except Exception as e:
        print(f"Error processing {filepath}: {e}", file=sys.stderr)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert DataFlash logs to InfluxDB 3 or files (IMU, VIBE, MAG, BAT, RCOU). "
                    "Processes all .bin/.BIN files in a directory recursively."
    )
    parser.add_argument(
        "--input-dir", "-i",
        required=True,
        help="Directory containing DataFlash .bin or .BIN files (searched recursively)"
    )
    parser.add_argument(
        "--influx-url",
        help="InfluxDB 3 URL (e.g., http://localhost:8181). If not provided, writes to files."
    )
    parser.add_argument(
        "--influx-token",
        help="InfluxDB authentication token (required if --influx-url is provided)"
    )
    parser.add_argument(
        "--org",
        default="mav-org",
        help="InfluxDB organization name (default: mav-org, legacy parameter - not used in InfluxDB 3)"
    )
    parser.add_argument(
        "--output-dir", "-o",
        help="Output directory for .lp files (required if --influx-url is not provided)"
    )
    args = parser.parse_args()

    # Determine mode
    use_influx = args.influx_url is not None
    
    if use_influx:
        if not args.influx_token:
            print("Error: --influx-token is required when --influx-url is provided", file=sys.stderr)
            return 1
        if not INFLUXDB_AVAILABLE:
            print("Error: influxdb-client not available. Install with: pip install influxdb-client", file=sys.stderr)
            return 1
    else:
        if not args.output_dir:
            print("Error: --output-dir is required when --influx-url is not provided", file=sys.stderr)
            return 1

    try:
        bin_files = list(find_bin_files(args.input_dir))
        if not bin_files:
            print(f"No .bin or .BIN files found in {args.input_dir}", file=sys.stderr)
            return 1
        
        print(f"Found {len(bin_files)} .bin/.BIN file(s)")
        print(f"Mode: {'InfluxDB' if use_influx else 'File output'}")
        
        for filepath in bin_files:
            try:
                if use_influx:
                    process_file_to_influx(filepath, args.influx_url, args.influx_token, args.org)
                else:
                    process_file_to_file(filepath, Path(args.output_dir))
            except Exception as e:
                print(f"Failed to process {filepath}: {e}", file=sys.stderr)
                # Continue with next file
                continue
        
        print(f"\nCompleted processing {len(bin_files)} file(s)")
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

