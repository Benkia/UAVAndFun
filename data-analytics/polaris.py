#!/usr/bin/env python
"""
Query InfluxDB 3 using Polars based on analysis parameters from analysis.json.

This script reads analysis parameters from analysis.json and queries InfluxDB
to perform various checks on UAV flight data including:
- Simple checks (GPS, PM, CURR, BAT, POWR, RCOU, VIBE, XKF4, ESC, MAG)
- Calculated checks (e.g., TotalMagField)

Usage:
    python data-analytics/polaris.py
    
    # With custom connection details
    python data-analytics/polaris.py \
        --url http://localhost:8181 \
        --token supersecrettoken \
        --database AeroSentinal \
        --time-range -24h
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from influxdb_client_3 import InfluxDBClient3
    INFLUXDB_AVAILABLE = True
except ImportError:
    INFLUXDB_AVAILABLE = False
    print("Error: influxdb3-python not available. Install with: pip install influxdb3-python", file=sys.stderr)
    sys.exit(1)

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print("Error: polars not available. Install with: pip install polars", file=sys.stderr)
    sys.exit(1)


def load_analysis_config(config_path: Path) -> Dict[str, Any]:
    """Load analysis parameters from JSON file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {config_path}: {e}", file=sys.stderr)
        sys.exit(1)


def map_field_name(message_type: str, field: str, instance: Optional[int]) -> str:
    """Map field names to match actual database schema.
    
    Some fields in analysis.json use generic names that need to be mapped
    to instance-specific field names in the database.
    """
    # VIBE.Clip maps to Clip0, Clip1, Clip2 based on instance
    if message_type == "VIBE" and field == "Clip" and instance is not None:
        return f"Clip{instance}"
    
    return field


def build_simple_query(
    message_type: str,
    field: str,
    instance: Optional[int],
    time_range: str,
    limit: Optional[int] = None
) -> str:
    """Build SQL query for a simple check."""
    # Map field name to actual database field name
    mapped_field = map_field_name(message_type, field, instance)
    
    # Build WHERE clause
    # In InfluxDB 3, instance is not a field in the schema for most measurements
    # We'll skip instance filtering for now (it may be encoded differently or not used)
    conditions = []
    
    # Note: instance filtering is disabled as it's not a field in InfluxDB 3 schema
    # if instance is not None:
    #     conditions.append(f"instance = {instance}")
    
    # Time range condition
    if time_range.startswith("-"):
        duration = time_range[1:]
        conditions.append(f"time >= NOW() - INTERVAL '{duration}'")
    elif time_range.startswith("now()") or time_range.startswith("NOW()"):
        conditions.append(f"time >= {time_range}")
    else:
        conditions.append(f"time >= '{time_range}'")
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # InfluxDB 3: _measurement and instance are not fields in the schema
    # Only select actual fields: time, bucket, and the field value
    # Quote field name to preserve case sensitivity
    query = f"""
    SELECT time, bucket, "{mapped_field}" as value
    FROM "{message_type}"
    WHERE {where_clause}
    ORDER BY time DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    return query


def build_calculated_query(
    required_fields: List[Dict[str, Any]],
    time_range: str,
    limit: Optional[int] = None
) -> str:
    """Build SQL query for a calculated check that requires multiple fields."""
    # For calculated checks, all fields should be from the same measurement/instance
    # We'll query all fields together
    if not required_fields:
        return ""
    
    # Check if all fields are from the same measurement/instance
    first_field = required_fields[0]
    msg_type = first_field["message_type"]
    instance = first_field.get("instance")
    
    # Build field list - quote field names to preserve case sensitivity
    # Map field names to match actual database schema
    mapped_fields = []
    for field_spec in required_fields:
        field = field_spec["field"]
        instance = field_spec.get("instance")
        msg_type = field_spec["message_type"]
        mapped_field = map_field_name(msg_type, field, instance)
        mapped_fields.append(f'"{mapped_field}"')
    field_list = ", ".join(mapped_fields)
    
    # In InfluxDB 3, instance is not a field in the schema for most measurements
    # We'll skip instance filtering for now (it may be encoded differently or not used)
    conditions = []
    
    # Note: instance filtering is disabled as it's not a field in InfluxDB 3 schema
    # if instance is not None:
    #     conditions.append(f"instance = {instance}")
    
    # Time range condition
    if time_range.startswith("-"):
        duration = time_range[1:]
        conditions.append(f"time >= NOW() - INTERVAL '{duration}'")
    elif time_range.startswith("now()") or time_range.startswith("NOW()"):
        conditions.append(f"time >= {time_range}")
    else:
        conditions.append(f"time >= '{time_range}'")
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # InfluxDB 3: _measurement and instance are not fields in the schema
    # Only select actual fields: time, bucket, and the required fields
    # Quote field names to preserve case sensitivity
    query = f"""
    SELECT time, bucket, {field_list}
    FROM "{msg_type}"
    WHERE {where_clause}
    ORDER BY time DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    return query


def query_simple_check(
    client: InfluxDBClient3,
    database: str,
    check_config: Dict[str, Any],
    time_range: str,
    limit: Optional[int] = None
) -> Optional[pl.DataFrame]:
    """Query InfluxDB for a simple check."""
    message_type = check_config["message_type"]
    field = check_config["field"]
    instance = check_config.get("instance")
    
    query = build_simple_query(message_type, field, instance, time_range, limit)
    
    try:
        df = client.query(
            query=query,
            database=database,
            language="sql",
            mode="polars"
        )
        return df
    except Exception as e:
        error_str = str(e)
        # Check if it's a "table not found" error - this is expected for missing measurements
        if "table" in error_str.lower() and "not found" in error_str.lower():
            # Silently handle missing tables - they're expected if data wasn't imported
            return None
        # Check if it's a "field not found" error - this is expected for missing fields
        if ("no field named" in error_str.lower() or "schema error" in error_str.lower()) and "valid fields" in error_str.lower():
            # Silently handle missing fields - they're expected if schema doesn't match
            return None
        # For other errors, print a shorter message
        print(f"  Error querying {message_type}.{field}: {error_str[:200]}", file=sys.stderr)
        return None


def query_calculated_check(
    client: InfluxDBClient3,
    database: str,
    check_config: Dict[str, Any],
    time_range: str,
    limit: Optional[int] = None
) -> Optional[pl.DataFrame]:
    """Query InfluxDB for a calculated check."""
    required_fields = check_config["required_fields"]
    
    query = build_calculated_query(required_fields, time_range, limit)
    
    try:
        df = client.query(
            query=query,
            database=database,
            language="sql",
            mode="polars"
        )
        return df
    except Exception as e:
        error_str = str(e)
        # Check if it's a "table not found" error - this is expected for missing measurements
        if "table" in error_str.lower() and "not found" in error_str.lower():
            # Silently handle missing tables - they're expected if data wasn't imported
            return None
        # Check if it's a "field not found" error - this is expected for missing fields
        if ("no field named" in error_str.lower() or "schema error" in error_str.lower()) and "valid fields" in error_str.lower():
            # Silently handle missing fields - they're expected if schema doesn't match
            return None
        # For other errors, print a shorter message
        analysis_name = check_config.get('analysis_name', 'unknown')
        print(f"  Error querying calculated check {analysis_name}: {error_str[:200]}", file=sys.stderr)
        return None


def evaluate_simple_check(
    df: pl.DataFrame,
    check_config: Dict[str, Any]
) -> pl.DataFrame:
    """Evaluate a simple check against min/max values."""
    if df is None or df.is_empty():
        return pl.DataFrame()
    
    min_value = check_config.get("min_value")
    max_value = check_config.get("max_value")
    
    # Add violation flags
    result = df.clone()
    
    if min_value is not None:
        result = result.with_columns(
            pl.when(pl.col("value") < min_value)
            .then(1)
            .otherwise(0)
            .alias("low_violation")
        )
    else:
        result = result.with_columns(pl.lit(0).alias("low_violation"))
    
    if max_value is not None:
        result = result.with_columns(
            pl.when(pl.col("value") > max_value)
            .then(1)
            .otherwise(0)
            .alias("high_violation")
        )
    else:
        result = result.with_columns(pl.lit(0).alias("high_violation"))
    
    # Add state flag (violation if either low or high)
    result = result.with_columns(
        ((pl.col("low_violation") == 1) | (pl.col("high_violation") == 1))
        .cast(pl.Int32)
        .alias("state")
    )
    
    # Add alert name
    result = result.with_columns(
        pl.when(pl.col("low_violation") == 1)
        .then(pl.lit(check_config.get("alert_name_low", "")))
        .when(pl.col("high_violation") == 1)
        .then(pl.lit(check_config.get("alert_name_high", "")))
        .otherwise(pl.lit(""))
        .alias("alert_name")
    )
    
    return result


def evaluate_calculated_check(
    df: pl.DataFrame,
    check_config: Dict[str, Any]
) -> pl.DataFrame:
    """Evaluate a calculated check using the expression."""
    if df is None or df.is_empty():
        return pl.DataFrame()
    
    required_fields = check_config["required_fields"]
    expression = check_config["expression"]
    min_value = check_config.get("min_value")
    max_value = check_config.get("max_value")
    
    # Create column name mapping for the expression
    # Expression uses format: MESSAGE_TYPE_INSTANCE_FIELD
    # But our query returns just the field names
    result = df.clone()
    
    # Map field names to expression column names
    column_mapping = {}
    for field_spec in required_fields:
        msg_type = field_spec["message_type"]
        instance = field_spec.get("instance")
        field = field_spec["field"]
        
        # Expression format: MAG_0_MagX
        expr_col_name = f"{msg_type}_{instance}_{field}" if instance is not None else f"{msg_type}_{field}"
        column_mapping[field] = expr_col_name
        
        # Rename columns to match expression format
        if field in result.columns:
            result = result.rename({field: expr_col_name})
    
    # Evaluate the expression using Polars
    # Replace the expression column names with actual column references
    # This is a simplified version - for full expression parsing, you'd need a proper parser
    try:
        # For the specific case of sqrt(MAG_0_MagX**2 + MAG_0_MagY**2 + MAG_0_MagZ**2)
        # We'll handle it directly
        if "sqrt" in expression and "**2" in expression:
            # Extract field names from expression
            # This is a simple parser for the specific format
            import re
            # Find all column references in the expression
            col_refs = re.findall(r'([A-Z_]+_\d+_[A-Za-z]+|[A-Z_]+_[A-Za-z]+)', expression)
            
            if col_refs:
                # Calculate the expression value
                # For sqrt(x**2 + y**2 + z**2), we can use Polars directly
                # Assuming the columns are renamed to match expression format
                expr_parts = []
                for col_ref in col_refs:
                    if col_ref in result.columns:
                        expr_parts.append(f"pl.col('{col_ref}').pow(2)")
                
                if len(expr_parts) >= 2:
                    # Build the expression: sqrt(sum of squares)
                    sum_expr = " + ".join(expr_parts)
                    # Use Polars to evaluate
                    # For now, let's calculate manually for the known pattern
                    if len(expr_parts) == 3:
                        # Three components (x, y, z)
                        cols = [col_refs[0], col_refs[1], col_refs[2]]
                        if all(col in result.columns for col in cols):
                            result = result.with_columns(
                                (
                                    pl.col(cols[0]).pow(2) + 
                                    pl.col(cols[1]).pow(2) + 
                                    pl.col(cols[2]).pow(2)
                                ).sqrt().alias("calculated_value")
                            )
        
        # If we couldn't parse, try to use the expression as-is with column mapping
        if "calculated_value" not in result.columns:
            # Fallback: try to evaluate using Python eval (not recommended for production)
            # For now, just add a placeholder
            result = result.with_columns(pl.lit(None).alias("calculated_value"))
    except Exception as e:
        print(f"    Warning: Could not evaluate expression '{expression}': {e}")
        result = result.with_columns(pl.lit(None).alias("calculated_value"))
    
    # Now evaluate min/max violations on calculated_value
    if "calculated_value" in result.columns and result["calculated_value"].null_count() < len(result):
        if min_value is not None:
            result = result.with_columns(
                pl.when(pl.col("calculated_value") < min_value)
                .then(1)
                .otherwise(0)
                .alias("low_violation")
            )
        else:
            result = result.with_columns(pl.lit(0).alias("low_violation"))
        
        if max_value is not None:
            result = result.with_columns(
                pl.when(pl.col("calculated_value") > max_value)
                .then(1)
                .otherwise(0)
                .alias("high_violation")
            )
        else:
            result = result.with_columns(pl.lit(0).alias("high_violation"))
        
        # Add state flag
        result = result.with_columns(
            ((pl.col("low_violation") == 1) | (pl.col("high_violation") == 1))
            .cast(pl.Int32)
            .alias("state")
        )
        
        # Add alert name
        result = result.with_columns(
            pl.when(pl.col("low_violation") == 1)
            .then(pl.lit(check_config.get("alert_name_low", "")))
            .when(pl.col("high_violation") == 1)
            .then(pl.lit(check_config.get("alert_name_high", "")))
            .otherwise(pl.lit(""))
            .alias("alert_name")
        )
    else:
        # No calculated value, add empty violation columns
        result = result.with_columns([
            pl.lit(0).alias("low_violation"),
            pl.lit(0).alias("high_violation"),
            pl.lit(0).alias("state"),
            pl.lit("").alias("alert_name")
        ])
    
    return result


def process_analysis_checks(
    client: InfluxDBClient3,
    database: str,
    analysis_config: Dict[str, Any],
    time_range: str,
    limit: Optional[int] = None,
    show_details: bool = False
) -> None:
    """Process all analysis checks from the configuration."""
    checks = analysis_config.get("analysis_parameters", [])
    
    print(f"Processing {len(checks)} analysis checks...")
    print(f"Time range: {time_range}")
    if limit:
        print(f"Limit: {limit} records per check")
    print("=" * 80)
    
    results_summary = []
    
    for i, check in enumerate(checks, 1):
        check_type = check.get("check_type", "simple")
        message_type = check.get("message_type", "UNKNOWN")
        field = check.get("field", "UNKNOWN")
        analysis_name = check.get("analysis_name", f"{message_type}.{field}")
        
        print(f"\n[{i}/{len(checks)}] {analysis_name} ({check_type})")
        print("-" * 80)
        
        if check_type == "simple":
            df = query_simple_check(client, database, check, time_range, limit)
            if df is not None and not df.is_empty():
                evaluated = evaluate_simple_check(df, check)
                
                # Count violations
                total_records = len(evaluated)
                violations = evaluated.filter(pl.col("state") == 1)
                violation_count = len(violations)
                
                print(f"  Total records: {total_records}")
                print(f"  Violations: {violation_count}")
                
                if violation_count > 0:
                    print(f"  Alert names: {violations['alert_name'].unique().to_list()}")
                    
                    if show_details:
                        print("\n  Violation details:")
                        print(str(violations.head(10)))
                
                results_summary.append({
                    "check": analysis_name,
                    "type": check_type,
                    "total": total_records,
                    "violations": violation_count,
                    "status": "VIOLATION" if violation_count > 0 else "OK"
                })
            else:
                print("  No data found")
                results_summary.append({
                    "check": analysis_name,
                    "type": check_type,
                    "total": 0,
                    "violations": 0,
                    "status": "NO_DATA"
                })
        
        elif check_type == "calculated":
            df = query_calculated_check(client, database, check, time_range, limit)
            if df is not None and not df.is_empty():
                evaluated = evaluate_calculated_check(df, check)
                
                # Count violations
                total_records = len(evaluated)
                violations = evaluated.filter(pl.col("state") == 1) if "state" in evaluated.columns else pl.DataFrame()
                violation_count = len(violations) if not violations.is_empty() else 0
                
                print(f"  Total records: {total_records}")
                print(f"  Violations: {violation_count}")
                
                if violation_count > 0:
                    if "alert_name" in evaluated.columns:
                        alert_names = violations["alert_name"].unique().to_list()
                        print(f"  Alert names: {alert_names}")
                    
                    if show_details:
                        print("\n  Violation details:")
                        print(str(violations.head(10)))
                
                if show_details and "calculated_value" in evaluated.columns:
                    print("\n  Calculated values sample:")
                    sample = evaluated.select(["time", "calculated_value"]).head(5)
                    print(str(sample))
                
                results_summary.append({
                    "check": analysis_name,
                    "type": check_type,
                    "total": total_records,
                    "violations": violation_count,
                    "status": "VIOLATION" if violation_count > 0 else "OK"
                })
            else:
                print("  No data found")
                results_summary.append({
                    "check": analysis_name,
                    "type": check_type,
                    "total": 0,
                    "violations": 0,
                    "status": "NO_DATA"
                })
        else:
            print(f"  Unknown check type: {check_type}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    summary_df = pl.DataFrame(results_summary)
    # Polars DataFrame can be printed directly or converted to string
    print(str(summary_df))
    
    # Count by status
    if not summary_df.is_empty():
        status_counts = summary_df.group_by("status").agg(pl.count().alias("count"))
        print("\nStatus counts:")
        print(str(status_counts))


def main() -> int:
    # Preprocess sys.argv to handle negative time-range values
    # argparse treats values starting with '-' as potential flags
    # We'll convert --time-range -2y to --time-range=-2y format
    processed_argv = []
    i = 0
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--time-range' and i + 1 < len(sys.argv):
            next_arg = sys.argv[i + 1]
            # If next argument starts with '-' but isn't a flag (doesn't start with '--')
            # and looks like a time range value, combine them
            if next_arg.startswith('-') and not next_arg.startswith('--') and len(next_arg) > 1:
                # Check if it looks like a time range (ends with h, d, y, m, s, or is a timestamp)
                if any(next_arg.endswith(suffix) for suffix in ['h', 'd', 'y', 'm', 's', 'w']) or 'T' in next_arg:
                    processed_argv.append(f'--time-range={next_arg}')
                    i += 2
                    continue
        processed_argv.append(arg)
        i += 1
    
    # Replace sys.argv with processed version
    original_argv = sys.argv[:]  # Make a copy
    sys.argv = processed_argv
    
    try:
        parser = argparse.ArgumentParser(
            description="Query InfluxDB 3 using Polars based on analysis parameters"
        )
        parser.add_argument(
            "--url",
            default="http://localhost:8181",
            help="InfluxDB URL (default: http://localhost:8181)"
        )
        parser.add_argument(
            "--token",
            default="supersecrettoken",
            help="InfluxDB authentication token (default: supersecrettoken)"
        )
        parser.add_argument(
            "--database",
            default="AeroSentinal",
            help="InfluxDB database name (default: AeroSentinal)"
        )
        parser.add_argument(
            "--config",
            type=Path,
            default=Path(__file__).parent / "analysis.json",
            help="Path to analysis.json configuration file"
        )
        parser.add_argument(
            "--time-range",
            default="-24h",
            type=str,
            metavar="RANGE",
            help="Time range for query (default: -24h, examples: -1h, -7d, -2y, 2024-01-01T00:00:00Z). "
                 "For negative values like -2y, use quotes: --time-range='-2y' or use equals: --time-range=-2y"
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit number of records returned per query"
        )
        parser.add_argument(
            "--details",
            action="store_true",
            help="Show detailed violation information"
        )
        
        args = parser.parse_args()
        
        if not INFLUXDB_AVAILABLE:
            print("Error: influxdb3-python not available. Install with: pip install influxdb3-python", file=sys.stderr)
            return 1
        
        if not POLARS_AVAILABLE:
            print("Error: polars not available. Install with: pip install polars", file=sys.stderr)
            return 1
        
        # Load analysis configuration
        analysis_config = load_analysis_config(args.config)
        # Create InfluxDB client
        client = InfluxDBClient3(
            host=args.url,
            token=args.token,
            database=args.database
        )
        
        print(f"Connected to InfluxDB at {args.url}")
        print(f"Database: {args.database}")
        print(f"Configuration: {args.config}")
        print("=" * 80)
        
        # Process analysis checks
        process_analysis_checks(
            client=client,
            database=args.database,
            analysis_config=analysis_config,
            time_range=args.time_range,
            limit=args.limit,
            show_details=args.details
        )
        
        # Close client
        client.close()
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Restore original sys.argv
        sys.argv = original_argv


if __name__ == "__main__":
    sys.exit(main())

