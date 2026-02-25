"""
SWORD Lint CLI

Command-line interface for running lint checks on SWORD DuckDB databases.

Usage:
    # Basic usage
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb

    # Filter by region
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --region NA

    # Specific checks
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --checks T001 T002 A001

    # By category
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --checks T  # all topology

    # Severity filter
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --severity error

    # Output formats
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --format json -o report.json

    # CI mode (exit code based on issues)
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --fail-on-error

    # Override thresholds
    python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --threshold A002 150
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from .core import Severity, get_registry
from .runner import LintRunner
from .formatters import ConsoleFormatter, JsonFormatter, MarkdownFormatter


def parse_threshold(s: str) -> Tuple[str, float]:
    """Parse threshold argument like 'A002 150' or 'A002=150'."""
    if "=" in s:
        parts = s.split("=", 1)
    else:
        parts = s.split(None, 1)

    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"Invalid threshold format: '{s}'. Use 'CHECK_ID VALUE' or 'CHECK_ID=VALUE'"
        )

    check_id, value = parts
    try:
        return check_id.upper(), float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid threshold value: '{value}'")


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        prog="sword-lint",
        description="Run lint checks on SWORD DuckDB databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --db sword_v17c.duckdb
  %(prog)s --db sword_v17c.duckdb --region NA
  %(prog)s --db sword_v17c.duckdb --checks T001 T002 A001
  %(prog)s --db sword_v17c.duckdb --checks T  # all topology
  %(prog)s --db sword_v17c.duckdb --severity error
  %(prog)s --db sword_v17c.duckdb --format json -o report.json
  %(prog)s --db sword_v17c.duckdb --fail-on-error
  %(prog)s --db sword_v17c.duckdb --threshold A002 150
        """,
    )

    # Required arguments
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SWORD DuckDB database",
    )

    # Filtering
    parser.add_argument(
        "--region",
        "-r",
        help="Filter by region (e.g., NA, SA, EU, AF, AS, OC)",
    )
    parser.add_argument(
        "--checks",
        "-c",
        nargs="+",
        help="Check IDs or category prefixes to run (e.g., T001 T002, or T for all topology)",
    )
    parser.add_argument(
        "--severity",
        "-s",
        choices=["error", "warning", "info"],
        help="Filter by minimum severity",
    )

    # Output
    parser.add_argument(
        "--format",
        "-f",
        choices=["console", "json", "markdown"],
        default="console",
        help="Output format (default: console)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed issue information",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colored output",
    )

    # CI mode
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with code 2 if any ERROR-level checks fail",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Exit with code 1 if any WARNING-level checks fail",
    )

    # Thresholds
    parser.add_argument(
        "--threshold",
        "-t",
        nargs=2,
        action="append",
        metavar=("CHECK_ID", "VALUE"),
        help="Override threshold for a check (can be repeated)",
    )

    # Info
    parser.add_argument(
        "--list-checks",
        action="store_true",
        help="List all available checks and exit",
    )

    return parser


def list_checks():
    """Print list of all available checks."""
    registry = get_registry()

    print("\nAvailable SWORD Lint Checks:")
    print("=" * 70)

    current_category = None
    for check_id in sorted(registry.keys()):
        spec = registry[check_id]

        # Category header (FL is a two-char prefix)
        category = check_id[:2] if check_id.startswith("FL") else check_id[0]
        if category != current_category:
            current_category = category
            cat_name = {
                "T": "TOPOLOGY",
                "A": "ATTRIBUTES",
                "G": "GEOMETRY",
                "C": "CLASSIFICATION",
                "V": "V17C",
                "F": "FACC",
                "FL": "FLAGS",
                "N": "NETWORK",
                "O": "OBSTRUCTION",
            }.get(category, category)
            print(f"\n{cat_name}:")

        # Check info
        sev = spec.severity.value.upper()
        threshold = (
            f" (threshold: {spec.default_threshold})" if spec.default_threshold else ""
        )
        print(f"  {check_id}: {spec.name}")
        print(f"       [{sev}] {spec.description}{threshold}")

    print("\n" + "=" * 70)
    print(f"Total: {len(registry)} checks")
    print()


def main(args: Optional[List[str]] = None) -> int:
    """
    Main CLI entry point.

    Args:
        args: Command-line arguments (default: sys.argv[1:])

    Returns:
        Exit code (0=success, 1=warnings, 2=errors)
    """
    parser = create_parser()
    opts = parser.parse_args(args)

    # Handle --list-checks
    if opts.list_checks:
        list_checks()
        return 0

    # Validate database path
    db_path = Path(opts.db)
    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        return 2

    # Parse severity filter
    severity_filter = None
    if opts.severity:
        severity_filter = Severity(opts.severity)

    # Initialize runner
    try:
        runner = LintRunner(db_path)
    except Exception as e:
        print(f"Error opening database: {e}", file=sys.stderr)
        return 2

    # Apply threshold overrides
    if opts.threshold:
        for check_id, value in opts.threshold:
            runner.set_threshold(check_id.upper(), value)

    # Run checks
    try:
        results = runner.run(
            checks=opts.checks,
            region=opts.region,
            severity=severity_filter,
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        runner.close()
        return 2
    except Exception as e:
        print(f"Error running checks: {e}", file=sys.stderr)
        runner.close()
        return 2
    finally:
        runner.close()

    # Format output
    if opts.format == "json":
        formatter = JsonFormatter(include_details=opts.verbose)
        output_text = formatter.format(
            results,
            db_path=str(db_path),
            region=opts.region,
        )
    elif opts.format == "markdown":
        formatter = MarkdownFormatter(include_details=opts.verbose)
        output_text = formatter.format(
            results,
            db_path=str(db_path),
            region=opts.region,
        )
    else:  # console
        formatter = ConsoleFormatter(
            use_color=not opts.no_color,
            verbose=opts.verbose,
        )
        output_text = formatter.format(results)

    # Write output
    if opts.output:
        output_path = Path(opts.output)
        output_path.write_text(output_text)
        print(f"Report written to: {output_path}")
    else:
        print(output_text)

    # Determine exit code
    errors = sum(1 for r in results if not r.passed and r.severity == Severity.ERROR)
    warnings = sum(
        1 for r in results if not r.passed and r.severity == Severity.WARNING
    )

    if opts.fail_on_error and errors > 0:
        return 2
    if opts.fail_on_warning and (errors > 0 or warnings > 0):
        return 1 if warnings > 0 and errors == 0 else 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
