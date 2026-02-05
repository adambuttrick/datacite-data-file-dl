"""CLI argument parsing and configuration."""

import argparse
import os

from . import __version__


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="datacite-data-file-dl",
        description="Download DataCite monthly data files from S3",
        epilog="""
Examples:
  %(prog)s -u USER -p PASS                    # Interactive mode
  %(prog)s --path dois/updated_2024-01/       # Download specific folder
  %(prog)s --list --path dois/                # List contents of folder
  %(prog)s --all -o ./data                    # Download entire bucket
  %(prog)s --since 2024-01 --until 2024-06    # Download date range
  %(prog)s --all -w 8                         # Parallel download with 8 workers

Environment variables:
  DATACITE_USERNAME    Account ID (alternative to -u)
  DATACITE_PASSWORD    Account password (alternative to -p)

Config file: ~/.datacite-data-file-dl.toml or ~/.config/datacite-data-file-dl/config.toml
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Authentication
    auth_group = parser.add_argument_group("Authentication")
    auth_group.add_argument(
        "-u",
        "--username",
        default=None,
        help="DataCite account ID (or set DATACITE_USERNAME)",
    )
    auth_group.add_argument(
        "-p",
        "--password",
        default=None,
        help="DataCite account password (or set DATACITE_PASSWORD)",
    )

    # Output
    output_group = parser.add_argument_group("Output")
    output_group.add_argument(
        "-o",
        "--output",
        default=os.getcwd(),
        help="Output directory for downloaded files (default: current directory)",
    )
    output_group.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    output_group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress output, only show errors",
    )
    output_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show debug information",
    )
    output_group.add_argument(
        "--log-file",
        metavar="PATH",
        help="Write logs to file",
    )

    # Download
    download_group = parser.add_argument_group("Download")
    download_group.add_argument(
        "--path",
        help="S3 path to download directly (e.g., 'dois/updated_2024-01/')",
    )
    download_group.add_argument(
        "-a",
        "--all",
        action="store_true",
        dest="download_all",
        help="Download the entire bucket",
    )
    download_group.add_argument(
        "--list",
        action="store_true",
        help="List contents without downloading",
    )
    download_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without downloading",
    )

    # Filtering
    filter_group = parser.add_argument_group("Filtering")
    filter_group.add_argument(
        "--include",
        action="append",
        metavar="PATTERN",
        help="Only download files matching glob pattern (can repeat)",
    )
    filter_group.add_argument(
        "--exclude",
        action="append",
        metavar="PATTERN",
        help="Skip files matching glob pattern (can repeat)",
    )
    filter_group.add_argument(
        "--since",
        metavar="YYYY-MM",
        help="Only files from this month onward",
    )
    filter_group.add_argument(
        "--until",
        metavar="YYYY-MM",
        help="Only files up to this month",
    )
    filter_group.add_argument(
        "--max-size",
        metavar="SIZE",
        help="Skip files larger than SIZE (e.g., 1GB, 500MB)",
    )

    # Reliability
    reliability_group = parser.add_argument_group("Reliability")
    reliability_group.add_argument(
        "--retries",
        type=int,
        default=3,
        metavar="N",
        help="Number of retry attempts (default: 3)",
    )
    reliability_group.add_argument(
        "--refresh-interval",
        type=int,
        default=None,
        metavar="MINUTES",
        help="Refresh AWS credentials every N minutes (default: 20, env: DATACITE_REFRESH_INTERVAL)",
    )
    reliability_group.add_argument(
        "--resume",
        action="store_true",
        help="Continue previous interrupted download",
    )
    reliability_group.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore previous progress, start over",
    )
    reliability_group.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip checksum verification",
    )

    # Performance
    perf_group = parser.add_argument_group("Performance")
    perf_group.add_argument(
        "-w",
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel download workers (default: 4, max: 32)",
    )

    # Advanced
    advanced_group = parser.add_argument_group("Advanced")
    advanced_group.add_argument(
        "--bucket",
        metavar="NAME",
        help="S3 bucket name (default: monthly-datafile.datacite.org)",
    )

    # Other
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip confirmation prompts",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    return parser.parse_args(args)
