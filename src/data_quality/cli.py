"""Command-line interface for the Sentri data quality framework."""

import argparse
import sys
from pathlib import Path
from typing import Optional

from data_quality.connectors import CSVConnector, get_connector
from data_quality.core.config_loader import load_config
from data_quality.core.framework import DataQualityFramework
from data_quality.formatters import OutputManager
from data_quality.managers import CheckManager
from data_quality.utils.constants import CheckStatus
from data_quality.utils.logger import get_logger, setup_logging
from data_quality.version import __version__


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        prog="dq-check", description="Sentri - Run data quality checks"
    )

    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )

    parser.add_argument(
        "-c", "--config", required=True, help="Path to configuration YAML file"
    )

    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")

    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")

    parser.add_argument("-o", "--output", help="Output directory (overrides config)")

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--log-format",
        choices=["text", "json"],
        default="text",
        help="Log output format (default: text)",
    )

    parser.add_argument(
        "--exit-on-failure",
        action="store_true",
        help="Exit with code 1 if any checks fail",
    )

    parser.add_argument(
        "--exit-on-warning",
        action="store_true",
        help="Exit with code 1 if any checks warn",
    )

    return parser


def main(args: Optional[list] = None) -> int:
    """
    Main CLI entry point.

    Args:
        args: Command line arguments (uses sys.argv if None)

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    parser = create_parser()
    parsed_args = parser.parse_args(args)

    # Setup logging
    setup_logging(level=parsed_args.log_level, log_format=parsed_args.log_format)

    logger = get_logger("cli")
    logger.info(f"Sentri v{__version__}")

    try:
        # Load configuration
        logger.info(f"Loading configuration from {parsed_args.config}")
        config = load_config(parsed_args.config)

        # Get data connector
        connector_type = config.source.type
        connector_class = get_connector(connector_type)

        if connector_class is None:
            logger.error(f"Unknown connector type: {connector_type}")
            return 1

        # Initialize connector based on type
        if connector_type == "csv":
            connector = connector_class(
                file_path=config.source.csv.file_path,
                date_column=config.source.csv.date_column,
                encoding=config.source.csv.encoding,
                delimiter=config.source.csv.delimiter,
            )
        else:
            logger.error(
                f"Connector type '{connector_type}' not yet implemented in CLI"
            )
            return 1

        # Validate connection
        logger.info("Validating data source connection...")
        connector.validate_connection()

        # Get data
        logger.info(
            f"Retrieving data from {parsed_args.start_date} to {parsed_args.end_date}"
        )
        df = connector.get_data(
            start_date=parsed_args.start_date, end_date=parsed_args.end_date
        )

        logger.info(f"Retrieved {len(df)} rows, {len(df.columns)} columns")

        if len(df) == 0:
            logger.warning("No data retrieved for the specified date range")

        # Run checks
        logger.info("Running data quality checks...")
        metadata = {
            "dq_check_name": config.metadata.dq_check_name,
            "date_column": config.metadata.date_column,
            "id_column": config.metadata.id_column,
        }

        manager = CheckManager(df=df, metadata=metadata, checks_config=config.checks)

        results = manager.run_all_checks()

        # Display summary
        summary = results["summary"]
        logger.info("=" * 50)
        logger.info("DQ Check Summary")
        logger.info("=" * 50)
        logger.info(f"Total Checks: {summary['total']}")
        logger.info(f"Passed: {summary['passed']}")
        logger.info(f"Warnings: {summary['warnings']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Pass Rate: {summary['pass_rate']}%")

        # Generate outputs
        output_dir = parsed_args.output or config.output.destination
        output_mgr = OutputManager(
            formats=config.output.formats,
            destination=output_dir,
            file_prefix=config.output.file_prefix,
            include_timestamp=config.output.include_timestamp,
            include_check_name=config.output.include_check_name,
            pretty_print=config.output.pretty_print,
        )

        output_paths = output_mgr.generate_outputs(
            results, config.metadata.dq_check_name
        )

        logger.info("Output files generated:")
        for fmt, path in output_paths.items():
            if isinstance(path, str):
                logger.info(f"  {fmt}: {path}")

        # Determine exit code
        exit_code = 0

        if parsed_args.exit_on_failure and summary["failed"] > 0:
            logger.error(f"{summary['failed']} check(s) failed")
            exit_code = 1
        elif parsed_args.exit_on_warning and summary["warnings"] > 0:
            logger.warning(f"{summary['warnings']} check(s) have warnings")
            exit_code = 1

        # Close connector
        connector.close()

        return exit_code

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
