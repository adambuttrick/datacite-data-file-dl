"""Logging configuration for datacite-data-file-dl."""

import logging
import sys

LOGGER_NAME = "datacite-data-file-dl"

_logger: logging.Logger | None = None


def setup_logging(
    verbose: bool = False,
    quiet: bool = False,
    log_file: str | None = None,
) -> None:
    """Configure logging for the CLI."""
    global _logger

    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()

    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logger.setLevel(level)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Always log everything to file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    _logger = logger


def get_logger() -> logging.Logger:
    """Get the logger, initializing with defaults if setup_logging wasn't called."""
    global _logger
    if _logger is None:
        setup_logging()
    # _logger is guaranteed to be set after setup_logging()
    assert _logger is not None
    return _logger
