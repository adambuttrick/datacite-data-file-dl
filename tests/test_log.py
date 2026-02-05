"""Tests for logging configuration."""

import logging

from datacite_data_file_dl.log import setup_logging, get_logger


def test_setup_logging_default_level():
    """Default logging level should be INFO."""
    setup_logging()
    logger = get_logger()
    assert logger.level == logging.INFO


def test_setup_logging_verbose():
    """Verbose mode should set DEBUG level."""
    setup_logging(verbose=True)
    logger = get_logger()
    assert logger.level == logging.DEBUG


def test_setup_logging_quiet():
    """Quiet mode should set WARNING level."""
    setup_logging(quiet=True)
    logger = get_logger()
    assert logger.level == logging.WARNING


def test_get_logger_returns_named_logger():
    """get_logger should return the datacite-data-file-dl logger."""
    logger = get_logger()
    assert logger.name == "datacite-data-file-dl"
