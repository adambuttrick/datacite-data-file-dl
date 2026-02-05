"""Tests for exit code constants."""

from datacite_data_file_dl.exit_codes import ExitCode


def test_exit_codes_are_distinct():
    """All exit codes should have unique values."""
    codes = [
        ExitCode.SUCCESS,
        ExitCode.AUTH_FAILURE,
        ExitCode.NETWORK_ERROR,
        ExitCode.NOT_FOUND,
        ExitCode.PARTIAL_FAILURE,
        ExitCode.USER_CANCELLED,
    ]
    assert len(codes) == len(set(codes))


def test_success_is_zero():
    """Success should be 0 per Unix convention."""
    assert ExitCode.SUCCESS == 0


def test_exit_codes_are_integers():
    """Exit codes should be usable as integers."""
    assert isinstance(ExitCode.SUCCESS.value, int)
    assert isinstance(ExitCode.AUTH_FAILURE.value, int)
