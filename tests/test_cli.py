"""Tests for CLI argument parsing."""

import pytest

from datacite_data_file_dl.cli import parse_args


class TestBasicArgs:
    """Test basic argument parsing."""

    def test_username_short_flag(self):
        """Should parse -u flag."""
        args = parse_args(["-u", "test-user", "-p", "test-pass"])
        assert args.username == "test-user"

    def test_username_long_flag(self):
        """Should parse --username flag."""
        args = parse_args(["--username", "test-user", "-p", "test-pass"])
        assert args.username == "test-user"

    def test_username_not_required(self):
        """Username should not be required (can come from env/config)."""
        args = parse_args([])
        assert args.username is None

    def test_password_not_required(self):
        """Password should not be required (can come from env/config/prompt)."""
        args = parse_args([])
        assert args.password is None


class TestOutputFlags:
    """Test output-related flags."""

    def test_version_flag(self):
        """Should have --version flag."""
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--version"])
        assert exc_info.value.code == 0

    def test_quiet_flag(self):
        """Should parse --quiet flag."""
        args = parse_args(["--quiet"])
        assert args.quiet is True

    def test_quiet_short_flag(self):
        """Should parse -q flag."""
        args = parse_args(["-q"])
        assert args.quiet is True

    def test_verbose_flag(self):
        """Should parse --verbose flag."""
        args = parse_args(["--verbose"])
        assert args.verbose is True

    def test_json_flag(self):
        """Should parse --json flag."""
        args = parse_args(["--json"])
        assert args.json is True

    def test_log_file_flag(self):
        """Should parse --log-file flag."""
        args = parse_args(["--log-file", "/tmp/test.log"])
        assert args.log_file == "/tmp/test.log"


class TestDownloadFlags:
    """Test download-related flags."""

    def test_list_flag(self):
        """Should parse --list flag."""
        args = parse_args(["--list"])
        assert args.list is True

    def test_dry_run_flag(self):
        """Should parse --dry-run flag."""
        args = parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_yes_flag(self):
        """Should parse --yes flag."""
        args = parse_args(["--yes"])
        assert args.yes is True


class TestReliabilityFlags:
    """Test reliability-related flags."""

    def test_retries_flag(self):
        """Should parse --retries flag."""
        args = parse_args(["--retries", "5"])
        assert args.retries == 5

    def test_retries_default(self):
        """Default retries should be 3."""
        args = parse_args([])
        assert args.retries == 3

    def test_refresh_interval_flag(self):
        """Should parse --refresh-interval flag."""
        args = parse_args(["--refresh-interval", "30"])
        assert args.refresh_interval == 30

    def test_refresh_interval_default(self):
        """Default refresh_interval should be None (handled by config)."""
        args = parse_args([])
        assert args.refresh_interval is None

    def test_resume_flag(self):
        """Should parse --resume flag."""
        args = parse_args(["--resume"])
        assert args.resume is True

    def test_fresh_flag(self):
        """Should parse --fresh flag."""
        args = parse_args(["--fresh"])
        assert args.fresh is True

    def test_skip_verify_flag(self):
        """Should parse --skip-verify flag."""
        args = parse_args(["--skip-verify"])
        assert args.skip_verify is True


class TestFilteringFlags:
    """Test filtering-related flags."""

    def test_include_flag(self):
        """Should parse --include flag."""
        args = parse_args(["--include", "*.json"])
        assert args.include == ["*.json"]

    def test_include_multiple(self):
        """Should allow multiple --include flags."""
        args = parse_args(["--include", "*.json", "--include", "*.csv"])
        assert args.include == ["*.json", "*.csv"]

    def test_exclude_flag(self):
        """Should parse --exclude flag."""
        args = parse_args(["--exclude", "*.zip"])
        assert args.exclude == ["*.zip"]

    def test_since_flag(self):
        """Should parse --since flag."""
        args = parse_args(["--since", "2024-01"])
        assert args.since == "2024-01"

    def test_until_flag(self):
        """Should parse --until flag."""
        args = parse_args(["--until", "2024-06"])
        assert args.until == "2024-06"

    def test_max_size_flag(self):
        """Should parse --max-size flag."""
        args = parse_args(["--max-size", "1GB"])
        assert args.max_size == "1GB"
