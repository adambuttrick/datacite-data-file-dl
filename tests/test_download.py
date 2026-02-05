"""Tests for download operations."""

from datacite_data_file_dl.download import (
    should_download_file,
    parse_size,
)
from datacite_data_file_dl.progress import ProgressTracker


class TestParseSize:
    """Test size parsing."""

    def test_parse_bytes(self):
        """Should parse plain bytes."""
        assert parse_size("1000") == 1000

    def test_parse_kb(self):
        """Should parse KB."""
        assert parse_size("10KB") == 10 * 1024

    def test_parse_mb(self):
        """Should parse MB."""
        assert parse_size("5MB") == 5 * 1024 * 1024

    def test_parse_gb(self):
        """Should parse GB."""
        assert parse_size("2GB") == 2 * 1024 * 1024 * 1024

    def test_parse_lowercase(self):
        """Should handle lowercase units."""
        assert parse_size("10mb") == 10 * 1024 * 1024

    def test_parse_with_space(self):
        """Should handle space between number and unit."""
        assert parse_size("10 MB") == 10 * 1024 * 1024


class TestShouldDownloadFile:
    """Test file filtering logic."""

    def test_already_complete(self, tmp_output_dir):
        """Should skip already completed files."""
        tracker = ProgressTracker(tmp_output_dir)
        tracker.mark_complete("test.json", size=100, checksum="abc")

        result = should_download_file(
            key="test.json",
            size=100,
            tracker=tracker,
        )
        assert result is False

    def test_include_pattern_match(self, tmp_output_dir):
        """Should include files matching include pattern."""
        tracker = ProgressTracker(tmp_output_dir)

        result = should_download_file(
            key="data/file.json",
            size=100,
            tracker=tracker,
            include_patterns=["*.json"],
        )
        assert result is True

    def test_include_pattern_no_match(self, tmp_output_dir):
        """Should exclude files not matching include pattern."""
        tracker = ProgressTracker(tmp_output_dir)

        result = should_download_file(
            key="data/file.csv",
            size=100,
            tracker=tracker,
            include_patterns=["*.json"],
        )
        assert result is False

    def test_exclude_pattern(self, tmp_output_dir):
        """Should exclude files matching exclude pattern."""
        tracker = ProgressTracker(tmp_output_dir)

        result = should_download_file(
            key="data/file.zip",
            size=100,
            tracker=tracker,
            exclude_patterns=["*.zip"],
        )
        assert result is False

    def test_max_size(self, tmp_output_dir):
        """Should exclude files over max size."""
        tracker = ProgressTracker(tmp_output_dir)

        result = should_download_file(
            key="large.json",
            size=1000,
            tracker=tracker,
            max_size=500,
        )
        assert result is False
