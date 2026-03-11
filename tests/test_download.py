"""Tests for download operations."""

import pytest
from botocore.exceptions import ClientError

from datacite_data_file_dl.download import (
    get_manifest_metadata,
    get_status_json,
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


class TestGetManifestMetadata:
    """Test MANIFEST metadata retrieval."""

    def test_returns_last_modified(self, populated_s3):
        """Should return LastModified datetime from MANIFEST."""
        result = get_manifest_metadata(populated_s3)
        assert result is not None
        assert hasattr(result, "year")

    def test_missing_manifest(self, mock_s3):
        """Should raise ClientError when MANIFEST doesn't exist."""
        with pytest.raises(ClientError) as exc_info:
            get_manifest_metadata(mock_s3)
        assert exc_info.value.response["Error"]["Code"] in ("404", "NoSuchKey")


class TestGetStatusJson:
    """Test STATUS.json retrieval."""

    def test_returns_parsed_json(self, populated_s3):
        """Should return parsed STATUS.json contents."""
        result = get_status_json(populated_s3)
        assert result["month"] == "2024-01"
        assert result["status"] == "Complete"

    def test_missing_status_json(self, mock_s3):
        """Should raise ClientError when STATUS.json doesn't exist."""
        with pytest.raises(ClientError) as exc_info:
            get_status_json(mock_s3)
        assert exc_info.value.response["Error"]["Code"] in ("404", "NoSuchKey")
