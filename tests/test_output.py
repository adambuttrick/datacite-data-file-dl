"""Tests for output formatting."""

import json


from datetime import datetime, timezone

from datacite_data_file_dl.output import (
    format_status,
    format_success,
    format_error,
    format_list,
)


class TestFormatSuccess:
    """Test success output formatting."""

    def test_json_format(self):
        """Should output valid JSON."""
        result = format_success(
            files=[{"path": "test.json", "size": 100}],
            total_bytes=100,
            elapsed_seconds=1.5,
            json_output=True,
        )

        data = json.loads(result)
        assert data["status"] == "success"
        assert data["total_bytes"] == 100
        assert len(data["files"]) == 1

    def test_human_format(self):
        """Should output human-readable text."""
        result = format_success(
            files=[{"path": "test.json", "size": 100}],
            total_bytes=100,
            elapsed_seconds=1.5,
            json_output=False,
        )

        assert "Download complete" in result
        assert "1 downloaded" in result or "1 file" in result


class TestFormatError:
    """Test error output formatting."""

    def test_json_format(self):
        """Should output valid JSON error."""
        result = format_error(
            code="AUTH_FAILED",
            message="Invalid credentials",
            json_output=True,
        )

        data = json.loads(result)
        assert data["status"] == "error"
        assert data["code"] == "AUTH_FAILED"
        assert data["message"] == "Invalid credentials"

    def test_human_format(self):
        """Should output human-readable error."""
        result = format_error(
            code="AUTH_FAILED",
            message="Invalid credentials",
            json_output=False,
        )

        assert "Invalid credentials" in result


class TestFormatList:
    """Test list output formatting."""

    def test_json_format(self):
        """Should output valid JSON list."""
        result = format_list(
            folders=["folder1", "folder2"],
            files=[{"name": "file.json", "size": 100}],
            json_output=True,
        )

        data = json.loads(result)
        assert "folders" in data
        assert "files" in data
        assert len(data["folders"]) == 2

    def test_human_format(self):
        """Should output human-readable list."""
        result = format_list(
            folders=["folder1"],
            files=[{"name": "file.json", "size": 100}],
            json_output=False,
        )

        assert "folder1" in result
        assert "file.json" in result


class TestFormatStatus:
    """Test status output formatting."""

    def test_json_format_full(self):
        """Should output valid JSON with all fields."""
        ts = datetime(2026, 3, 1, 7, 53, 36, tzinfo=timezone.utc)
        status = {"month": "2026-02", "status": "Complete"}

        result = format_status(
            manifest_last_modified=ts,
            status_json=status,
            json_output=True,
        )

        data = json.loads(result)
        assert data["manifest_last_modified"] == "2026-03-01T07:53:36+00:00"
        assert data["status"]["month"] == "2026-02"
        assert data["status"]["status"] == "Complete"

    def test_json_format_missing_manifest(self):
        """Should handle missing MANIFEST in JSON mode."""
        status = {"month": "2026-02", "status": "Complete"}

        result = format_status(
            manifest_last_modified=None,
            status_json=status,
            json_output=True,
        )

        data = json.loads(result)
        assert data["manifest_last_modified"] is None
        assert data["status"]["month"] == "2026-02"

    def test_json_format_missing_status(self):
        """Should handle missing STATUS.json in JSON mode."""
        ts = datetime(2026, 3, 1, 7, 53, 36, tzinfo=timezone.utc)

        result = format_status(
            manifest_last_modified=ts,
            status_json=None,
            json_output=True,
        )

        data = json.loads(result)
        assert data["manifest_last_modified"] is not None
        assert data["status"] is None

    def test_human_format_full(self):
        """Should output human-readable status with all fields."""
        ts = datetime(2026, 3, 1, 7, 53, 36, tzinfo=timezone.utc)
        status = {"month": "2026-02", "status": "Complete"}

        result = format_status(
            manifest_last_modified=ts,
            status_json=status,
        )

        assert "Data file status:" in result
        assert "MANIFEST last modified:" in result
        assert "2026-02" in result
        assert "Complete" in result

    def test_human_format_missing_manifest(self):
        """Should show 'Not available' for missing MANIFEST."""
        result = format_status(
            manifest_last_modified=None,
            status_json={"month": "2026-02", "status": "Complete"},
        )

        assert "Not available" in result
        assert "2026-02" in result

    def test_human_format_missing_status(self):
        """Should show 'Not available' for missing STATUS.json."""
        ts = datetime(2026, 3, 1, 7, 53, 36, tzinfo=timezone.utc)

        result = format_status(
            manifest_last_modified=ts,
            status_json=None,
        )

        assert "MANIFEST last modified:" in result
        assert "STATUS.json:" in result
        assert "Not available" in result
