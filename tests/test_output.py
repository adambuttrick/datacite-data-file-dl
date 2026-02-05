"""Tests for output formatting."""

import json


from datacite_data_file_dl.output import (
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
