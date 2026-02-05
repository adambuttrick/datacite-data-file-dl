"""CLI integration tests that invoke main() directly."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest
from moto import mock_aws
import boto3

from datacite_data_file_dl.__main__ import main
from datacite_data_file_dl.download import BUCKET
from datacite_data_file_dl.exit_codes import ExitCode


def create_mock_auth_response():
    """Create a mock authentication API response."""
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_key_id": "test-key-id",
        "secret_access_key": "test-secret",
        "session_token": "test-token",
    }
    return mock_response


AUTH_PATCH_PATH = "datacite_data_file_dl.auth.requests.get"


class TestCLIAuthFailures:
    """Test authentication failure scenarios."""

    def test_missing_username(self, capsys, monkeypatch):
        """Should fail with AUTH_FAILURE when username is missing."""
        monkeypatch.delenv("DATACITE_USERNAME", raising=False)
        monkeypatch.delenv("DATACITE_PASSWORD", raising=False)

        monkeypatch.setattr(sys, "argv", ["datacite-data-file-dl", "--json"])

        result = main()

        assert result == ExitCode.AUTH_FAILURE
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output["status"] == "error"
        assert output["code"] == "AUTH_FAILED"

    def test_missing_password(self, capsys, monkeypatch):
        """Should fail with AUTH_FAILURE when password is missing."""
        monkeypatch.delenv("DATACITE_PASSWORD", raising=False)

        monkeypatch.setattr(
            sys, "argv", ["datacite-data-file-dl", "-u", "testuser", "--json"]
        )

        result = main()

        assert result == ExitCode.AUTH_FAILURE
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output["status"] == "error"

    def test_auth_api_failure(self, capsys, monkeypatch):
        """Should fail with AUTH_FAILURE when API returns error."""
        with patch(AUTH_PATCH_PATH) as mock_post:
            mock_response = MagicMock()
            mock_response.ok = False
            mock_response.status_code = 401
            mock_response.text = "Unauthorized"
            mock_post.return_value = mock_response

            monkeypatch.setattr(
                sys,
                "argv",
                ["datacite-data-file-dl", "-u", "bad", "-p", "creds", "--json", "--list"],
            )

            result = main()

            assert result == ExitCode.AUTH_FAILURE


class TestCLIListMode:
    """Test --list mode functionality."""

    def test_list_mode_json(self, capsys, monkeypatch):
        """Should list bucket contents in JSON format."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                # Set up S3
                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                client.put_object(Bucket=BUCKET, Key="MANIFEST", Body=b"content")
                client.put_object(
                    Bucket=BUCKET, Key="dois/updated_2024-01/file.json", Body=b"{}"
                )

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--list",
                        "--json",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                captured = capsys.readouterr()
                output = json.loads(captured.out)
                assert "folders" in output
                assert "files" in output
                assert "dois" in output["folders"]
                assert any(f["name"] == "MANIFEST" for f in output["files"])

    def test_list_mode_with_path(self, capsys, monkeypatch):
        """Should list contents under a specific path."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                client.put_object(
                    Bucket=BUCKET, Key="dois/updated_2024-01/file.json", Body=b"{}"
                )
                client.put_object(
                    Bucket=BUCKET, Key="dois/updated_2024-02/file.json", Body=b"{}"
                )

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--list",
                        "--path",
                        "dois",
                        "--json",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                captured = capsys.readouterr()
                output = json.loads(captured.out)
                assert "updated_2024-01" in output["folders"]
                assert "updated_2024-02" in output["folders"]


class TestCLIDryRun:
    """Test --dry-run functionality."""

    def test_dry_run_shows_files(self, capsys, monkeypatch):
        """Should show what would be downloaded without downloading."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                client.put_object(Bucket=BUCKET, Key="MANIFEST", Body=b"content")

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--all",
                        "--dry-run",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                captured = capsys.readouterr()
                assert "Would download: MANIFEST" in captured.out


class TestCLIDownload:
    """Test actual download functionality."""

    def test_download_single_file(self, tmp_path, capsys, monkeypatch):
        """Should download a single file successfully."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                client.put_object(Bucket=BUCKET, Key="test.txt", Body=b"test content")

                output_dir = tmp_path / "output"
                output_dir.mkdir()

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--all",
                        "-o",
                        str(output_dir),
                        "--json",
                        "-y",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                assert (output_dir / "test.txt").exists()
                assert (output_dir / "test.txt").read_text() == "test content"

                captured = capsys.readouterr()
                output = json.loads(captured.out)
                assert output["status"] == "success"
                assert len(output["files"]) == 1

    def test_download_with_path(self, tmp_path, capsys, monkeypatch):
        """Should download files under specific path only."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                client.put_object(Bucket=BUCKET, Key="MANIFEST", Body=b"manifest")
                client.put_object(
                    Bucket=BUCKET, Key="dois/updated_2024-01/file.json", Body=b"{}"
                )

                output_dir = tmp_path / "output"
                output_dir.mkdir()

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--path",
                        "dois/updated_2024-01",
                        "-o",
                        str(output_dir),
                        "--json",
                        "-y",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                # Only the file under the path should be downloaded
                assert (output_dir / "file.json").exists()
                # Root MANIFEST should not be downloaded
                assert not (output_dir / "MANIFEST").exists()

    def test_download_not_found(self, capsys, monkeypatch):
        """Should return NOT_FOUND when no files match."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                # Empty bucket

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--path",
                        "nonexistent/",
                        "--json",
                    ],
                )

                result = main()

                assert result == ExitCode.NOT_FOUND
                captured = capsys.readouterr()
                output = json.loads(captured.out)
                assert output["status"] == "error"
                assert output["code"] == "NOT_FOUND"


class TestCLIFiltering:
    """Test file filtering options."""

    def test_include_pattern(self, tmp_path, capsys, monkeypatch):
        """Should only download files matching include pattern."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                client.put_object(Bucket=BUCKET, Key="file1.json", Body=b"{}")
                client.put_object(Bucket=BUCKET, Key="file2.txt", Body=b"text")
                client.put_object(Bucket=BUCKET, Key="file3.json", Body=b"{}")

                output_dir = tmp_path / "output"
                output_dir.mkdir()

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--all",
                        "-o",
                        str(output_dir),
                        "--include",
                        "*.json",
                        "--json",
                        "-y",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                assert (output_dir / "file1.json").exists()
                assert (output_dir / "file3.json").exists()
                assert not (output_dir / "file2.txt").exists()

    def test_exclude_pattern(self, tmp_path, capsys, monkeypatch):
        """Should skip files matching exclude pattern."""
        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)
                client.put_object(Bucket=BUCKET, Key="keep.json", Body=b"{}")
                client.put_object(Bucket=BUCKET, Key="skip.tmp", Body=b"temp")

                output_dir = tmp_path / "output"
                output_dir.mkdir()

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--all",
                        "-o",
                        str(output_dir),
                        "--exclude",
                        "*.tmp",
                        "--json",
                        "-y",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                assert (output_dir / "keep.json").exists()
                assert not (output_dir / "skip.tmp").exists()


class TestCLIBucketConfig:
    """Test custom bucket configuration."""

    def test_custom_bucket(self, capsys, monkeypatch):
        """Should use custom bucket when specified."""
        custom_bucket = "my-custom-bucket"

        with mock_aws():
            with patch(AUTH_PATCH_PATH) as mock_post:
                mock_post.return_value = create_mock_auth_response()

                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=custom_bucket)
                client.put_object(
                    Bucket=custom_bucket, Key="custom-file.txt", Body=b"custom"
                )

                monkeypatch.setattr(
                    sys,
                    "argv",
                    [
                        "datacite-data-file-dl",
                        "-u",
                        "user",
                        "-p",
                        "pass",
                        "--bucket",
                        custom_bucket,
                        "--list",
                        "--json",
                    ],
                )

                result = main()

                assert result == ExitCode.SUCCESS
                captured = capsys.readouterr()
                output = json.loads(captured.out)
                assert any(f["name"] == "custom-file.txt" for f in output["files"])
