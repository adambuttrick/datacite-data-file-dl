"""Tests for checksum verification."""

import hashlib

import pytest

from datacite_data_file_dl.checksum import verify_checksum, compute_md5, ChecksumMismatch


class TestComputeMd5:
    """Test MD5 computation."""

    def test_compute_md5(self, tmp_path):
        """Should compute correct MD5 hash."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"hello world")

        expected = hashlib.md5(b"hello world").hexdigest()
        actual = compute_md5(test_file)

        assert actual == expected

    def test_compute_md5_large_file(self, tmp_path):
        """Should handle large files efficiently."""
        test_file = tmp_path / "large.bin"
        # Create 10MB file
        data = b"x" * (10 * 1024 * 1024)
        test_file.write_bytes(data)

        expected = hashlib.md5(data).hexdigest()
        actual = compute_md5(test_file)

        assert actual == expected


class TestVerifyChecksum:
    """Test checksum verification."""

    def test_verify_success(self, tmp_path):
        """Should pass when checksum matches."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"hello world")

        expected = hashlib.md5(b"hello world").hexdigest()
        # S3 ETag format: "hash" (with quotes) or just hash

        # Should not raise
        verify_checksum(test_file, expected)

    def test_verify_with_quoted_etag(self, tmp_path):
        """Should handle S3 ETags with surrounding quotes."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"hello world")

        expected = hashlib.md5(b"hello world").hexdigest()
        etag = f'"{expected}"'

        verify_checksum(test_file, etag)

    def test_verify_failure(self, tmp_path):
        """Should raise ChecksumMismatch on mismatch."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"hello world")

        with pytest.raises(ChecksumMismatch) as exc_info:
            verify_checksum(test_file, "wrong_checksum")

        assert "wrong_checksum" in str(exc_info.value)

    def test_skip_multipart_etag(self, tmp_path):
        """Should skip verification for multipart upload ETags."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"hello world")

        # Multipart ETags have format: hash-partcount
        multipart_etag = "abc123-5"

        # Should not raise, just skip verification
        verify_checksum(test_file, multipart_etag)
