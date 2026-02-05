"""Checksum computation and verification."""

import hashlib
from pathlib import Path

from .log import get_logger


class ChecksumMismatch(Exception):
    """Raised when file checksum doesn't match expected value."""

    def __init__(self, path: Path, expected: str, actual: str):
        self.path = path
        self.expected = expected
        self.actual = actual
        super().__init__(f"Checksum mismatch for {path}: expected {expected}, got {actual}")


def compute_md5(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Compute MD5 hash of a file.

    Args:
        path: Path to file
        chunk_size: Size of chunks to read (default 8MB)

    Returns:
        Hex-encoded MD5 hash
    """
    md5 = hashlib.md5()

    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)

    return md5.hexdigest()


def verify_checksum(path: Path, expected_etag: str) -> None:
    """Verify file checksum against S3 ETag.

    Args:
        path: Path to file
        expected_etag: S3 ETag value (may include quotes, may be multipart)

    Raises:
        ChecksumMismatch: If checksum doesn't match

    Note:
        Multipart upload ETags (containing '-') cannot be verified as they
        use a different algorithm. These are silently skipped.
    """
    logger = get_logger()

    # Strip surrounding quotes from ETag
    expected = expected_etag.strip('"')

    # Multipart uploads have ETags like "hash-partcount"
    # These can't be verified with simple MD5
    if "-" in expected:
        logger.debug(f"Skipping checksum for multipart upload: {path}")
        return

    actual = compute_md5(Path(path))

    if actual != expected:
        raise ChecksumMismatch(Path(path), expected, actual)

    logger.debug(f"Checksum verified: {path}")
