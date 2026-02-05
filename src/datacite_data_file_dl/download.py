"""S3 download operations."""

import fnmatch
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import boto3
from tqdm import tqdm

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from .auth import AWSCredentials, CredentialManager
from .checksum import verify_checksum, ChecksumMismatch
from .log import get_logger
from .progress import AggregateProgress, ProgressTracker
from .retry import retry_with_backoff, retry_with_credential_refresh, RetryExhausted
from .safe_path import PathTraversalError, safe_join


# S3 configuration
DEFAULT_BUCKET = "monthly-datafile.datacite.org"
BUCKET = DEFAULT_BUCKET  # Backwards compatibility alias

# Size thresholds
LARGE_DOWNLOAD_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100 MB


@dataclass
class DownloadResult:
    """Result of a single file download operation."""

    key: str
    size: int
    checksum: str
    success: bool
    error: str | None = None


def create_s3_client(creds: AWSCredentials) -> "S3Client":
    """Create boto3 S3 client with temporary credentials."""
    return boto3.client(
        "s3",
        aws_access_key_id=creds.access_key_id,
        aws_secret_access_key=creds.secret_access_key,
        aws_session_token=creds.session_token,
    )


def list_contents(
    client: "S3Client", prefix: str = "", bucket: str = BUCKET
) -> tuple[list[str], list[str]]:
    """List folders and files at a given prefix.

    Returns:
        Tuple of (folder_names, file_names) at this level
    """
    folders: list[str] = []
    files: list[str] = []

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")

    for page in pages:
        # Common prefixes are "folders" in S3
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"]
            name = folder[len(prefix) :].rstrip("/")
            folders.append(name)

        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key == prefix:
                continue
            name = key[len(prefix) :]
            if name:  # Skip empty names
                files.append(name)

    return sorted(folders), sorted(files)


def get_object_size(client: "S3Client", key: str, bucket: str = BUCKET) -> int:
    """Get the size of an S3 object in bytes."""
    response = client.head_object(Bucket=bucket, Key=key)
    return response["ContentLength"]


def download_file(
    client: "S3Client",
    s3_key: str,
    local_path: str,
    show_progress: bool = True,
    bucket: str = BUCKET,
) -> None:
    """Download a single file with progress bar."""
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

    if show_progress:
        size = get_object_size(client, s3_key, bucket=bucket)
        filename = os.path.basename(s3_key)

        with tqdm(
            total=size,
            unit="B",
            unit_scale=True,
            desc=filename,
            ncols=80,
        ) as pbar:

            def callback(bytes_transferred: int) -> None:
                pbar.update(bytes_transferred)

            client.download_file(bucket, s3_key, local_path, Callback=callback)
    else:
        client.download_file(bucket, s3_key, local_path)


def download_prefix(
    client: "S3Client",
    prefix: str,
    output_dir: str,
    show_progress: bool = True,
    bucket: str = BUCKET,
) -> int:
    """Download all files under a prefix recursively.

    Returns:
        Number of files downloaded
    """
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    keys: list[str] = []
    total_size = 0

    for page in pages:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
            total_size += obj["Size"]

    if not keys:
        print(f"No files found under '{prefix}'")
        return 0

    print(f"Downloading {len(keys)} files ({total_size / (1024 * 1024):.1f} MB)")

    logger = get_logger()
    for key in tqdm(keys, desc="Files", disable=not show_progress):
        # Preserve directory structure relative to prefix
        relative_path = key[len(prefix) :].lstrip("/")
        if not relative_path:
            relative_path = os.path.basename(key)
        try:
            local_path = safe_join(output_dir, relative_path)
        except PathTraversalError as e:
            logger.warning(f"Skipping unsafe path for {key}: {e}")
            continue

        download_file(client, key, str(local_path), show_progress=False, bucket=bucket)

    return len(keys)


def list_all_objects(
    client: "S3Client",
    prefix: str = "",
    bucket: str = BUCKET,
) -> list[dict]:
    """List all objects under a prefix recursively.

    Returns:
        List of object dicts with Key, Size, and ETag
    """
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    objects: list[dict] = []
    for page in pages:
        for obj in page.get("Contents", []):
            objects.append(
                {
                    "Key": obj["Key"],
                    "Size": obj["Size"],
                    "ETag": obj["ETag"],
                }
            )

    return objects


def parse_size(size_str: str) -> int:
    """Parse human-readable size string (e.g., "10MB", "1GB") to bytes."""
    size_str = size_str.strip().upper()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB|B)?$", size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    value = float(match.group(1))
    unit = match.group(2) or "B"

    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
    }

    return int(value * multipliers[unit])


def should_download_file(
    key: str,
    size: int,
    tracker: ProgressTracker,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    max_size: int | None = None,
) -> bool:
    """Determine if a file should be downloaded based on filters and completion status."""
    logger = get_logger()

    if tracker.is_complete(key):
        logger.debug(f"Skipping completed file: {key}")
        return False

    filename = os.path.basename(key)

    if include_patterns:
        matches_include = any(fnmatch.fnmatch(filename, pattern) for pattern in include_patterns)
        if not matches_include:
            logger.debug(f"Skipping {key}: doesn't match include patterns")
            return False

    if exclude_patterns:
        matches_exclude = any(fnmatch.fnmatch(filename, pattern) for pattern in exclude_patterns)
        if matches_exclude:
            logger.debug(f"Skipping {key}: matches exclude pattern")
            return False

    if max_size is not None and size > max_size:
        logger.debug(f"Skipping {key}: size {size} exceeds max {max_size}")
        return False

    return True


def download_file_with_retry(
    client: "S3Client",
    s3_key: str,
    local_path: str | Path,
    expected_etag: str | None = None,
    retries: int = 3,
    skip_verify: bool = False,
    progress: bool | Callable[[int], None] = True,
    credential_manager: CredentialManager | None = None,
    bucket: str = BUCKET,
) -> None:
    """Download a file with retry logic and checksum verification.

    Downloads to a temporary file first, verifies checksum (if enabled),
    then moves to the target location atomically.

    Args:
        progress: True for tqdm bar, False for none, or a callback for parallel downloads
    """
    logger = get_logger()
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = local_path.with_suffix(local_path.suffix + ".tmp")

    def _download(active_client: "S3Client") -> None:
        if progress is True:
            size = get_object_size(active_client, s3_key, bucket=bucket)
            filename = os.path.basename(s3_key)

            with tqdm(
                total=size,
                unit="B",
                unit_scale=True,
                desc=filename,
                ncols=80,
            ) as pbar:

                def callback(bytes_transferred: int) -> None:
                    pbar.update(bytes_transferred)

                active_client.download_file(bucket, s3_key, str(temp_path), Callback=callback)
        elif callable(progress):
            active_client.download_file(bucket, s3_key, str(temp_path), Callback=progress)
        else:
            active_client.download_file(bucket, s3_key, str(temp_path))

    try:
        if credential_manager is not None:
            retry_with_credential_refresh(
                _download,
                credential_manager=credential_manager,
                max_retries=retries,
            )
        else:
            @retry_with_backoff(max_retries=retries)
            def _download_with_client() -> None:
                _download(client)

            _download_with_client()

        if not skip_verify and expected_etag:
            verify_checksum(temp_path, expected_etag)

        temp_path.rename(local_path)
        logger.debug(f"Downloaded: {s3_key} -> {local_path}")

    except (RetryExhausted, ChecksumMismatch):
        if temp_path.exists():
            temp_path.unlink()
        raise
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise


def download_worker(
    client: "S3Client",
    obj: dict,
    output_dir: str,
    prefix: str,
    retries: int,
    skip_verify: bool,
    aggregate_progress: AggregateProgress | None,
    credential_manager: CredentialManager | None = None,
    bucket: str = BUCKET,
) -> DownloadResult:
    """Download a single file for use with ThreadPoolExecutor."""
    key = obj["Key"]
    size = obj["Size"]
    etag = obj["ETag"]

    if credential_manager is not None:
        client = credential_manager.get_client()

    relative_path = key[len(prefix) :].lstrip("/") if prefix else key
    if not relative_path:
        relative_path = key.split("/")[-1]
    try:
        local_path = safe_join(output_dir, relative_path)
    except PathTraversalError as e:
        return DownloadResult(
            key=key,
            size=size,
            checksum=etag.strip('"'),
            success=False,
            error=f"Unsafe path: {e}",
        )

    def progress_callback(bytes_transferred: int) -> None:
        if aggregate_progress is not None:
            aggregate_progress.update(bytes_transferred)

    try:
        download_file_with_retry(
            client=client,
            s3_key=key,
            local_path=local_path,
            expected_etag=etag,
            retries=retries,
            skip_verify=skip_verify,
            progress=progress_callback if aggregate_progress else False,
            credential_manager=credential_manager,
            bucket=bucket,
        )

        if aggregate_progress is not None:
            aggregate_progress.complete_file()

        return DownloadResult(
            key=key,
            size=size,
            checksum=etag.strip('"'),
            success=True,
        )

    except Exception as e:
        if aggregate_progress is not None:
            aggregate_progress.fail_file()

        return DownloadResult(
            key=key,
            size=size,
            checksum=etag.strip('"'),
            success=False,
            error=str(e),
        )
