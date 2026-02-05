"""Entry point for datacite-data-file-dl CLI."""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import Logger
from typing import TYPE_CHECKING

from .auth import AuthenticationError, CredentialManager
from .cli import parse_args
from .config import Config, load_config
from .download import (
    BUCKET,
    DEFAULT_BUCKET,
    LARGE_DOWNLOAD_THRESHOLD_BYTES,
    download_file_with_retry,
    download_worker,
    list_all_objects,
    list_contents,
    parse_size,
    should_download_file,
)
from .exit_codes import ExitCode
from .interactive import select_download
from .log import get_logger, setup_logging
from .output import format_error, format_list, format_success
from .progress import AggregateProgress, ProgressTracker
from .safe_path import PathTraversalError, safe_join

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _print_error(config: Config, code: str, message: str) -> None:
    if config.json_output:
        print(format_error(code, message, json_output=True))
    else:
        print(f"Error: {message}", file=sys.stderr)


def _validate_credentials(config: Config) -> int | None:
    if not config.username:
        _print_error(
            config,
            "AUTH_FAILED",
            "Username required. Use -u, DATACITE_USERNAME env var, or config file.",
        )
        return ExitCode.AUTH_FAILURE

    if not config.password:
        _print_error(
            config,
            "AUTH_FAILED",
            "Password required. Use -p, DATACITE_PASSWORD env var, or run interactively.",
        )
        return ExitCode.AUTH_FAILURE

    return None


def _authenticate(
    config: Config, logger: Logger
) -> tuple[CredentialManager, "S3Client"] | int:
    """Assumes credentials have been validated by _validate_credentials."""
    logger.info("Authenticating with DataCite API...")
    # These assertions are safe because _validate_credentials checks for None
    assert config.username is not None
    assert config.password is not None
    try:
        cred_manager = CredentialManager(
            config.username,
            config.password,
            refresh_interval_seconds=config.refresh_interval * 60,
        )
        client = cred_manager.get_client()
    except AuthenticationError as e:
        if config.json_output:
            print(format_error("AUTH_FAILED", str(e), json_output=True))
        else:
            logger.error(f"Authentication failed: {e}")
        return ExitCode.AUTH_FAILURE
    except Exception as e:
        if config.json_output:
            print(format_error("NETWORK_ERROR", str(e), json_output=True))
        else:
            logger.error(f"Failed to create S3 client: {e}")
        return ExitCode.NETWORK_ERROR

    logger.info("Authentication successful.")
    return cred_manager, client


def _handle_list_mode(
    client: "S3Client", config: Config, logger: Logger
) -> int:
    prefix = (config.path or "").strip("/")
    if prefix:
        prefix += "/"

    bucket = config.bucket or DEFAULT_BUCKET

    try:
        folders, files = list_contents(client, prefix, bucket=bucket)
        file_info = [{"name": f, "size": 0} for f in files]

        output = format_list(
            folders=folders,
            files=file_info,
            json_output=config.json_output,
            prefix=prefix,
        )
        print(output)
        return ExitCode.SUCCESS
    except Exception as e:
        if config.json_output:
            print(format_error("NETWORK_ERROR", str(e), json_output=True))
        else:
            logger.error(f"Failed to list contents: {e}")
        return ExitCode.NETWORK_ERROR


def _resolve_prefix(
    config: Config, logger: Logger
) -> str | int | None:
    """Returns S3 prefix, ExitCode on error, or None for interactive mode."""
    bucket = config.bucket or DEFAULT_BUCKET

    if config.download_all:
        logger.info(f"Downloading entire bucket: s3://{bucket}/")
        return ""

    if config.path:
        prefix = config.path.strip("/")
        if not prefix.endswith("/") and "/" in prefix:
            prefix += "/"
        logger.info(f"Downloading: s3://{bucket}/{prefix}")
        return prefix

    # Interactive mode not compatible with JSON output
    if config.json_output:
        print(
            format_error(
                "INVALID_ARGUMENT",
                "Interactive mode not supported with --json",
                json_output=True,
            )
        )
        return ExitCode.USER_CANCELLED

    return None  # Signals need for interactive mode


def _build_download_list(
    client: "S3Client",
    prefix: str,
    config: Config,
    tracker: ProgressTracker,
    max_size_bytes: int | None,
    logger: Logger,
) -> tuple[list[dict], int] | int:
    bucket = config.bucket or DEFAULT_BUCKET

    try:
        all_objects = list_all_objects(client, prefix, bucket=bucket)
    except Exception as e:
        if config.json_output:
            print(format_error("NETWORK_ERROR", str(e), json_output=True))
        else:
            logger.error(f"Failed to list objects: {e}")
        return ExitCode.NETWORK_ERROR

    if not all_objects:
        msg = f"No files found under '{prefix}'"
        if config.json_output:
            print(format_error("NOT_FOUND", msg, json_output=True))
        else:
            logger.warning(msg)
        return ExitCode.NOT_FOUND

    to_download = []
    skipped = 0
    for obj in all_objects:
        if should_download_file(
            key=obj["Key"],
            size=obj["Size"],
            tracker=tracker,
            include_patterns=config.include_patterns,
            exclude_patterns=config.exclude_patterns,
            max_size=max_size_bytes,
        ):
            to_download.append(obj)
        else:
            skipped += 1

    return to_download, skipped


def _download_sequential(
    to_download: list[dict],
    prefix: str,
    config: Config,
    tracker: ProgressTracker,
    cred_manager: CredentialManager,
    logger: Logger,
) -> tuple[list[dict], int]:
    bucket = config.bucket or DEFAULT_BUCKET
    downloaded = []
    failed = 0

    for i, obj in enumerate(to_download, 1):
        key = obj["Key"]
        relative_path = key[len(prefix) :].lstrip("/") if prefix else key
        if not relative_path:
            relative_path = key.split("/")[-1]

        try:
            local_path = safe_join(config.output_dir, relative_path)
        except PathTraversalError as e:
            logger.warning(f"Skipping unsafe path for {key}: {e}")
            failed += 1
            continue

        logger.info(f"[{i}/{len(to_download)}] {key}")
        client = cred_manager.get_client()

        try:
            download_file_with_retry(
                client=client,
                s3_key=key,
                local_path=local_path,
                expected_etag=obj["ETag"],
                retries=config.retries,
                skip_verify=config.skip_verify,
                progress=not config.quiet,
                credential_manager=cred_manager,
                bucket=bucket,
            )
            tracker.mark_complete(key, obj["Size"], obj["ETag"])
            downloaded.append({
                "path": key,
                "size": obj["Size"],
                "checksum": obj["ETag"].strip('"'),
            })
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"Failed to download {key}: {e}")
            failed += 1

    return downloaded, failed


def _download_parallel(
    to_download: list[dict],
    prefix: str,
    config: Config,
    tracker: ProgressTracker,
    cred_manager: CredentialManager,
    client: "S3Client",
    total_size: int,
    logger: Logger,
) -> tuple[list[dict], int]:
    bucket = config.bucket or DEFAULT_BUCKET
    downloaded = []
    failed = 0

    aggregate_progress = AggregateProgress(
        total_files=len(to_download),
        total_bytes=total_size,
        show_progress=not config.quiet,
    )

    try:
        with ThreadPoolExecutor(max_workers=config.workers) as executor:
            futures = {
                executor.submit(
                    download_worker,
                    client=client,
                    obj=obj,
                    output_dir=config.output_dir,
                    prefix=prefix,
                    retries=config.retries,
                    skip_verify=config.skip_verify,
                    aggregate_progress=aggregate_progress,
                    credential_manager=cred_manager,
                    bucket=bucket,
                ): obj
                for obj in to_download
            }

            for future in as_completed(futures):
                result = future.result()
                if result.success:
                    tracker.mark_complete(result.key, result.size, result.checksum)
                    downloaded.append({
                        "path": result.key,
                        "size": result.size,
                        "checksum": result.checksum,
                    })
                else:
                    logger.error(f"Failed to download {result.key}: {result.error}")
                    failed += 1
    finally:
        aggregate_progress.close()

    return downloaded, failed


def _print_results(
    config: Config,
    downloaded: list[dict],
    skipped: int,
    failed: int,
    elapsed: float,
) -> None:
    print(
        format_success(
            files=downloaded,
            total_bytes=sum(f["size"] for f in downloaded),
            elapsed_seconds=elapsed,
            json_output=config.json_output,
            skipped=skipped,
            failed=failed,
        )
    )


def main() -> int:
    args = parse_args()

    config = load_config(
        cli_username=args.username,
        cli_password=args.password,
        cli_output_dir=args.output,
        prompt_for_missing=not args.quiet and sys.stdin.isatty(),
        path=args.path,
        download_all=args.download_all,
        list_only=args.list,
        dry_run=args.dry_run,
        json_output=args.json,
        quiet=args.quiet,
        verbose=args.verbose,
        log_file=args.log_file,
        retries=args.retries,
        refresh_interval=args.refresh_interval,
        resume=args.resume,
        fresh=args.fresh,
        skip_verify=args.skip_verify,
        include_patterns=args.include,
        exclude_patterns=args.exclude,
        since=args.since,
        until=args.until,
        max_size=args.max_size,
        yes=args.yes,
        workers=min(args.workers, 32),
        bucket=args.bucket,
    )

    setup_logging(verbose=config.verbose, quiet=config.quiet, log_file=config.log_file)
    logger = get_logger()

    if error := _validate_credentials(config):
        return error

    auth_result = _authenticate(config, logger)
    if isinstance(auth_result, int):
        return auth_result
    cred_manager, client = auth_result

    if config.list_only:
        return _handle_list_mode(client, config, logger)

    tracker = ProgressTracker(config.output_dir)
    if config.fresh:
        tracker.clear()

    max_size_bytes = None
    if config.max_size:
        try:
            max_size_bytes = parse_size(config.max_size)
        except ValueError as e:
            _print_error(config, "INVALID_ARGUMENT", str(e))
            return ExitCode.NOT_FOUND

    prefix_result = _resolve_prefix(config, logger)
    if isinstance(prefix_result, int):
        return prefix_result
    if prefix_result is None:
        try:
            select_download(client, config.output_dir, credential_manager=cred_manager)
            return ExitCode.SUCCESS
        except KeyboardInterrupt:
            logger.info("Cancelled by user.")
            return ExitCode.USER_CANCELLED
        except Exception as e:
            logger.error(f"Error: {e}")
            return ExitCode.NETWORK_ERROR
    prefix = prefix_result

    list_result = _build_download_list(
        client, prefix, config, tracker, max_size_bytes, logger
    )
    if isinstance(list_result, int):
        return list_result
    to_download, skipped = list_result

    if not to_download:
        logger.info("All files already downloaded or filtered out.")
        if config.json_output:
            print(format_success(
                files=[], total_bytes=0, elapsed_seconds=0,
                json_output=True, skipped=skipped,
            ))
        return ExitCode.SUCCESS

    total_size = sum(obj["Size"] for obj in to_download)
    logger.info(
        f"Found {len(to_download)} files to download ({total_size / (1024 * 1024):.1f} MB)"
    )

    if config.dry_run:
        for obj in to_download:
            print(f"Would download: {obj['Key']} ({obj['Size']} bytes)")
        return ExitCode.SUCCESS

    if not config.yes and not config.quiet and sys.stdin.isatty():
        if total_size > LARGE_DOWNLOAD_THRESHOLD_BYTES:
            response = input(
                f"Download {len(to_download)} files "
                f"({total_size / (1024 * 1024):.1f} MB)? [y/N] "
            )
            if response.lower() != "y":
                logger.info("Cancelled by user.")
                return ExitCode.USER_CANCELLED

    start_time = time.time()
    try:
        if config.workers == 1:
            downloaded, failed = _download_sequential(
                to_download, prefix, config, tracker, cred_manager, logger
            )
        else:
            downloaded, failed = _download_parallel(
                to_download, prefix, config, tracker, cred_manager,
                client, total_size, logger
            )
    except KeyboardInterrupt:
        logger.info("Cancelled by user.")
        return ExitCode.USER_CANCELLED

    elapsed = time.time() - start_time
    _print_results(config, downloaded, skipped, failed, elapsed)

    return ExitCode.PARTIAL_FAILURE if failed > 0 else ExitCode.SUCCESS


if __name__ == "__main__":
    sys.exit(main())
