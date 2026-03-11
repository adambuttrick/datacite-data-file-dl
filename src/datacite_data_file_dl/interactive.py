"""Interactive file selection for browsing the S3 bucket."""

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from botocore.exceptions import ClientError

from .auth import CredentialManager
from .download import (
    BUCKET,
    download_file,
    download_prefix,
    get_manifest_metadata,
    get_status_json,
    list_contents,
)
from .output import format_status
from .safe_path import PathTraversalError, safe_join


def _fetch_and_print_status(client: "S3Client", bucket: str = BUCKET) -> None:
    manifest_last_modified = None
    status_json = None

    try:
        manifest_last_modified = get_manifest_metadata(client, bucket=bucket)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code not in ("404", "NoSuchKey"):
            print(f"  Warning: Could not fetch MANIFEST metadata: {e}")
    except Exception as e:
        print(f"  Warning: Could not fetch MANIFEST metadata: {e}")

    try:
        status_json = get_status_json(client, bucket=bucket)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code not in ("404", "NoSuchKey"):
            print(f"  Warning: Could not fetch STATUS.json: {e}")
    except Exception as e:
        print(f"  Warning: Could not fetch STATUS.json: {e}")

    print(
        format_status(
            manifest_last_modified=manifest_last_modified,
            status_json=status_json,
        )
    )


def print_menu(current_path: str, folders: list[str], files: list[str]) -> None:
    print()
    print("=" * 60)
    location = current_path if current_path else "(root)"
    print(f"Location: s3://{BUCKET}/{location}")
    print("=" * 60)

    idx = 1
    if folders:
        print("\nFolders:")
        for folder in folders:
            print(f"  [{idx}] {folder}/")
            idx += 1

    if files:
        print("\nFiles:")
        for file in files:
            print(f"  [{idx}] {file}")
            idx += 1

    if not folders and not files:
        print("\n  (empty)")

    print()
    print("Commands:")
    print("  [number]  - Select item (enter folder or download file)")
    print("  d[number] - Download folder/file (e.g., 'd1' to download item 1)")
    print("  a         - Download all (everything at current level)")
    print("  s         - Check data file status")
    if current_path:
        print("  b         - Go back to parent folder")
    print("  q         - Quit")
    print()


def select_download(
    client: "S3Client",
    output_dir: str,
    credential_manager: CredentialManager | None = None,
    bucket: str = BUCKET,
) -> None:
    """Interactive loop for browsing and selecting files to download."""
    path_stack: list[str] = []
    current_prefix = ""

    def get_client() -> "S3Client":
        if credential_manager is not None:
            return credential_manager.get_client()
        return client

    _fetch_and_print_status(get_client(), bucket=bucket)

    while True:
        try:
            folders, files = list_contents(get_client(), current_prefix)
        except Exception as e:
            print(f"Error listing contents: {e}")
            return

        print_menu(current_prefix, folders, files)

        try:
            choice = input("Select: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            return

        if choice == "q":
            print("Goodbye!")
            return

        if choice == "b":
            if path_stack:
                path_stack.pop()
                current_prefix = "/".join(path_stack) + "/" if path_stack else ""
            else:
                print("Already at root.")
            continue

        if choice == "a":
            prefix = current_prefix if current_prefix else ""
            location = current_prefix if current_prefix else "entire bucket"
            print(f"\nDownloading all from: {location}")
            count = download_prefix(get_client(), prefix, output_dir)
            print(f"Downloaded {count} files to {output_dir}")
            continue

        if choice == "s":
            _fetch_and_print_status(get_client(), bucket=bucket)
            continue

        download_item = False
        if choice.startswith("d"):
            download_item = True
            choice = choice[1:]

        try:
            num = int(choice)
        except ValueError:
            print(f"Invalid input: '{choice}'")
            continue

        all_items = folders + files
        if num < 1 or num > len(all_items):
            print(f"Invalid selection. Choose 1-{len(all_items)}.")
            continue

        selected = all_items[num - 1]
        is_folder = num <= len(folders)

        if is_folder:
            full_path = current_prefix + selected + "/"
            if download_item:
                print(f"\nDownloading folder: {full_path}")
                count = download_prefix(get_client(), full_path, output_dir)
                print(f"Downloaded {count} files to {output_dir}")
            else:
                path_stack.append(selected)
                current_prefix = full_path
        else:
            s3_key = current_prefix + selected
            try:
                local_path = safe_join(output_dir, selected)
            except PathTraversalError as e:
                print(f"Error: Cannot download - unsafe path: {e}")
                continue
            print(f"\nDownloading: {s3_key}")
            download_file(get_client(), s3_key, str(local_path))
            print(f"Saved to: {local_path}")
