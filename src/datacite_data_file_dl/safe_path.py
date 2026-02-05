"""Path validation utilities to prevent path traversal attacks."""

import os
from pathlib import Path


class PathTraversalError(ValueError):
    """Raised when a path traversal attack is detected."""

    def __init__(self, untrusted_path: str, reason: str) -> None:
        self.untrusted_path = untrusted_path
        self.reason = reason
        super().__init__(
            f"Path traversal detected: {reason} (input: {untrusted_path!r})"
        )


def safe_join(base_dir: str | Path, untrusted_path: str) -> Path:
    """Safely join a base directory with an untrusted path.

    This function validates that the resulting path is contained within
    the base directory, preventing path traversal attacks.

    Args:
        base_dir: The trusted base directory (must exist or be creatable)
        untrusted_path: The untrusted relative path from S3 or user input

    Returns:
        Validated absolute Path object within base_dir

    Raises:
        PathTraversalError: If the path would escape base_dir

    Examples:
        >>> safe_join("/data", "file.json")
        PosixPath('/data/file.json')

        >>> safe_join("/data", "subdir/file.json")
        PosixPath('/data/subdir/file.json')

        >>> safe_join("/data", "../etc/passwd")
        Raises PathTraversalError
    """
    base = Path(base_dir).resolve()

    if not untrusted_path or untrusted_path.strip() == "":
        raise PathTraversalError(untrusted_path, "empty path")

    if os.path.isabs(untrusted_path):
        raise PathTraversalError(untrusted_path, "absolute path not allowed")

    if untrusted_path.startswith("."):
        raise PathTraversalError(untrusted_path, "path cannot start with '.'")

    try:
        joined = (base / untrusted_path).resolve()
    except (ValueError, OSError) as e:
        raise PathTraversalError(untrusted_path, f"invalid path: {e}") from e

    try:
        joined.relative_to(base)
    except ValueError as e:
        raise PathTraversalError(
            untrusted_path, f"path escapes base directory {base}"
        ) from e

    return joined
