"""Output formatting for JSON and human-readable modes."""

import json
from dataclasses import dataclass


def format_size(size_bytes: int | float) -> str:
    """Format bytes as human-readable size."""
    size: float = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_success(
    files: list[dict],
    total_bytes: int,
    elapsed_seconds: float,
    json_output: bool = False,
    skipped: int = 0,
    failed: int = 0,
) -> str:
    """Format successful download result."""
    if json_output:
        return json.dumps(
            {
                "status": "success",
                "files": files,
                "total_bytes": total_bytes,
                "elapsed_seconds": round(elapsed_seconds, 2),
                "summary": {
                    "downloaded": len(files),
                    "skipped": skipped,
                    "failed": failed,
                },
            },
            indent=2,
        )

    lines = [
        "",
        "Download complete:",
        f"  Files: {len(files)} downloaded, {skipped} skipped, {failed} failed",
        f"  Size:  {format_size(total_bytes)}",
        f"  Time:  {format_duration(elapsed_seconds)}",
    ]
    return "\n".join(lines)


def format_error(
    code: str,
    message: str,
    json_output: bool = False,
) -> str:
    """Format error result."""
    if json_output:
        return json.dumps(
            {
                "status": "error",
                "code": code,
                "message": message,
            },
            indent=2,
        )

    return f"Error: {message}"


def format_list(
    folders: list[str],
    files: list[dict],
    json_output: bool = False,
    prefix: str = "",
) -> str:
    """Format directory listing."""
    if json_output:
        return json.dumps(
            {
                "prefix": prefix,
                "folders": folders,
                "files": files,
            },
            indent=2,
        )

    lines = []

    if folders:
        lines.append("Folders:")
        for folder in folders:
            lines.append(f"  {folder}/")

    if files:
        lines.append("Files:")
        for f in files:
            size_str = format_size(f.get("size", 0))
            lines.append(f"  {f['name']:40} {size_str:>10}")

    if not folders and not files:
        lines.append("(empty)")

    return "\n".join(lines)


@dataclass
class OutputFormatter:
    """Stateful output formatter."""

    json_output: bool = False
    quiet: bool = False

    def success(self, **kwargs: object) -> str:
        return format_success(json_output=self.json_output, **kwargs)  # type: ignore[arg-type]

    def error(self, **kwargs: object) -> str:
        return format_error(json_output=self.json_output, **kwargs)  # type: ignore[arg-type]

    def list(self, **kwargs: object) -> str:
        return format_list(json_output=self.json_output, **kwargs)  # type: ignore[arg-type]
