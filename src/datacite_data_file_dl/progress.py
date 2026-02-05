"""Progress tracking and checkpointing for downloads."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock

from tqdm import tqdm


PROGRESS_FILE = ".datacite-data-file-dl-progress.json"


@dataclass
class FileStatus:
    path: str
    size: int
    checksum: str
    completed: bool = False
    completed_at: str | None = None

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "size": self.size,
            "checksum": self.checksum,
            "completed": self.completed,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FileStatus":
        return cls(
            path=d["path"],
            size=d["size"],
            checksum=d["checksum"],
            completed=d.get("completed", False),
            completed_at=d.get("completed_at"),
        )


@dataclass
class ProgressTracker:
    """Track download progress with checkpoint support for resumable downloads."""

    output_dir: Path | str
    files: dict[str, FileStatus] = field(default_factory=dict)
    _loaded: bool = field(default=False, repr=False)
    _lock: RLock = field(default_factory=RLock, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.output_dir, str):
            self.output_dir = Path(self.output_dir)
        self._load()

    @property
    def progress_file(self) -> Path:
        assert isinstance(self.output_dir, Path)  # Converted in __post_init__
        return self.output_dir / PROGRESS_FILE

    def _load(self) -> None:
        if self._loaded:
            return

        if self.progress_file.exists():
            with open(self.progress_file) as f:
                data = json.load(f)
                for file_data in data.get("files", []):
                    status = FileStatus.from_dict(file_data)
                    self.files[status.path] = status

        self._loaded = True

    def save(self) -> None:
        with self._lock:
            assert isinstance(self.output_dir, Path)  # Converted in __post_init__
            self.output_dir.mkdir(parents=True, exist_ok=True)

            data = {
                "version": 1,
                "updated_at": datetime.now().isoformat(),
                "files": [status.to_dict() for status in self.files.values()],
            }

            with open(self.progress_file, "w") as f:
                json.dump(data, f, indent=2)

    def mark_complete(self, path: str, size: int, checksum: str) -> None:
        with self._lock:
            self.files[path] = FileStatus(
                path=path,
                size=size,
                checksum=checksum,
                completed=True,
                completed_at=datetime.now().isoformat(),
            )
            self.save()

    def is_complete(self, path: str) -> bool:
        with self._lock:
            status = self.files.get(path)
            return status is not None and status.completed

    def get_completed_files(self) -> list[str]:
        return [path for path, status in self.files.items() if status.completed]

    def get_stats(self) -> dict:
        completed = [s for s in self.files.values() if s.completed]
        return {
            "files_completed": len(completed),
            "bytes_completed": sum(s.size for s in completed),
        }

    def clear(self) -> None:
        with self._lock:
            self.files.clear()
            if self.progress_file.exists():
                self.progress_file.unlink()


class AggregateProgress:
    """Thread-safe aggregate progress bar for parallel downloads."""

    def __init__(
        self,
        total_files: int,
        total_bytes: int,
        show_progress: bool = True,
    ) -> None:
        self.total_files = total_files
        self.total_bytes = total_bytes
        self._lock = RLock()
        self._completed_bytes = 0
        self._completed_files = 0
        self._failed_files = 0
        self._show_progress = show_progress

        self._pbar: tqdm | None  # type: ignore[type-arg]
        if show_progress:
            self._pbar = tqdm(
                total=total_bytes,
                unit="B",
                unit_scale=True,
                desc=f"Downloading {total_files} files",
                ncols=80,
            )
        else:
            self._pbar = None

    def update(self, bytes_downloaded: int) -> None:
        with self._lock:
            self._completed_bytes += bytes_downloaded
            if self._pbar is not None:
                self._pbar.update(bytes_downloaded)

    def complete_file(self) -> None:
        with self._lock:
            self._completed_files += 1
            if self._pbar is not None:
                self._pbar.set_postfix(
                    files=f"{self._completed_files}/{self.total_files}",
                    refresh=False,
                )

    def fail_file(self) -> None:
        with self._lock:
            self._failed_files += 1

    def close(self) -> None:
        if self._pbar is not None:
            self._pbar.close()

    @property
    def completed_files(self) -> int:
        with self._lock:
            return self._completed_files

    @property
    def failed_files(self) -> int:
        with self._lock:
            return self._failed_files
