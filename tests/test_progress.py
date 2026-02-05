"""Tests for progress tracking and checkpointing."""

import threading
from concurrent.futures import ThreadPoolExecutor

from datacite_data_file_dl.progress import ProgressTracker, FileStatus


class TestProgressTracker:
    """Test progress tracking functionality."""

    def test_mark_complete(self, tmp_output_dir):
        """Should track completed files."""
        tracker = ProgressTracker(tmp_output_dir)
        tracker.mark_complete("file1.json", size=1000, checksum="abc123")

        assert tracker.is_complete("file1.json")
        assert not tracker.is_complete("file2.json")

    def test_persistence(self, tmp_output_dir):
        """Progress should persist to disk."""
        tracker1 = ProgressTracker(tmp_output_dir)
        tracker1.mark_complete("file1.json", size=1000, checksum="abc123")
        tracker1.save()

        tracker2 = ProgressTracker(tmp_output_dir)
        assert tracker2.is_complete("file1.json")

    def test_get_completed_files(self, tmp_output_dir):
        """Should return list of completed files."""
        tracker = ProgressTracker(tmp_output_dir)
        tracker.mark_complete("file1.json", size=1000, checksum="abc")
        tracker.mark_complete("file2.json", size=2000, checksum="def")

        completed = tracker.get_completed_files()
        assert set(completed) == {"file1.json", "file2.json"}

    def test_clear(self, tmp_output_dir):
        """Should clear all progress."""
        tracker = ProgressTracker(tmp_output_dir)
        tracker.mark_complete("file1.json", size=1000, checksum="abc")
        tracker.clear()

        assert not tracker.is_complete("file1.json")
        assert tracker.get_completed_files() == []

    def test_stats(self, tmp_output_dir):
        """Should track download statistics."""
        tracker = ProgressTracker(tmp_output_dir)
        tracker.mark_complete("file1.json", size=1000, checksum="abc")
        tracker.mark_complete("file2.json", size=2000, checksum="def")

        stats = tracker.get_stats()
        assert stats["files_completed"] == 2
        assert stats["bytes_completed"] == 3000


class TestFileStatus:
    """Test FileStatus dataclass."""

    def test_to_dict(self):
        """Should convert to dict for JSON serialization."""
        status = FileStatus(
            path="test.json",
            size=1000,
            checksum="abc123",
            completed=True,
        )
        d = status.to_dict()
        assert d["path"] == "test.json"
        assert d["size"] == 1000
        assert d["checksum"] == "abc123"
        assert d["completed"] is True

    def test_from_dict(self):
        """Should create from dict."""
        d = {"path": "test.json", "size": 1000, "checksum": "abc123", "completed": True}
        status = FileStatus.from_dict(d)
        assert status.path == "test.json"
        assert status.size == 1000


class TestProgressTrackerThreadSafety:
    """Test thread-safety of ProgressTracker."""

    def test_concurrent_mark_complete(self, tmp_output_dir):
        """10 threads marking 100 files should not corrupt state."""
        tracker = ProgressTracker(tmp_output_dir)
        num_threads = 10
        files_per_thread = 10

        def mark_files(thread_id: int) -> None:
            for i in range(files_per_thread):
                path = f"thread{thread_id}_file{i}.json"
                tracker.mark_complete(path, size=1000, checksum=f"checksum_{thread_id}_{i}")

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mark_files, t) for t in range(num_threads)]
            for f in futures:
                f.result()

        expected_count = num_threads * files_per_thread
        assert len(tracker.get_completed_files()) == expected_count

        for t in range(num_threads):
            for i in range(files_per_thread):
                path = f"thread{t}_file{i}.json"
                assert tracker.is_complete(path), f"Missing: {path}"

    def test_concurrent_read_write(self, tmp_output_dir):
        """Simultaneous readers and writers should not deadlock or corrupt."""
        tracker = ProgressTracker(tmp_output_dir)
        results = {"reads": 0, "writes": 0}
        lock = threading.Lock()

        def writer(thread_id: int) -> None:
            for i in range(20):
                tracker.mark_complete(f"w{thread_id}_f{i}.json", size=100, checksum="x")
                with lock:
                    results["writes"] += 1

        def reader() -> None:
            for _ in range(50):
                tracker.is_complete("some_file.json")
                tracker.get_completed_files()
                with lock:
                    results["reads"] += 1

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            # 3 writers
            for t in range(3):
                futures.append(executor.submit(writer, t))
            # 3 readers
            for _ in range(3):
                futures.append(executor.submit(reader))

            for f in futures:
                f.result()

        assert results["writes"] == 60  # 3 writers * 20 writes
        assert results["reads"] == 150  # 3 readers * 50 reads

    def test_persistence_under_concurrent_writes(self, tmp_output_dir):
        """Progress file should remain valid after concurrent writes."""
        tracker = ProgressTracker(tmp_output_dir)

        def mark_files(thread_id: int) -> None:
            for i in range(10):
                tracker.mark_complete(f"t{thread_id}_f{i}.json", size=500, checksum="abc")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(mark_files, t) for t in range(5)]
            for f in futures:
                f.result()

        tracker2 = ProgressTracker(tmp_output_dir)
        assert len(tracker2.get_completed_files()) == 50

        stats = tracker2.get_stats()
        assert stats["files_completed"] == 50
        assert stats["bytes_completed"] == 50 * 500
