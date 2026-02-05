"""Integration tests with mocked S3."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from datacite_data_file_dl.download import (
    list_contents,
    list_all_objects,
    download_file_with_retry,
    download_worker,
)
from datacite_data_file_dl.progress import AggregateProgress, ProgressTracker


class TestListContents:
    """Test S3 listing operations."""

    def test_list_root(self, populated_s3):
        """Should list root level contents."""
        folders, files = list_contents(populated_s3, "")

        assert "dois" in folders
        assert "MANIFEST" in files

    def test_list_prefix(self, populated_s3):
        """Should list contents under a prefix."""
        folders, files = list_contents(populated_s3, "dois/")

        assert "updated_2024-01" in folders
        assert "updated_2024-02" in folders

    def test_list_nonexistent_prefix(self, populated_s3):
        """Should return empty lists for non-existent prefix."""
        folders, files = list_contents(populated_s3, "nonexistent/")

        assert folders == []
        assert files == []


class TestListAllObjects:
    """Test recursive object listing."""

    def test_list_all_under_prefix(self, populated_s3):
        """Should list all objects recursively."""
        objects = list_all_objects(populated_s3, "dois/updated_2024-01/")

        assert len(objects) == 2
        keys = [o["Key"] for o in objects]
        assert "dois/updated_2024-01/part-001.json" in keys
        assert "dois/updated_2024-01/part-002.json" in keys


class TestDownloadWithRetry:
    """Test download operations."""

    def test_download_single_file(self, populated_s3, tmp_output_dir):
        """Should download a single file."""
        local_path = tmp_output_dir / "MANIFEST"

        download_file_with_retry(
            client=populated_s3,
            s3_key="MANIFEST",
            local_path=local_path,
            progress=False,
        )

        assert local_path.exists()
        assert local_path.read_text() == "manifest content"

    def test_download_with_progress_tracking(self, populated_s3, tmp_output_dir):
        """Should integrate with progress tracker."""
        tracker = ProgressTracker(tmp_output_dir)
        local_path = tmp_output_dir / "MANIFEST"

        download_file_with_retry(
            client=populated_s3,
            s3_key="MANIFEST",
            local_path=local_path,
            progress=False,
        )
        tracker.mark_complete("MANIFEST", size=16, checksum="abc")

        assert tracker.is_complete("MANIFEST")


class TestParallelDownload:
    """Test parallel download operations."""

    def test_parallel_download_basic(self, populated_s3, tmp_output_dir):
        """Should download multiple files in parallel."""
        objects = [
            {"Key": "dois/updated_2024-01/part-001.json", "Size": 17, "ETag": '"abc1"'},
            {"Key": "dois/updated_2024-01/part-002.json", "Size": 17, "ETag": '"abc2"'},
            {"Key": "dois/updated_2024-02/part-001.json", "Size": 17, "ETag": '"abc3"'},
        ]

        tracker = ProgressTracker(tmp_output_dir)
        aggregate_progress = AggregateProgress(
            total_files=len(objects),
            total_bytes=sum(o["Size"] for o in objects),
            show_progress=False,
        )

        downloaded = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(
                    download_worker,
                    client=populated_s3,
                    obj=obj,
                    output_dir=str(tmp_output_dir),
                    prefix="dois/",
                    retries=3,
                    skip_verify=True,
                    aggregate_progress=aggregate_progress,
                ): obj
                for obj in objects
            }

            for future in as_completed(futures):
                result = future.result()
                assert result.success, f"Download failed: {result.error}"
                tracker.mark_complete(result.key, result.size, result.checksum)
                downloaded.append(result.key)

        aggregate_progress.close()

        assert len(downloaded) == 3
        assert tracker.is_complete("dois/updated_2024-01/part-001.json")
        assert tracker.is_complete("dois/updated_2024-01/part-002.json")
        assert tracker.is_complete("dois/updated_2024-02/part-001.json")

        assert (tmp_output_dir / "updated_2024-01" / "part-001.json").exists()
        assert (tmp_output_dir / "updated_2024-01" / "part-002.json").exists()
        assert (tmp_output_dir / "updated_2024-02" / "part-001.json").exists()

    def test_parallel_download_with_failure(self, populated_s3, tmp_output_dir):
        """Should handle individual file failures gracefully."""
        objects = [
            {"Key": "dois/updated_2024-01/part-001.json", "Size": 17, "ETag": '"abc1"'},
            {"Key": "nonexistent/file.json", "Size": 100, "ETag": '"xyz"'},  # Will fail
        ]

        aggregate_progress = AggregateProgress(
            total_files=len(objects),
            total_bytes=sum(o["Size"] for o in objects),
            show_progress=False,
        )

        results = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    download_worker,
                    client=populated_s3,
                    obj=obj,
                    output_dir=str(tmp_output_dir),
                    prefix="",
                    retries=1,
                    skip_verify=True,
                    aggregate_progress=aggregate_progress,
                ): obj
                for obj in objects
            }

            for future in as_completed(futures):
                results.append(future.result())

        aggregate_progress.close()

        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert successes[0].key == "dois/updated_2024-01/part-001.json"
        assert failures[0].key == "nonexistent/file.json"
        assert failures[0].error is not None

    def test_workers_one_equals_sequential(self, populated_s3, tmp_output_dir):
        """Single worker should produce same results as sequential."""
        obj = {"Key": "MANIFEST", "Size": 16, "ETag": '"manifest_etag"'}

        # Download with single worker
        aggregate_progress = AggregateProgress(
            total_files=1,
            total_bytes=16,
            show_progress=False,
        )

        result = download_worker(
            client=populated_s3,
            obj=obj,
            output_dir=str(tmp_output_dir),
            prefix="",
            retries=3,
            skip_verify=True,
            aggregate_progress=aggregate_progress,
        )

        aggregate_progress.close()

        assert result.success
        assert result.key == "MANIFEST"
        assert result.size == 16

        assert (tmp_output_dir / "MANIFEST").exists()
        assert (tmp_output_dir / "MANIFEST").read_text() == "manifest content"
