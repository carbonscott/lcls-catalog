"""Tests for Parquet catalog with base + delta incremental updates."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Skip all tests if parquet dependencies not installed
pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

from lcls_catalog.parquet_catalog import ParquetCatalog


@pytest.fixture
def parquet_catalog_dir(tmp_path):
    """Return a path for a test Parquet catalog directory."""
    return tmp_path / "parquet_catalog"


class TestBaseSnapshot:
    """Tests for initial base snapshot creation."""

    def test_first_snapshot_creates_base(self, fake_experiment, parquet_catalog_dir):
        """First snapshot should create a base file."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            added, modified, removed = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            assert added == fake_experiment.expected_file_count
            assert modified == 0
            assert removed == 0

            # Check base file was created
            exp_dirs = list(parquet_catalog_dir.iterdir())
            assert len(exp_dirs) == 1

            base_files = list(exp_dirs[0].glob("base_*.parquet"))
            assert len(base_files) == 1

    def test_snapshot_captures_correct_count(self, fake_experiment, parquet_catalog_dir):
        """Snapshot should capture all files."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )
            assert cat.count() == fake_experiment.expected_file_count

    def test_snapshot_captures_correct_size(self, fake_experiment, parquet_catalog_dir):
        """Snapshot should capture correct total size."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )
            assert cat.total_size() == fake_experiment.expected_total_size


class TestDeltaUpdates:
    """Tests for incremental delta updates."""

    def test_no_changes_no_delta(self, fake_experiment, parquet_catalog_dir):
        """Re-indexing with no changes should not create a delta."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            # First snapshot
            cat.snapshot(str(fake_experiment.experiment_path))

            exp_dir = list(parquet_catalog_dir.iterdir())[0]
            initial_files = list(exp_dir.glob("*.parquet"))

            # Second snapshot (no changes)
            added, modified, removed = cat.snapshot(str(fake_experiment.experiment_path))

            assert added == 0
            assert modified == 0
            assert removed == 0

            # Should still have same number of files
            final_files = list(exp_dir.glob("*.parquet"))
            assert len(final_files) == len(initial_files)

    def test_new_file_creates_delta(self, fake_experiment, parquet_catalog_dir):
        """Adding a new file should create a delta with status='added'."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            # First snapshot
            cat.snapshot(str(fake_experiment.experiment_path))
            initial_count = cat.count()

            # Add a new file
            new_file = fake_experiment.experiment_path / "scratch" / "new_file.txt"
            new_file.write_text("new content")

            # Second snapshot
            added, modified, removed = cat.snapshot(str(fake_experiment.experiment_path))

            assert added == 1
            assert modified == 0
            assert removed == 0
            assert cat.count() == initial_count + 1

            # Check delta file was created
            exp_dir = list(parquet_catalog_dir.iterdir())[0]
            delta_files = list(exp_dir.glob("delta_*.parquet"))
            assert len(delta_files) == 1

    def test_modified_file_creates_delta(self, fake_experiment, parquet_catalog_dir):
        """Modifying a file should create a delta with status='modified'."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            # First snapshot
            cat.snapshot(str(fake_experiment.experiment_path))

            # Modify a file (change size)
            modified_file = fake_experiment.experiment_path / "scratch" / "run0001" / "metadata.json"
            modified_file.write_text("much longer content than before" * 10)

            # Second snapshot
            added, modified, removed = cat.snapshot(str(fake_experiment.experiment_path))

            assert added == 0
            assert modified == 1
            assert removed == 0

    def test_removed_file_creates_delta(self, fake_experiment, parquet_catalog_dir):
        """Removing a file should create a delta with status='removed'."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            # First snapshot
            cat.snapshot(str(fake_experiment.experiment_path))
            initial_count = cat.count()

            # Remove a file
            removed_file = fake_experiment.experiment_path / "scratch" / "run0001" / "metadata.json"
            removed_file.unlink()

            # Second snapshot
            added, modified, removed = cat.snapshot(str(fake_experiment.experiment_path))

            assert added == 0
            assert modified == 0
            assert removed == 1

            # Total count should still be same (file is tracked but on_disk=false)
            assert cat.count() == initial_count
            # But on_disk count should be less
            assert cat.count(on_disk_only=True) == initial_count - 1


class TestOnDiskFiltering:
    """Tests for on_disk filtering in queries."""

    def test_count_on_disk_only(self, fake_experiment, parquet_catalog_dir):
        """count(on_disk_only=True) should exclude removed files."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path))

            # Remove two files
            (fake_experiment.experiment_path / "scratch" / "run0001" / "metadata.json").unlink()
            (fake_experiment.experiment_path / "calib" / "calibration.dat").unlink()

            cat.snapshot(str(fake_experiment.experiment_path))

            total = cat.count()
            on_disk = cat.count(on_disk_only=True)

            assert total == fake_experiment.expected_file_count
            assert on_disk == fake_experiment.expected_file_count - 2

    def test_find_removed_only(self, fake_experiment, parquet_catalog_dir):
        """find(removed_only=True) should only return removed files."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path))

            # Remove a file
            (fake_experiment.experiment_path / "scratch" / "run0001" / "metadata.json").unlink()

            cat.snapshot(str(fake_experiment.experiment_path))

            removed = cat.find("%", removed_only=True)
            assert len(removed) == 1
            assert "metadata.json" in removed[0].path


class TestFileRestoration:
    """Tests for file restoration (re-appearing files)."""

    def test_restored_file_becomes_on_disk_true(self, fake_experiment, parquet_catalog_dir):
        """A file that reappears should have on_disk=True again."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            # First snapshot
            cat.snapshot(str(fake_experiment.experiment_path))

            # Remove file
            removed_file = fake_experiment.experiment_path / "scratch" / "run0001" / "metadata.json"
            removed_file.unlink()

            # Second snapshot (file removed)
            cat.snapshot(str(fake_experiment.experiment_path))
            assert cat.count(on_disk_only=True) == fake_experiment.expected_file_count - 1

            # Restore file
            removed_file.write_text("restored content")

            # Third snapshot (file restored)
            added, modified, removed = cat.snapshot(str(fake_experiment.experiment_path))

            # Should show as added (since it was removed and came back)
            assert added == 1
            assert cat.count(on_disk_only=True) == fake_experiment.expected_file_count


class TestConsolidate:
    """Tests for consolidation of base + deltas."""

    def test_consolidate_merges_files(self, fake_experiment, parquet_catalog_dir):
        """Consolidate should merge base + deltas into new base."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            # First snapshot (base)
            cat.snapshot(str(fake_experiment.experiment_path))

            # Add file (delta 1)
            (fake_experiment.experiment_path / "new1.txt").write_text("new")
            cat.snapshot(str(fake_experiment.experiment_path))

            # Add another file (delta 2)
            (fake_experiment.experiment_path / "new2.txt").write_text("new")
            cat.snapshot(str(fake_experiment.experiment_path))

            exp_dir = list(parquet_catalog_dir.iterdir())[0]
            files_before = list(exp_dir.glob("*.parquet"))
            assert len(files_before) == 3  # 1 base + 2 deltas

            # Consolidate
            stats = cat.consolidate()

            assert stats["experiments"] == 1
            assert stats["files_removed"] == 3  # old base + 2 deltas

            files_after = list(exp_dir.glob("*.parquet"))
            assert len(files_after) == 1  # new base only

            # Data should still be correct
            assert cat.count() == fake_experiment.expected_file_count + 2

    def test_consolidate_with_archive(self, fake_experiment, parquet_catalog_dir, tmp_path):
        """Consolidate with archive should move old files instead of deleting."""
        archive_dir = tmp_path / "archive"

        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path))

            (fake_experiment.experiment_path / "new.txt").write_text("new")
            cat.snapshot(str(fake_experiment.experiment_path))

            stats = cat.consolidate(archive_dir=str(archive_dir))

            assert stats["files_archived"] == 2  # old base + delta
            assert archive_dir.exists()
            assert len(list(archive_dir.rglob("*.parquet"))) == 2


class TestListSnapshots:
    """Tests for listing snapshot files."""

    def test_list_snapshots(self, fake_experiment, parquet_catalog_dir):
        """list_snapshots should return info about all snapshot files."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path))

            (fake_experiment.experiment_path / "new.txt").write_text("new")
            cat.snapshot(str(fake_experiment.experiment_path))

            snapshots = cat.list_snapshots()

            assert len(snapshots) == 2
            types = {s["type"] for s in snapshots}
            assert types == {"base", "delta"}


class TestBrowseOperations:
    """Tests for browse operations (ls, find, tree)."""

    def test_ls_returns_files(self, fake_experiment, parquet_catalog_dir):
        """ls should return files in the specified directory."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path))

            run0001_path = str(fake_experiment.experiment_path / "scratch" / "run0001")
            files = cat.ls(run0001_path)

            assert len(files) == 3
            filenames = {f.filename for f in files}
            assert filenames == {"image_0001.h5", "image_0002.h5", "metadata.json"}

    def test_find_by_pattern(self, fake_experiment, parquet_catalog_dir):
        """find should match filename patterns."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path))

            results = cat.find("%.h5")
            assert len(results) == 3  # image_0001.h5, image_0002.h5, data.h5

    def test_find_with_size_filter(self, fake_experiment, parquet_catalog_dir):
        """find should filter by size."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path))

            results = cat.find("%", size_gt=1000)
            assert len(results) == 2  # image_0001.h5 (1024) and image_0002.h5 (2048)


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_directory(self, tmp_path, parquet_catalog_dir):
        """Snapshot of empty directory should return (0, 0, 0)."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            added, modified, removed = cat.snapshot(str(empty_dir))
            assert added == 0
            assert modified == 0
            assert removed == 0

    def test_deep_nesting(self, deep_nested_structure, parquet_catalog_dir):
        """Should handle deeply nested directories."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            added, _, _ = cat.snapshot(str(deep_nested_structure.experiment_path))
            assert added == 1

            results = cat.find("%/deep_file.txt")
            assert len(results) == 1

    def test_special_characters(self, special_chars_structure, parquet_catalog_dir):
        """Should handle special characters in filenames."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            added, _, _ = cat.snapshot(str(special_chars_structure.experiment_path))
            assert added == special_chars_structure.expected_file_count


class TestParallelProcessing:
    """Tests for parallel processing."""

    def test_parallel_snapshot(self, fake_experiment, parquet_catalog_dir):
        """Snapshot should work with multiple workers."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            added, _, _ = cat.snapshot(
                str(fake_experiment.experiment_path),
                workers=2,
            )
            assert added == fake_experiment.expected_file_count

    def test_parallel_with_checksum(self, fake_experiment, parquet_catalog_dir):
        """Parallel checksum computation should work correctly."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                compute_checksum=True,
                workers=4,
            )

            results = cat.find("%")
            for r in results:
                assert r.checksum is not None
