"""Tests for Parquet catalog functionality."""

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


class TestParquetSnapshot:
    """Tests for ParquetCatalog.snapshot()."""

    def test_snapshot_captures_all_files(self, fake_experiment, parquet_catalog_dir):
        """Snapshot should capture all files in the directory tree."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )
            assert count == fake_experiment.expected_file_count

    def test_snapshot_total_size(self, fake_experiment, parquet_catalog_dir):
        """Snapshot should capture correct total size."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )
            assert cat.total_size() == fake_experiment.expected_total_size

    def test_snapshot_creates_parquet_file(self, fake_experiment, parquet_catalog_dir):
        """Snapshot should create a Parquet file."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                purge_date="2024-06-01",
            )

        # Filename is now {path_hash}_{purge_date}.parquet
        parquet_files = list(parquet_catalog_dir.glob("*_2024-06-01.parquet"))
        assert len(parquet_files) == 1

    def test_snapshot_with_workers(self, fake_experiment, parquet_catalog_dir):
        """Snapshot should work with multiple workers."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                workers=2,
            )
            assert count == fake_experiment.expected_file_count

    def test_snapshot_with_checksum(self, fake_experiment, parquet_catalog_dir):
        """Checksums should be computed when requested."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                compute_checksum=True,
            )

            results = cat.find("metadata.json")
            assert len(results) == 1
            assert results[0].checksum is not None
            assert len(results[0].checksum) == 64


class TestParquetBrowse:
    """Tests for ParquetCatalog browse operations."""

    def test_ls_returns_files(self, fake_experiment, parquet_catalog_dir):
        """ls should return files in the specified directory."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            run0001_path = str(fake_experiment.experiment_path / "scratch" / "run0001")
            files = cat.ls(run0001_path)

            assert len(files) == 3
            filenames = {f.filename for f in files}
            assert filenames == {"image_0001.h5", "image_0002.h5", "metadata.json"}

    def test_ls_dirs_returns_subdirectories(self, fake_experiment, parquet_catalog_dir):
        """ls_dirs should return subdirectories with stats."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            scratch_path = str(fake_experiment.experiment_path / "scratch")
            dirs = cat.ls_dirs(scratch_path)

            dirnames = {d.dirname for d in dirs}
            assert "run0001" in dirnames
            assert "run0002" in dirnames

    def test_find_by_pattern(self, fake_experiment, parquet_catalog_dir):
        """find should match filename patterns."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%.h5")
            assert len(results) == 3

    def test_find_with_size_filter(self, fake_experiment, parquet_catalog_dir):
        """find should filter by size."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%", size_gt=1000)
            assert len(results) == 2  # image_0001.h5 (1024) and image_0002.h5 (2048)

    def test_tree_output(self, fake_experiment, parquet_catalog_dir):
        """tree should generate ASCII tree output."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            tree_output = cat.tree(str(fake_experiment.experiment_path), depth=2)
            assert "scratch/" in tree_output
            assert "results/" in tree_output


class TestParquetEdgeCases:
    """Edge case tests for ParquetCatalog."""

    def test_empty_directory(self, tmp_path, parquet_catalog_dir):
        """Snapshot of empty directory should return 0."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            count = cat.snapshot(str(empty_dir))
            assert count == 0

    def test_deep_nesting(self, deep_nested_structure, parquet_catalog_dir):
        """Should handle deeply nested directories."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            count = cat.snapshot(
                str(deep_nested_structure.experiment_path),
                experiment=deep_nested_structure.experiment,
            )
            assert count == 1

            results = cat.find("deep_file.txt")
            assert len(results) == 1

    def test_special_characters(self, special_chars_structure, parquet_catalog_dir):
        """Should handle special characters in filenames."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            count = cat.snapshot(
                str(special_chars_structure.experiment_path),
                experiment=special_chars_structure.experiment,
            )
            assert count == special_chars_structure.expected_file_count


class TestParquetParallel:
    """Tests for parallel processing."""

    def test_parallel_checksum(self, fake_experiment, parquet_catalog_dir):
        """Parallel checksum computation should work correctly."""
        with ParquetCatalog(str(parquet_catalog_dir)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                compute_checksum=True,
                workers=4,
            )

            results = cat.find("%")
            # All files should have checksums
            for r in results:
                assert r.checksum is not None

    def test_parallel_results_match_sequential(self, fake_experiment, tmp_path):
        """Parallel and sequential should produce same results."""
        seq_dir = tmp_path / "seq_catalog"
        par_dir = tmp_path / "par_catalog"

        # Sequential
        with ParquetCatalog(str(seq_dir)) as cat:
            seq_count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                workers=1,
            )
            seq_total = cat.total_size()

        # Parallel
        with ParquetCatalog(str(par_dir)) as cat:
            par_count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                workers=4,
            )
            par_total = cat.total_size()

        assert seq_count == par_count
        assert seq_total == par_total


class TestParquetStreaming:
    """Tests for streaming writes."""

    def test_streaming_produces_same_results(self, fake_experiment, tmp_path):
        """Different batch sizes should produce identical catalogs."""
        small_batch_dir = tmp_path / "small_batch"
        large_batch_dir = tmp_path / "large_batch"

        # Small batch (forces multiple writes)
        with ParquetCatalog(str(small_batch_dir)) as cat:
            small_count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                batch_size=2,  # Very small to force multiple batches
            )
            small_total = cat.total_size()
            small_files = sorted([f.path for f in cat.find("%")])

        # Large batch (single write)
        with ParquetCatalog(str(large_batch_dir)) as cat:
            large_count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                batch_size=100000,  # Large enough to fit all files
            )
            large_total = cat.total_size()
            large_files = sorted([f.path for f in cat.find("%")])

        assert small_count == large_count
        assert small_total == large_total
        assert small_files == large_files
