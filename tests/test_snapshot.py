"""Tests for catalog snapshot functionality."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from lcls_catalog import Catalog


class TestSnapshot:
    """Tests for the snapshot() method."""

    def test_snapshot_captures_all_files(self, fake_experiment, catalog_db):
        """Snapshot should capture all files in the directory tree."""
        with Catalog(str(catalog_db)) as cat:
            count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            assert count == fake_experiment.expected_file_count

    def test_snapshot_total_size(self, fake_experiment, catalog_db):
        """Snapshot should capture correct total size."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            assert cat.total_size() == fake_experiment.expected_total_size

    def test_snapshot_file_sizes_accurate(self, fake_experiment, catalog_db):
        """Individual file sizes should match actual file sizes."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            # Check a specific file
            results = cat.find("image_0001.h5")
            assert len(results) == 1
            assert results[0].size == 1024

            results = cat.find("image_0002.h5")
            assert len(results) == 1
            assert results[0].size == 2048

    def test_snapshot_paths_stored_correctly(self, fake_experiment, catalog_db):
        """Full paths should be stored correctly."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("metadata.json")
            assert len(results) == 1
            assert "run0001" in results[0].path
            assert results[0].filename == "metadata.json"

    def test_snapshot_parent_paths_computed(self, fake_experiment, catalog_db):
        """Parent paths should be computed correctly."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("image_0001.h5")
            assert len(results) == 1
            assert results[0].parent_path.endswith("run0001")

    def test_snapshot_captures_mtime(self, fake_experiment, catalog_db):
        """Modification time should be captured."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("analysis.npz")
            assert len(results) == 1
            assert results[0].mtime is not None
            assert results[0].mtime > 0

    def test_snapshot_captures_permissions(self, fake_experiment, catalog_db):
        """File permissions should be captured."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("calibration.dat")
            assert len(results) == 1
            assert results[0].permissions is not None

    def test_snapshot_extracts_run_number(self, fake_experiment, catalog_db):
        """Run numbers should be extracted from paths."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("image_0001.h5")
            assert len(results) == 1
            assert results[0].run == 1

            results = cat.find("data.h5")
            assert len(results) == 1
            assert results[0].run == 2

    def test_snapshot_stores_experiment(self, fake_experiment, catalog_db):
        """Experiment ID should be stored."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("image_0001.h5")
            assert len(results) == 1
            assert results[0].experiment == fake_experiment.experiment

    def test_snapshot_with_checksum(self, fake_experiment, catalog_db):
        """Checksums should be computed when requested."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                compute_checksum=True,
            )

            results = cat.find("metadata.json")
            assert len(results) == 1
            assert results[0].checksum is not None
            assert len(results[0].checksum) == 64  # SHA-256 hex digest

    def test_snapshot_without_checksum(self, fake_experiment, catalog_db):
        """Checksums should be None when not requested."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                compute_checksum=False,
            )

            results = cat.find("metadata.json")
            assert len(results) == 1
            assert results[0].checksum is None

    def test_snapshot_purge_date(self, fake_experiment, catalog_db):
        """Purge date should be stored."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                purge_date="2024-06-01",
            )

            results = cat.find("image_0001.h5")
            assert len(results) == 1
            assert results[0].purge_date == "2024-06-01"
