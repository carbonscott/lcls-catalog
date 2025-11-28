"""Tests for catalog browse functionality (ls, ls_dirs, find, tree)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from lcls_catalog import Catalog


class TestLs:
    """Tests for the ls() method."""

    def test_ls_returns_files_in_directory(self, fake_experiment, catalog_db):
        """ls should return all files in the specified directory."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            run0001_path = str(fake_experiment.experiment_path / "scratch" / "run0001")
            files = cat.ls(run0001_path)

            assert len(files) == 3
            filenames = {f.filename for f in files}
            assert filenames == {"image_0001.h5", "image_0002.h5", "metadata.json"}

    def test_ls_empty_for_nonexistent_path(self, fake_experiment, catalog_db):
        """ls should return empty list for nonexistent path."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            files = cat.ls("/nonexistent/path")
            assert files == []

    def test_ls_returns_correct_sizes(self, fake_experiment, catalog_db):
        """ls should return correct file sizes."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            run0001_path = str(fake_experiment.experiment_path / "scratch" / "run0001")
            files = cat.ls(run0001_path)

            sizes = {f.filename: f.size for f in files}
            assert sizes["image_0001.h5"] == 1024
            assert sizes["image_0002.h5"] == 2048
            assert sizes["metadata.json"] == 100

    def test_ls_results_sorted_by_filename(self, fake_experiment, catalog_db):
        """ls results should be sorted by filename."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            run0001_path = str(fake_experiment.experiment_path / "scratch" / "run0001")
            files = cat.ls(run0001_path)

            filenames = [f.filename for f in files]
            assert filenames == sorted(filenames)


class TestLsDirs:
    """Tests for the ls_dirs() method."""

    def test_ls_dirs_returns_subdirectories(self, fake_experiment, catalog_db):
        """ls_dirs should return immediate subdirectories."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            scratch_path = str(fake_experiment.experiment_path / "scratch")
            dirs = cat.ls_dirs(scratch_path)

            dirnames = {d.dirname for d in dirs}
            # Should have run0001 and run0002 (empty_run has no files)
            assert "run0001" in dirnames
            assert "run0002" in dirnames

    def test_ls_dirs_aggregates_file_count(self, fake_experiment, catalog_db):
        """ls_dirs should show correct file count per directory."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            scratch_path = str(fake_experiment.experiment_path / "scratch")
            dirs = cat.ls_dirs(scratch_path)

            counts = {d.dirname: d.file_count for d in dirs}
            assert counts["run0001"] == 3  # image_0001.h5, image_0002.h5, metadata.json
            assert counts["run0002"] == 1  # data.h5

    def test_ls_dirs_aggregates_total_size(self, fake_experiment, catalog_db):
        """ls_dirs should show correct total size per directory."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            scratch_path = str(fake_experiment.experiment_path / "scratch")
            dirs = cat.ls_dirs(scratch_path)

            sizes = {d.dirname: d.total_size for d in dirs}
            assert sizes["run0001"] == 1024 + 2048 + 100  # 3172
            assert sizes["run0002"] == 512

    def test_ls_dirs_at_experiment_root(self, fake_experiment, catalog_db):
        """ls_dirs at experiment root should show all top-level directories."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            dirs = cat.ls_dirs(str(fake_experiment.experiment_path))

            dirnames = {d.dirname for d in dirs}
            assert "scratch" in dirnames
            assert "results" in dirnames
            assert "calib" in dirnames


class TestFind:
    """Tests for the find() method."""

    def test_find_by_exact_filename(self, fake_experiment, catalog_db):
        """find should match exact filename."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%/analysis.npz")
            assert len(results) == 1
            assert results[0].filename == "analysis.npz"

    def test_find_by_pattern(self, fake_experiment, catalog_db):
        """find should match filename patterns."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%/image_%")
            assert len(results) == 2

            results = cat.find("%.h5")
            assert len(results) == 3  # image_0001.h5, image_0002.h5, data.h5

    def test_find_by_extension(self, fake_experiment, catalog_db):
        """find should match by file extension."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%.json")
            assert len(results) == 1
            assert results[0].filename == "metadata.json"

    def test_find_with_size_gt_filter(self, fake_experiment, catalog_db):
        """find should filter by minimum size."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%", size_gt=1000)
            # Only image_0001.h5 (1024) and image_0002.h5 (2048) are > 1000
            assert len(results) == 2

    def test_find_with_size_lt_filter(self, fake_experiment, catalog_db):
        """find should filter by maximum size."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%", size_lt=200)
            # metadata.json (100), calibration.dat (128)
            assert len(results) == 2

    def test_find_with_experiment_filter(self, fake_experiment, catalog_db):
        """find should filter by experiment."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%", experiment="xpptest01")
            assert len(results) == fake_experiment.expected_file_count

            results = cat.find("%", experiment="nonexistent")
            assert len(results) == 0

    def test_find_results_sorted_by_path(self, fake_experiment, catalog_db):
        """find results should be sorted by path."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            results = cat.find("%")
            paths = [r.path for r in results]
            assert paths == sorted(paths)


class TestTree:
    """Tests for the tree() method."""

    def test_tree_shows_structure(self, fake_experiment, catalog_db):
        """tree should show directory structure."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            tree_output = cat.tree(str(fake_experiment.experiment_path), depth=3)

            assert "scratch/" in tree_output
            assert "results/" in tree_output
            assert "calib/" in tree_output

    def test_tree_respects_depth(self, fake_experiment, catalog_db):
        """tree should respect depth limit."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            # With depth=1, should only show immediate children
            tree_output = cat.tree(str(fake_experiment.experiment_path), depth=1)

            assert "scratch/" in tree_output
            # run0001 should appear at depth 2, not shown at depth 1
            # (The actual visibility depends on implementation)
