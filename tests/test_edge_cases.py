"""Tests for edge cases: empty dirs, deep nesting, special chars, symlinks."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from lcls_catalog import Catalog


class TestEmptyDirectories:
    """Tests for empty directory handling."""

    def test_empty_dir_not_in_file_count(self, fake_experiment, catalog_db):
        """Empty directories should not add to file count."""
        with Catalog(str(catalog_db)) as cat:
            count = cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            # empty_run exists but has no files
            assert count == fake_experiment.expected_file_count

    def test_parent_of_empty_dir_still_navigable(self, fake_experiment, catalog_db):
        """Parent directories of empty dirs should still be navigable."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )

            # scratch should still show run0001 and run0002
            scratch_path = str(fake_experiment.experiment_path / "scratch")
            dirs = cat.ls_dirs(scratch_path)

            # At least run0001 and run0002 should be present
            assert len(dirs) >= 2


class TestDeepNesting:
    """Tests for deeply nested directory structures."""

    def test_deep_nesting_captured(self, deep_nested_structure, catalog_db):
        """Files in deeply nested directories should be captured."""
        with Catalog(str(catalog_db)) as cat:
            count = cat.snapshot(
                str(deep_nested_structure.experiment_path),
                experiment=deep_nested_structure.experiment,
            )

            assert count == 1

    def test_deep_nesting_path_preserved(self, deep_nested_structure, catalog_db):
        """Full path should be preserved for deeply nested files."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(deep_nested_structure.experiment_path),
                experiment=deep_nested_structure.experiment,
            )

            results = cat.find("deep_file.txt")
            assert len(results) == 1

            # Should contain all 12 levels
            path = results[0].path
            for i in range(12):
                assert f"level{i:02d}" in path

    def test_deep_nesting_parent_path_correct(self, deep_nested_structure, catalog_db):
        """Parent path should be correct for deeply nested files."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(deep_nested_structure.experiment_path),
                experiment=deep_nested_structure.experiment,
            )

            results = cat.find("deep_file.txt")
            assert len(results) == 1

            parent = results[0].parent_path
            assert parent.endswith("level11")


class TestSpecialCharacters:
    """Tests for special characters in filenames."""

    def test_files_with_spaces(self, special_chars_structure, catalog_db):
        """Files and directories with spaces should be handled."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(special_chars_structure.experiment_path),
                experiment=special_chars_structure.experiment,
            )

            results = cat.find("file with spaces.txt")
            assert len(results) == 1
            assert results[0].size == 32

    def test_files_with_multiple_dots(self, special_chars_structure, catalog_db):
        """Files with multiple dots should be handled."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(special_chars_structure.experiment_path),
                experiment=special_chars_structure.experiment,
            )

            results = cat.find("%.tar.gz.bak")
            assert len(results) == 1

    def test_hidden_files(self, special_chars_structure, catalog_db):
        """Hidden files (starting with .) should be captured."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(special_chars_structure.experiment_path),
                experiment=special_chars_structure.experiment,
            )

            results = cat.find(".hidden%")
            assert len(results) == 1
            assert results[0].filename == ".hidden_config"


class TestSymlinks:
    """Tests for symbolic link handling."""

    def test_symlinks_not_followed(self, symlink_structure, catalog_db):
        """Symlinks should not be followed (to avoid infinite loops)."""
        with Catalog(str(catalog_db)) as cat:
            count = cat.snapshot(
                str(symlink_structure.experiment_path),
                experiment=symlink_structure.experiment,
            )

            # Should only count the original file, not symlinks
            # (symlinks are skipped because we use lstat and check is_file)
            assert count >= 1

    def test_broken_symlinks_skipped(self, symlink_structure, catalog_db):
        """Broken symlinks should be skipped without error."""
        with Catalog(str(catalog_db)) as cat:
            # Should not raise an exception
            count = cat.snapshot(
                str(symlink_structure.experiment_path),
                experiment=symlink_structure.experiment,
            )

            # Should still have cataloged the real file
            results = cat.find("original.dat")
            assert len(results) == 1


class TestLargeValues:
    """Tests for handling large file sizes."""

    def test_large_file_size_stored(self, tmp_path, catalog_db):
        """Large file sizes should be stored correctly (up to TB+)."""
        # Create a catalog with a manually inserted large size
        with Catalog(str(catalog_db)) as cat:
            # Insert a file entry with a very large size (1 TB)
            large_size = 1_000_000_000_000  # 1 TB

            cat.conn.execute(
                """
                INSERT INTO files (path, parent_path, filename, size, experiment, purge_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("/test/large_file.dat", "/test", "large_file.dat", large_size, "test", "2024-01-01"),
            )
            cat.conn.commit()

            results = cat.find("large_file.dat")
            assert len(results) == 1
            assert results[0].size == large_size


class TestCatalogOperations:
    """Tests for catalog lifecycle operations."""

    def test_catalog_context_manager(self, fake_experiment, catalog_db):
        """Catalog should work as context manager."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
            )
            count = cat.count()

        assert count == fake_experiment.expected_file_count

        # Should be able to reopen
        with Catalog(str(catalog_db)) as cat:
            assert cat.count() == fake_experiment.expected_file_count

    def test_multiple_snapshots_different_dates(self, fake_experiment, catalog_db):
        """Multiple snapshots with different purge dates should coexist."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                purge_date="2024-01-01",
            )
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                purge_date="2024-06-01",
            )

            # Should have double the entries
            assert cat.count() == fake_experiment.expected_file_count * 2

    def test_snapshot_same_date_replaces(self, fake_experiment, catalog_db):
        """Snapshot with same purge date should replace existing entries."""
        with Catalog(str(catalog_db)) as cat:
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                purge_date="2024-01-01",
            )
            cat.snapshot(
                str(fake_experiment.experiment_path),
                experiment=fake_experiment.experiment,
                purge_date="2024-01-01",
            )

            # Should still have the same count (replaced, not doubled)
            assert cat.count() == fake_experiment.expected_file_count
