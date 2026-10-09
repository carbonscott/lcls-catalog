"""Tests for the materialized current state under <catalog>/_state/current."""

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

import duckdb

from lcls_catalog.parquet_catalog import ParquetCatalog

ROWS_SQL = """SELECT path, size, mtime, experiment, indexed_at, on_disk
              FROM files ORDER BY path, experiment"""


def current_files(catalog_dir):
    return sorted((Path(catalog_dir) / "_state" / "current").glob("*.parquet"))


def rows_without_current(cat):
    """Query result when no materialized state exists (live dedup fallback)."""
    state = cat.catalog_dir / "_state"
    hidden = cat.catalog_dir.parent / "_state.hidden"
    shutil.move(str(state), str(hidden))
    try:
        return cat.query(ROWS_SQL)
    finally:
        shutil.move(str(hidden), str(state))


def file_rows(path):
    return duckdb.execute(
        f"SELECT * FROM read_parquet('{path}') ORDER BY path").fetchall()


def history(cat, exp_path):
    """Snapshot after each kind of change; yield after every snapshot."""
    cat.snapshot(str(exp_path), experiment="xpptest01")
    yield "base"

    (exp_path / "results" / "new.h5").write_bytes(b"n" * 300)
    cat.snapshot(str(exp_path), experiment="xpptest01")
    yield "added"

    (exp_path / "calib" / "calibration.dat").write_bytes(b"m" * 999)
    cat.snapshot(str(exp_path), experiment="xpptest01")
    yield "modified"

    victim = exp_path / "scratch" / "run0002" / "data.h5"
    victim.unlink()
    cat.snapshot(str(exp_path), experiment="xpptest01")
    yield "removed"

    victim.write_bytes(b"r" * 512)
    cat.snapshot(str(exp_path), experiment="xpptest01")
    yield "restored"


class TestCurrentState:

    def test_snapshot_writes_current_named_after_newest_input(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        with ParquetCatalog(str(catalog)) as cat:
            for step in history(cat, fake_experiment.experiment_path):
                newest = sorted((catalog / "xpptest01").glob("*.parquet"))[-1]
                names = [p.name for p in current_files(catalog)]
                assert f"xpptest01__{newest.stem}.parquet" in names, step
                # newest plus at most one previous version is kept
                assert len(names) <= 2

    def test_query_matches_live_dedup_at_every_step(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        with ParquetCatalog(str(catalog)) as cat:
            for step in history(cat, fake_experiment.experiment_path):
                assert cat.query(ROWS_SQL) == rows_without_current(cat), step

    def test_restored_file_is_on_disk(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        with ParquetCatalog(str(catalog)) as cat:
            for _ in history(cat, fake_experiment.experiment_path):
                pass
            on_disk = dict(cat.query("SELECT filename, on_disk FROM files"))
            assert on_disk["data.h5"] is True
            assert cat.count(on_disk_only=True) == cat.count()

    def test_stale_current_falls_back_to_live_dedup(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        exp_path = fake_experiment.experiment_path
        with ParquetCatalog(str(catalog)) as cat:
            cat.snapshot(str(exp_path), experiment="xpptest01")
            stale = current_files(catalog)
            (exp_path / "results" / "late.h5").write_bytes(b"l" * 10)
            cat.snapshot(str(exp_path), experiment="xpptest01")
            # Put back only the pre-delta version: the query must notice.
            for p in current_files(catalog):
                if p not in stale:
                    p.unlink()
            assert "late.h5" in [r[0] for r in cat.query("SELECT filename FROM files")]
            assert cat.query(ROWS_SQL) == rows_without_current(cat)

    def test_incremental_refresh_equals_full_rebuild(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        with ParquetCatalog(str(catalog)) as cat:
            modes = []
            for _ in history(cat, fake_experiment.experiment_path):
                pass
            incremental = file_rows(current_files(catalog)[-1])
            shutil.rmtree(catalog / "_state")
            modes.append(cat.refresh_current(catalog / "xpptest01"))
            assert modes == ["full"]
            assert file_rows(current_files(catalog)[-1]) == incremental

    def test_refresh_modes(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        exp_path = fake_experiment.experiment_path
        with ParquetCatalog(str(catalog)) as cat:
            cat.snapshot(str(exp_path), experiment="xpptest01")
            assert cat.refresh_current(catalog / "xpptest01") == "fresh"
            # Write a delta without refreshing, then refresh by hand.
            (exp_path / "results" / "x.h5").write_bytes(b"x")
            cat.refresh_current = lambda *a, **k: "skipped"  # snapshot's own refresh
            cat.snapshot(str(exp_path), experiment="xpptest01")
            del cat.refresh_current
            assert cat.refresh_current(catalog / "xpptest01") == "incremental"

    def test_refresh_all_and_orphan_cleanup(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        with ParquetCatalog(str(catalog)) as cat:
            cat.snapshot(str(fake_experiment.experiment_path), experiment="xpptest01")
            cat.snapshot(str(fake_experiment.experiment_path), experiment="gone01")
            shutil.rmtree(catalog / "gone01")
            outcome = cat.refresh_all()
            assert outcome == {"fresh": ["xpptest01"]}
            assert [p.name.split("__")[0] for p in current_files(catalog)] == ["xpptest01"]

    def test_mixed_fresh_base_only_and_stale_experiments(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        exp_path = fake_experiment.experiment_path
        with ParquetCatalog(str(catalog)) as cat:
            for exp in ("fresh01", "baseonly01", "stale01"):
                cat.snapshot(str(exp_path), experiment=exp)
            (exp_path / "results" / "more.h5").write_bytes(b"m" * 7)
            cat.snapshot(str(exp_path), experiment="stale01")
            for p in current_files(catalog):
                if p.name.startswith(("baseonly01__", "stale01__delta_")):
                    p.unlink()
            expected = rows_without_current(cat)
            assert cat.query(ROWS_SQL) == expected
            n = fake_experiment.expected_file_count
            assert cat.count() == 3 * n + 1

    def test_consolidate_preserves_results(self, fake_experiment, tmp_path):
        catalog = tmp_path / "cat"
        with ParquetCatalog(str(catalog)) as cat:
            for _ in history(cat, fake_experiment.experiment_path):
                pass
            before = cat.query("SELECT path, size, mtime, on_disk FROM files ORDER BY path")
            stats = cat.consolidate()
            assert stats["experiments"] == 1
            assert len(list((catalog / "xpptest01").glob("*.parquet"))) == 1
            after = cat.query("SELECT path, size, mtime, on_disk FROM files ORDER BY path")
            assert after == before
            newest = next((catalog / "xpptest01").glob("base_*.parquet"))
            names = [p.name for p in current_files(catalog)]
            assert f"xpptest01__{newest.stem}.parquet" in names

    def test_empty_catalog_queries_return_nothing(self, tmp_path):
        with ParquetCatalog(str(tmp_path / "cat")) as cat:
            assert cat.count() == 0
            assert cat.query("SELECT * FROM files") == []


DIRS_SQL = """SELECT path, level, files, bytes, direct_files, direct_bytes
              FROM dirs ORDER BY path"""


def without_state(cat, kinds, sql):
    """Query result with the given materialized kinds hidden (live fallback)."""
    moved = []
    try:
        for kind in kinds:
            src = cat.catalog_dir / "_state" / kind
            if src.exists():
                dst = cat.catalog_dir.parent / f"hidden-{kind}"
                shutil.move(str(src), str(dst))
                moved.append((dst, src))
        return cat.query(sql)
    finally:
        for dst, src in moved:
            shutil.move(str(dst), str(src))


class TestDirs:

    def test_recursive_totals(self, fake_experiment, tmp_path):
        root = str(fake_experiment.experiment_path)
        with ParquetCatalog(str(tmp_path / "cat")) as cat:
            cat.snapshot(root, experiment="xpptest01")
            got = {r[0][len(root):] or "/": r[1:] for r in cat.query(DIRS_SQL)}
        assert got == {
            "/": (0, 6, 4068, 0, 0),
            "/calib": (1, 1, 128, 1, 128),
            "/results": (1, 1, 256, 1, 256),
            "/scratch": (1, 4, 3684, 0, 0),
            "/scratch/run0001": (2, 3, 3172, 3, 3172),
            "/scratch/run0002": (2, 1, 512, 1, 512),
        }

    def test_removed_files_do_not_count(self, fake_experiment, tmp_path):
        exp_path = fake_experiment.experiment_path
        with ParquetCatalog(str(tmp_path / "cat")) as cat:
            cat.snapshot(str(exp_path), experiment="xpptest01")
            (exp_path / "scratch" / "run0002" / "data.h5").unlink()
            cat.snapshot(str(exp_path), experiment="xpptest01")
            paths = {r[0][len(str(exp_path)):]: r[2:4] for r in cat.query(DIRS_SQL)}
        assert "/scratch/run0002" not in paths
        assert paths["/scratch"] == (3, 3172)

    def test_materialized_matches_live_at_every_step(self, fake_experiment, tmp_path):
        with ParquetCatalog(str(tmp_path / "cat")) as cat:
            for step in history(cat, fake_experiment.experiment_path):
                fresh = cat.query(DIRS_SQL)
                assert fresh == without_state(cat, ["dirs"], DIRS_SQL), step
                assert fresh == without_state(cat, ["dirs", "current"], DIRS_SQL), step

    def test_files_and_dirs_together(self, fake_experiment, tmp_path):
        with ParquetCatalog(str(tmp_path / "cat")) as cat:
            cat.snapshot(str(fake_experiment.experiment_path), experiment="xpptest01")
            rows = cat.query("""SELECT d.files, COUNT(*) FROM dirs d JOIN files f
                                ON f.parent_path = d.path GROUP BY d.files ORDER BY 1""")
        assert rows == [(1, 3), (3, 3)]

    def test_empty_catalog_dirs(self, tmp_path):
        with ParquetCatalog(str(tmp_path / "cat")) as cat:
            assert cat.query("SELECT * FROM dirs") == []
