"""Tests for the resource limits on read queries."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

pytest.importorskip("duckdb")

from lcls_catalog.parquet_catalog import ParquetCatalog, QueryLimitError, QueryLimits

# Needs ~800 MB that cannot spill: a single list aggregate.
BIG_SQL = "SELECT list(range) FROM range(100000000)"
# Takes many seconds on one thread.
SLOW_SQL = "SELECT SUM(hash(range)) FROM range(20000000000)"


def catalog(tmp_path, **limits):
    limits.setdefault("spill_dir", str(tmp_path))
    return ParquetCatalog(str(tmp_path / "cat"), limits=QueryLimits(**limits))


def test_memory_limit_raises_clear_error(tmp_path):
    cat = catalog(tmp_path, memory_limit="50MB", max_spill="10MB", threads=1)
    with pytest.raises(QueryLimitError) as err:
        cat.query(BIG_SQL)
    assert "LCAT_MEMORY_LIMIT" in str(err.value)
    assert "will not help" in str(err.value)


def test_timeout_raises_clear_error(tmp_path):
    cat = catalog(tmp_path, timeout=0.5, threads=1)
    t0 = time.time()
    with pytest.raises(QueryLimitError) as err:
        cat.query(SLOW_SQL)
    assert time.time() - t0 < 10
    assert "LCAT_TIMEOUT" in str(err.value)


def test_timeout_stops_once_rows_flow(tmp_path):
    """A slow consumer of a long listing is not cut off."""
    cat = catalog(tmp_path, timeout=0.5)
    rows = cat.iter_query("SELECT range FROM range(25000)")
    first = next(rows)
    time.sleep(1.0)
    assert 1 + sum(1 for _ in rows) == 25000
    assert isinstance(first, tuple)


def test_spill_dir_is_cleaned_up(tmp_path):
    cat = catalog(tmp_path)
    cat.query("SELECT 1")
    with pytest.raises(QueryLimitError):
        catalog(tmp_path, memory_limit="50MB", max_spill="10MB", threads=1).query(BIG_SQL)
    assert not list(tmp_path.glob("lcat-duckdb-*"))


def test_limits_from_env(monkeypatch):
    monkeypatch.setenv("LCAT_MEMORY_LIMIT", "2GB")
    monkeypatch.setenv("LCAT_THREADS", "3")
    monkeypatch.setenv("LCAT_TIMEOUT", "0")
    monkeypatch.setenv("LCAT_SPILL_DIR", "/somewhere")
    monkeypatch.setenv("LCAT_MAX_SPILL", "1GB")
    assert QueryLimits.from_env() == QueryLimits("2GB", 3, 0.0, "/somewhere", "1GB")


def test_cli_exits_3_with_message_on_limit(tmp_path):
    env = dict(os.environ, LCAT_MEMORY_LIMIT="50MB", LCAT_MAX_SPILL="10MB",
               LCAT_THREADS="1", LCAT_SPILL_DIR=str(tmp_path),
               PYTHONPATH=str(Path(__file__).parent.parent / "src"))
    proc = subprocess.run(
        [sys.executable, "-m", "lcls_catalog.cli", "query", str(tmp_path / "cat"), BIG_SQL],
        env=env, capture_output=True, text=True)
    assert proc.returncode == 3
    assert proc.stderr.startswith("lcat: query stopped")
    assert "Traceback" not in proc.stderr
