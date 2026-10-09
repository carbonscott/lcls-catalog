"""Parquet-based catalog with incremental delta updates.

Parallelism Architecture
========================
The `--workers` flag controls parallelism in two sequential phases:

  Phase 1: Directory Walking (_parallel_walk)
  -------------------------------------------
  - Uses ThreadPoolExecutor to scan multiple directories concurrently
  - Multiple scandir() calls in flight simultaneously
  - Benefits: Hides I/O latency on network filesystems

  Phase 2: File Processing (_process_batch)
  -----------------------------------------
  - ThreadPoolExecutor for lstat() calls (I/O-bound, GIL released)
  - ProcessPoolExecutor when --checksum enabled (CPU-bound SHA256)

  Timeline:
  ┌─────────────────────┐   ┌─────────────────────┐
  │ Phase 1: Walk dirs  │ → │ Phase 2: Process    │
  │ (ThreadPool closes) │   │ (Thread/ProcessPool)│
  └─────────────────────┘   └─────────────────────┘

No conflict between phases - each executor is fully closed before next starts.
"""

import glob
import hashlib
import os
import re
import shutil
import sys
import tempfile
import threading
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import DirSummary, FileEntry

# Materialized state lives under <catalog>/_state/<kind>/, two levels below the
# catalog root so the */*.parquet experiment globs never pick it up.
STATE_DIR = "_state"

# Columns of a deduplicated row, in schema order; on_disk is appended.
COLUMNS = ("path, parent_path, filename, size, mtime, owner, group_name, "
           "permissions, checksum, experiment, run, indexed_at")

# on_disk for a row of a base or delta file: base rows carry on_disk, delta
# rows carry status.
ON_DISK_SQL = """CASE
    WHEN on_disk IS NOT NULL THEN on_disk
    WHEN status IS NOT NULL THEN status != 'removed'
    ELSE true
END"""

# The newest row per path wins.
LATEST_ROW_SQL = "QUALIFY ROW_NUMBER() OVER (PARTITION BY path ORDER BY indexed_at DESC) = 1"

EMPTY_FILES_SQL = """SELECT NULL::VARCHAR AS path, NULL::VARCHAR AS parent_path,
    NULL::VARCHAR AS filename, NULL::BIGINT AS size, NULL::BIGINT AS mtime,
    NULL::VARCHAR AS owner, NULL::VARCHAR AS group_name, NULL::INTEGER AS permissions,
    NULL::VARCHAR AS checksum, NULL::VARCHAR AS experiment, NULL::INTEGER AS run,
    NULL::VARCHAR AS indexed_at, NULL::BOOLEAN AS on_disk
WHERE false"""


def _stem_ts(stem: str) -> str:
    """Timestamp part of a base_/delta_ file stem; sorts snapshots in time."""
    return stem.split("_", 1)[1]


def _snapshot_inputs(exp_dir: Path) -> list[Path]:
    """Base and delta files of one experiment, oldest first."""
    files = [p for p in exp_dir.glob("*.parquet") if p.name.startswith(("base_", "delta_"))]
    return sorted(files, key=lambda p: _stem_ts(p.stem))


def _sql_list(paths) -> str:
    return "[" + ", ".join(f"'{p}'" for p in paths) + "]"


class QueryLimitError(RuntimeError):
    """A query hit lcat's memory or time limit. Retrying will not help."""


LIMIT_ADVICE = """This is a hard limit, not a transient failure: retrying, running it in the
background, or waiting longer will not help. Make the query smaller instead:
  - filter early, e.g. WHERE experiment = '<exp>' or
    WHERE parent_path LIKE '/sdf/data/lcls/ds/<hutch>/<exp>/%'
  - aggregate to fewer groups (GROUP BY experiment, not GROUP BY path)
  - add LIMIT when listing rows"""


@dataclass
class QueryLimits:
    """Resource limits for read queries; they run on shared interactive nodes.

    Each comes from an environment variable so a batch job can raise it.
    """

    memory_limit: str = "8GB"   # LCAT_MEMORY_LIMIT
    threads: int = 8            # LCAT_THREADS
    timeout: float = 90         # LCAT_TIMEOUT, seconds until the first rows; 0 = none
    spill_dir: str = ""         # LCAT_SPILL_DIR, default $TMPDIR or /tmp
    max_spill: str = "4GB"      # LCAT_MAX_SPILL

    @classmethod
    def from_env(cls) -> "QueryLimits":
        env = os.environ.get
        return cls(
            memory_limit=env("LCAT_MEMORY_LIMIT", cls.memory_limit),
            threads=int(env("LCAT_THREADS", min(cls.threads, os.cpu_count() or 1))),
            timeout=float(env("LCAT_TIMEOUT", cls.timeout)),
            spill_dir=env("LCAT_SPILL_DIR") or tempfile.gettempdir(),
            max_spill=env("LCAT_MAX_SPILL", cls.max_spill),
        )


def _scan_directory(dirpath: str) -> tuple[list[str], list[str]]:
    """Scan a single directory and return subdirs and files.

    Args:
        dirpath: Path to directory to scan.

    Returns:
        Tuple of (subdirectory paths, file paths).
    """
    subdirs = []
    files = []
    try:
        with os.scandir(dirpath) as entries:
            for entry in entries:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        subdirs.append(entry.path)
                    else:
                        files.append(entry.path)
                except OSError:
                    # Permission denied or other error
                    continue
    except OSError:
        # Can't read directory
        pass
    return subdirs, files


def _parallel_walk(root: str, workers: int) -> Iterator[str]:
    """Walk directory tree in parallel, yielding file paths.

    Uses a thread pool to scan multiple directories concurrently,
    which can significantly speed up traversal on network filesystems.

    Args:
        root: Root directory to walk.
        workers: Number of parallel workers.

    Yields:
        File paths discovered during traversal.
    """
    # Queue of directories to process
    pending_dirs = deque([root])
    # Files discovered but not yet yielded
    pending_files: list[str] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        while pending_dirs or pending_files:
            # Submit batch of directory scans
            futures = []
            batch_size = min(len(pending_dirs), workers * 2)
            for _ in range(batch_size):
                if pending_dirs:
                    dirpath = pending_dirs.popleft()
                    futures.append(executor.submit(_scan_directory, dirpath))

            # Process results as they complete
            for future in futures:
                subdirs, files = future.result()
                pending_dirs.extend(subdirs)
                pending_files.extend(files)

            # Yield files in batches to avoid memory buildup
            while pending_files:
                yield pending_files.pop()


def _process_file(args: tuple) -> Optional[dict]:
    """Process a single file and return its metadata. Runs in worker process."""
    fpath_str, compute_checksum, experiment, indexed_at = args
    fpath = Path(fpath_str)

    try:
        stat = fpath.lstat()

        checksum = None
        if compute_checksum and fpath.is_file():
            sha256 = hashlib.sha256()
            with open(fpath, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            checksum = sha256.hexdigest()

        # Extract run number
        match = re.search(r"run(\d+)", fpath_str)
        run = int(match.group(1)) if match else None

        return {
            "path": fpath_str,
            "parent_path": str(fpath.parent),
            "filename": fpath.name,
            "size": stat.st_size,
            "mtime": int(stat.st_mtime),
            "owner": str(stat.st_uid),
            "group_name": str(stat.st_gid),
            "permissions": stat.st_mode,
            "checksum": checksum,
            "experiment": experiment,
            "run": run,
            "indexed_at": indexed_at,
        }
    except (OSError, PermissionError):
        return None


class ParquetCatalog:
    """A catalog using Parquet files with base + delta incremental updates."""

    # Unified schema for both base and delta files
    # Base files: on_disk is set, status is NULL
    # Delta files: status is set, on_disk is NULL
    SCHEMA = pa.schema([
        ("path", pa.string()),
        ("parent_path", pa.string()),
        ("filename", pa.string()),
        ("size", pa.int64()),
        ("mtime", pa.int64()),
        ("owner", pa.string()),
        ("group_name", pa.string()),
        ("permissions", pa.int32()),
        ("checksum", pa.string()),
        ("experiment", pa.string()),
        ("run", pa.int32()),
        ("indexed_at", pa.string()),
        ("on_disk", pa.bool_()),     # Set in base files, NULL in deltas
        ("status", pa.string()),      # Set in delta files ("added"/"modified"/"removed"), NULL in base
    ])

    def __init__(self, catalog_dir: str, limits: Optional[QueryLimits] = None):
        """
        Initialize a Parquet catalog.

        Args:
            catalog_dir: Directory to store Parquet files.
            limits: Resource limits for read queries (default: from LCAT_* env vars).
        """
        self.catalog_dir = Path(catalog_dir)
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        self.limits = limits or QueryLimits.from_env()

    def _get_exp_dir(self, root: str, experiment: Optional[str] = None) -> Path:
        """Get directory for a specific experiment."""
        if experiment:
            dir_name = experiment
        else:
            dir_name = hashlib.md5(root.encode()).hexdigest()[:8]
        exp_dir = self.catalog_dir / dir_name
        exp_dir.mkdir(parents=True, exist_ok=True)
        return exp_dir

    def _find_latest_base(self, exp_dir: Path) -> Optional[Path]:
        """Find the most recent base file in an experiment directory."""
        base_files = sorted(exp_dir.glob("base_*.parquet"))
        return base_files[-1] if base_files else None

    def _find_deltas_after_base(self, exp_dir: Path, base_file: Optional[Path]) -> list[Path]:
        """Find all delta files created after the given base file."""
        if not base_file:
            return []
        base_name = base_file.name
        delta_files = sorted(exp_dir.glob("delta_*.parquet"))
        return [d for d in delta_files if d.name > base_name.replace("base_", "delta_")]

    def load_current_state(self, exp_dir: Path) -> dict[str, dict]:
        """
        Reconstruct current state from base + deltas.

        Args:
            exp_dir: Experiment directory containing parquet files.

        Returns:
            Dictionary mapping path to file record.
        """
        base_file = self._find_latest_base(exp_dir)
        if not base_file:
            return {}

        # Start with base
        state = {}
        base_table = pq.read_table(base_file)
        for i in range(base_table.num_rows):
            record = {col: base_table[col][i].as_py() for col in base_table.column_names}
            state[record["path"]] = record

        # Apply deltas in order
        for delta_file in self._find_deltas_after_base(exp_dir, base_file):
            delta_table = pq.read_table(delta_file)
            for i in range(delta_table.num_rows):
                record = {col: delta_table[col][i].as_py() for col in delta_table.column_names}
                path = record["path"]
                status = record.pop("status", None)

                if status == "removed":
                    if path in state:
                        state[path]["on_disk"] = False
                        state[path]["indexed_at"] = record["indexed_at"]
                else:  # added or modified
                    record["on_disk"] = True
                    state[path] = record

        return state

    def snapshot(
        self,
        root: str,
        experiment: Optional[str] = None,
        compute_checksum: bool = False,
        workers: int = 1,
        batch_size: int = 10000,
    ) -> tuple[int, int, int]:
        """
        Walk a directory tree and capture metadata, creating base or delta.

        Args:
            root: Root directory to snapshot.
            experiment: Optional experiment identifier.
            compute_checksum: Whether to compute SHA-256 checksums.
            workers: Number of parallel workers for processing.
            batch_size: Number of files to process per batch.

        Returns:
            Tuple of (added_count, modified_count, removed_count).
        """
        timestamp = datetime.now().strftime("%Y-%m-%dT%H%M%S.%f")
        root_path = Path(root).resolve()
        root_str = str(root_path)
        exp_dir = self._get_exp_dir(root_str, experiment)

        # Walk directory to get current files
        current_files = {}
        file_args = []

        if workers > 1:
            # Use parallel directory walking for better performance
            for fpath in _parallel_walk(root_str, workers):
                file_args.append((fpath, compute_checksum, experiment, timestamp))
        else:
            # Fall back to sequential walk for single worker
            for dirpath, _, filenames in os.walk(root_path):
                for fname in filenames:
                    fpath = str(Path(dirpath) / fname)
                    file_args.append((fpath, compute_checksum, experiment, timestamp))

        # Process files in batches
        for i in range(0, len(file_args), batch_size):
            batch = file_args[i:i + batch_size]
            records = self._process_batch(batch, workers, compute_checksum)
            for rec in records:
                current_files[rec["path"]] = rec

        # Load previous state
        previous_state = self.load_current_state(exp_dir)

        if not previous_state:
            # First run: create base snapshot
            result = self._write_base(exp_dir, timestamp, current_files)
        else:
            # Compute delta
            result = self._write_delta(exp_dir, timestamp, current_files, previous_state)

        # Runs even when nothing changed, so a missing current file heals itself.
        try:
            self.refresh_current(exp_dir)
        except Exception as e:  # queries fall back to live dedup for this experiment
            print(f"lcls-catalog: warning: could not refresh current state for "
                  f"{exp_dir.name}: {e}", file=sys.stderr)
        return result

    def _write_base(self, exp_dir: Path, timestamp: str, files: dict[str, dict]) -> tuple[int, int, int]:
        """Write a base snapshot file."""
        records = []
        for rec in files.values():
            rec["on_disk"] = True
            rec["status"] = None  # Base files don't have status
            records.append(rec)

        if not records:
            return (0, 0, 0)

        output_path = exp_dir / f"base_{timestamp}.parquet"
        temp_path = output_path.with_suffix('.parquet.tmp')
        table = pa.Table.from_pylist(records, schema=self.SCHEMA)
        pq.write_table(table, temp_path)
        temp_path.rename(output_path)  # Atomic rename

        return (len(records), 0, 0)

    def _write_delta(
        self,
        exp_dir: Path,
        timestamp: str,
        current_files: dict[str, dict],
        previous_state: dict[str, dict],
    ) -> tuple[int, int, int]:
        """Compute and write a delta file."""
        delta_records = []
        added = 0
        modified = 0
        removed = 0

        # Find added/modified files
        for path, meta in current_files.items():
            if path not in previous_state:
                delta_records.append({**meta, "status": "added", "on_disk": None})
                added += 1
            elif not previous_state[path].get("on_disk", True):
                # File was removed but now exists again (restored)
                delta_records.append({**meta, "status": "added", "on_disk": None})
                added += 1
            elif self._file_changed(meta, previous_state[path]):
                delta_records.append({**meta, "status": "modified", "on_disk": None})
                modified += 1

        # Find removed files (only those that were on_disk=True)
        for path, prev_rec in previous_state.items():
            if path not in current_files and prev_rec.get("on_disk", True):
                delta_records.append({
                    "path": path,
                    "parent_path": prev_rec.get("parent_path", ""),
                    "filename": prev_rec.get("filename", ""),
                    "size": prev_rec.get("size"),
                    "mtime": prev_rec.get("mtime"),
                    "owner": prev_rec.get("owner"),
                    "group_name": prev_rec.get("group_name"),
                    "permissions": prev_rec.get("permissions"),
                    "checksum": prev_rec.get("checksum"),
                    "experiment": prev_rec.get("experiment"),
                    "run": prev_rec.get("run"),
                    "indexed_at": timestamp,
                    "status": "removed",
                    "on_disk": None,
                })
                removed += 1

        if not delta_records:
            return (0, 0, 0)

        output_path = exp_dir / f"delta_{timestamp}.parquet"
        temp_path = output_path.with_suffix('.parquet.tmp')
        table = pa.Table.from_pylist(delta_records, schema=self.SCHEMA)
        pq.write_table(table, temp_path)
        temp_path.rename(output_path)  # Atomic rename

        return (added, modified, removed)

    def _file_changed(self, current: dict, previous: dict) -> bool:
        """Check if a file has changed based on size or mtime."""
        return (
            current.get("size") != previous.get("size") or
            current.get("mtime") != previous.get("mtime")
        )

    def _process_batch(
        self, batch: list[tuple], workers: int, compute_checksum: bool
    ) -> list[dict]:
        """Process a batch of files with optional parallelism."""
        if workers > 1:
            executor_class = ProcessPoolExecutor if compute_checksum else ThreadPoolExecutor
            chunksize = max(1, len(batch) // (workers * 4))
            with executor_class(max_workers=workers) as executor:
                results = list(executor.map(_process_file, batch, chunksize=chunksize))
        else:
            results = [_process_file(arg) for arg in batch]

        return [r for r in results if r is not None]

    def _exp_dirs(self) -> list[Path]:
        """Experiment directories (skips _state and hidden dirs)."""
        return sorted(p for p in self.catalog_dir.iterdir()
                      if p.is_dir() and not p.name.startswith(("_", ".")))

    def _get_all_current_state(self) -> dict[str, dict]:
        """Load current state from all experiments."""
        all_state = {}
        for exp_dir in self._exp_dirs():
            state = self.load_current_state(exp_dir)
            all_state.update(state)
        return all_state

    # --- Materialized current state -------------------------------------
    #
    # Deduplicating base + deltas at query time means sorting every row of
    # every experiment that has a delta (195M rows / 83 GB peak RSS for one
    # query in Oct 2026). Instead each experiment keeps one deduplicated file,
    #   _state/current/<exp>__<stem of newest base/delta>.parquet
    # whose name says which snapshot it is current as of. Queries read it
    # directly and only dedup live for experiments whose file is missing or
    # stale.

    @property
    def _current_dir(self) -> Path:
        return self.catalog_dir / STATE_DIR / "current"

    def _current_versions(self, exp: Optional[str] = None) -> dict[str, list[tuple[str, Path]]]:
        """Materialized files per experiment as (source stem, path), oldest first."""
        pattern = f"{glob.escape(exp)}__*.parquet" if exp else "*.parquet"
        versions: dict[str, list[tuple[str, Path]]] = {}
        if self._current_dir.is_dir():
            for p in self._current_dir.glob(pattern):
                name, _, source = p.stem.rpartition("__")
                if name and source.startswith(("base_", "delta_")):
                    versions.setdefault(name, []).append((source, p))
        for v in versions.values():
            v.sort(key=lambda sp: _stem_ts(sp[0]))
        return versions

    def refresh_current(
        self, exp_dir: Path, memory_limit: str = "4GB", threads: int = 4
    ) -> str:
        """Bring one experiment's materialized current state up to date.

        Applies a single new delta incrementally (memory ~ that delta);
        otherwise rebuilds from all base/delta files.

        Returns:
            "fresh", "incremental", "full", or "empty".
        """
        exp = exp_dir.name
        inputs = _snapshot_inputs(exp_dir)
        versions = self._current_versions(exp).get(exp, [])

        if not inputs:
            for _, p in versions:
                p.unlink(missing_ok=True)
            return "empty"

        newest = inputs[-1].stem
        if versions and versions[-1][0] == newest:
            return "fresh"

        prev = versions[-1] if versions else None
        newer = [p for p in inputs if prev and _stem_ts(p.stem) > _stem_ts(prev[0])]
        if prev and len(newer) == 1 and newer[0].name.startswith("delta_"):
            mode = "incremental"
            delta = newer[0]
            select = f"""
                SELECT o.* FROM read_parquet('{prev[1]}') o
                    ANTI JOIN read_parquet('{delta}') d ON o.path = d.path
                UNION ALL
                SELECT {COLUMNS}, {ON_DISK_SQL} AS on_disk FROM read_parquet('{delta}')"""
        else:
            mode = "full"
            dedup = LATEST_ROW_SQL if len(inputs) > 1 else ""
            select = f"""
                SELECT {COLUMNS}, {ON_DISK_SQL} AS on_disk
                FROM read_parquet({_sql_list(inputs)}, union_by_name=true)
                {dedup}"""

        self._current_dir.mkdir(parents=True, exist_ok=True)
        dst = self._current_dir / f"{exp}__{newest}.parquet"
        tmp = dst.with_name(dst.name + ".tmp")
        self._copy_to_parquet(select, tmp, memory_limit, threads)
        tmp.rename(dst)  # Atomic rename

        # Keep the previous version: a query may have listed it a moment ago.
        for _, p in versions[:-1]:
            p.unlink(missing_ok=True)
        return mode

    def refresh_all(
        self,
        experiments: Optional[list[str]] = None,
        memory_limit: str = "4GB",
        threads: int = 4,
    ) -> dict[str, list[str]]:
        """Refresh materialized current state for all (or the named) experiments.

        Returns:
            Experiment names grouped by outcome ("fresh", "incremental",
            "full", "empty", "failed").
        """
        exp_dirs = self._exp_dirs()
        if experiments:
            wanted = set(experiments)
            exp_dirs = [d for d in exp_dirs if d.name in wanted]
        outcome: dict[str, list[str]] = {}
        for exp_dir in exp_dirs:
            try:
                mode = self.refresh_current(exp_dir, memory_limit, threads)
            except Exception as e:
                print(f"lcls-catalog: refresh failed for {exp_dir.name}: {e}", file=sys.stderr)
                mode = "failed"
            outcome.setdefault(mode, []).append(exp_dir.name)

        # Drop materialized files whose experiment directory is gone.
        live = {d.name for d in self._exp_dirs()}
        if not experiments:
            for exp, versions in self._current_versions().items():
                if exp not in live:
                    for _, p in versions:
                        p.unlink(missing_ok=True)
        return outcome

    @staticmethod
    def _copy_to_parquet(select: str, dst: Path, memory_limit: str, threads: int):
        """COPY a SELECT to a Parquet file under a memory cap.

        DuckDB 1.4 sometimes runs out of memory just under a cap instead of
        spilling, yet spills fine under a much smaller one (measured
        2026-10-08: a dedup that failed at 4GB passed at 2GB, an anti-join
        that failed at 2GB passed at 1GB). So on OOM, retry smaller.
        """
        for limit in dict.fromkeys([memory_limit, "2GB", "1GB"]):
            spill = tempfile.mkdtemp(prefix="lcat-duckdb-")
            con = duckdb.connect(config={
                "memory_limit": limit,
                "threads": threads,
                "temp_directory": spill,
                "preserve_insertion_order": False,
            })
            try:
                con.execute("SET enable_progress_bar = false")  # keeps batch logs clean
                con.execute(f"COPY ({select}) TO '{dst}' (FORMAT parquet)")
                return
            except duckdb.OutOfMemoryException:
                if limit == "1GB":
                    raise
            finally:
                con.close()
                shutil.rmtree(spill, ignore_errors=True)

    def _query_with_dedup(self, sql: str) -> list[tuple]:
        """Execute SQL against `files`: the current state of every experiment."""
        return list(self._iter_query_with_dedup(sql))

    def _iter_query_with_dedup(self, sql: str, batch_size: int = 10000) -> Iterator[tuple]:
        """Run SQL against `files` under self.limits, yielding rows as they arrive.

        The timeout covers execution until the first rows are ready; streaming
        the rest (a long find listing, say) is not timed.

        Raises:
            QueryLimitError: the query hit the memory or time limit.
        """
        lim = self.limits
        spill = tempfile.mkdtemp(prefix="lcat-duckdb-", dir=lim.spill_dir)
        con = duckdb.connect(config={
            "memory_limit": lim.memory_limit,
            "threads": lim.threads,
            "temp_directory": spill,
            "max_temp_directory_size": lim.max_spill,
            "preserve_insertion_order": False,
        })
        con.execute("SET enable_progress_bar = false")
        timed_out = threading.Event()

        def interrupt():
            timed_out.set()
            con.interrupt()

        timer = threading.Timer(lim.timeout, interrupt) if lim.timeout > 0 else None
        try:
            try:
                if timer:
                    timer.start()
                cursor = con.execute(f"WITH files AS ({self._files_sql()})\n" + sql)
                batch = cursor.fetchmany(batch_size)
            finally:
                if timer:
                    timer.cancel()
            while batch:
                yield from batch
                batch = cursor.fetchmany(batch_size)
        except duckdb.OutOfMemoryException as e:
            raise QueryLimitError(
                f"query stopped: it needed more than {lim.memory_limit} of memory "
                f"(LCAT_MEMORY_LIMIT; spilling to disk is capped at {lim.max_spill}, "
                f"LCAT_MAX_SPILL).\n{LIMIT_ADVICE}") from e
        except duckdb.InterruptException as e:
            if not timed_out.is_set():
                raise
            raise QueryLimitError(
                f"query stopped after {lim.timeout:g}s (LCAT_TIMEOUT).\n{LIMIT_ADVICE}") from e
        finally:
            con.close()
            shutil.rmtree(spill, ignore_errors=True)

    def _files_sql(self) -> str:
        """SELECT producing the current state of every experiment.

        Reads each experiment's materialized current file when it is fresh.
        Otherwise falls back to the base files as-is (no deltas: nothing to
        dedup) or to a live ROW_NUMBER() dedup over that experiment's files.
        """
        current = self._current_versions()
        fresh, base_only, stale = [], [], []
        for exp_dir in self._exp_dirs():
            inputs = _snapshot_inputs(exp_dir)
            if not inputs:
                continue
            versions = current.get(exp_dir.name)
            if versions and versions[-1][0] == inputs[-1].stem:
                fresh.append(versions[-1][1])
            elif all(p.name.startswith("base_") for p in inputs):
                base_only.extend(inputs)
            else:
                stale.extend(inputs)

        parts = []
        if fresh:
            parts.append(f"""
                SELECT {COLUMNS}, on_disk
                FROM read_parquet({_sql_list(fresh)}, union_by_name=true)""")
        if base_only:
            parts.append(f"""
                SELECT {COLUMNS}, COALESCE(on_disk, true) AS on_disk
                FROM read_parquet({_sql_list(base_only)}, union_by_name=true)""")
        if stale:
            parts.append(f"""
                SELECT {COLUMNS}, {ON_DISK_SQL} AS on_disk
                FROM read_parquet({_sql_list(stale)}, union_by_name=true)
                {LATEST_ROW_SQL}""")
        return " UNION ALL ".join(parts) if parts else EMPTY_FILES_SQL

    def ls(self, path: str, on_disk_only: bool = False) -> list[FileEntry]:
        """
        List files in a directory.

        Args:
            path: Directory path to list.
            on_disk_only: If True, only return files currently on disk.

        Returns:
            List of FileEntry objects for files in that directory.
        """
        on_disk_filter = "AND on_disk = true" if on_disk_only else ""
        sql = f"""
            SELECT path, parent_path, filename, size, mtime, owner, group_name,
                   permissions, checksum, NULL as archive_uri, experiment, run, indexed_at
            FROM files
            WHERE parent_path = '{path}' {on_disk_filter}
            ORDER BY filename
        """
        rows = self._query_with_dedup(sql)
        return [FileEntry(*row) for row in rows]

    def ls_dirs(self, path: str, on_disk_only: bool = False) -> list[DirSummary]:
        """
        List immediate subdirectories under a path with aggregated stats.

        Args:
            path: Parent directory path.
            on_disk_only: If True, only count files currently on disk.

        Returns:
            List of DirSummary objects for each subdirectory.
        """
        path = path.rstrip("/")
        prefix = path + "/"
        prefix_len = len(prefix)
        on_disk_filter = "AND on_disk = true" if on_disk_only else ""

        sql = f"""
            SELECT
                CASE
                    WHEN POSITION('/' IN SUBSTR(parent_path, {prefix_len + 1})) > 0
                    THEN SUBSTR(SUBSTR(parent_path, {prefix_len + 1}), 1,
                                POSITION('/' IN SUBSTR(parent_path, {prefix_len + 1})) - 1)
                    ELSE SUBSTR(parent_path, {prefix_len + 1})
                END as dirname,
                SUM(size) as total_size,
                COUNT(*) as file_count
            FROM files
            WHERE parent_path LIKE '{prefix}%'
              AND parent_path != '{path}'
              {on_disk_filter}
            GROUP BY dirname
            HAVING dirname != ''
            ORDER BY dirname
        """
        rows = self._query_with_dedup(sql)
        return [DirSummary(*row) for row in rows]

    def find(self, pattern: str, **filters) -> list[FileEntry]:
        """Search for files matching a pattern; see iter_find for arguments."""
        return list(self.iter_find(pattern, **filters))

    def iter_find(
        self,
        pattern: str,
        size_gt: Optional[int] = None,
        size_lt: Optional[int] = None,
        experiment: Optional[str] = None,
        exclude: Optional[list[str]] = None,
        on_disk_only: bool = False,
        removed_only: bool = False,
        skip_symlinks: bool = False,
    ) -> Iterator[FileEntry]:
        """
        Search for files matching a pattern, yielding entries as they arrive.

        Args:
            pattern: SQL LIKE pattern for path (e.g., "%.h5", "%mfx%").
            size_gt: Minimum file size in bytes.
            size_lt: Maximum file size in bytes.
            experiment: Filter by experiment ID.
            exclude: List of patterns to exclude (NOT LIKE).
            on_disk_only: If True, only return files currently on disk.
            removed_only: If True, only return files that have been removed.
            skip_symlinks: If True, exclude symbolic links from results.

        Yields:
            Matching FileEntry objects, ordered by path.
        """
        conditions = [f"path LIKE '{pattern}'"]

        if exclude:
            for exc in exclude:
                conditions.append(f"path NOT LIKE '{exc}'")
        if size_gt is not None:
            conditions.append(f"size > {size_gt}")
        if size_lt is not None:
            conditions.append(f"size < {size_lt}")
        if experiment is not None:
            conditions.append(f"experiment = '{experiment}'")
        if on_disk_only:
            conditions.append("on_disk = true")
        if removed_only:
            conditions.append("on_disk = false")
        if skip_symlinks:
            # S_IFMT=0o170000=61440, S_IFLNK=0o120000=40960
            conditions.append("(permissions & 61440) != 40960")

        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT path, parent_path, filename, size, mtime, owner, group_name,
                   permissions, checksum, NULL as archive_uri, experiment, run, indexed_at
            FROM files
            WHERE {where_clause}
            ORDER BY path
        """
        for row in self._iter_query_with_dedup(sql):
            yield FileEntry(*row)

    def tree(self, path: str, depth: int = 2) -> str:
        """
        Generate an ASCII tree representation of the directory structure.

        Args:
            path: Root path for the tree.
            depth: Maximum depth to display.

        Returns:
            ASCII tree string.
        """
        lines = [path]
        self._build_tree(path, "", depth, lines)
        return "\n".join(lines)

    def _build_tree(self, path: str, prefix: str, depth: int, lines: list[str]):
        """Recursively build tree representation."""
        if depth <= 0:
            return

        dirs = self.ls_dirs(path, on_disk_only=True)
        files = self.ls(path, on_disk_only=True)

        items = [(d.dirname, True, d) for d in dirs] + [(f.filename, False, f) for f in files]
        items.sort(key=lambda x: x[0])

        for i, (name, is_dir, item) in enumerate(items):
            is_last = i == len(items) - 1
            connector = "└── " if is_last else "├── "
            next_prefix = prefix + ("    " if is_last else "│   ")

            if is_dir:
                lines.append(f"{prefix}{connector}{name}/ ({item.file_count} files, {item.size_human})")
                self._build_tree(f"{path}/{name}", next_prefix, depth - 1, lines)
            else:
                lines.append(f"{prefix}{connector}{name} ({item.size_human})")

    def count(self, on_disk_only: bool = False) -> int:
        """Return total number of files in the catalog."""
        on_disk_filter = "WHERE on_disk = true" if on_disk_only else ""
        sql = f"SELECT COUNT(*) FROM files {on_disk_filter}"
        result = self._query_with_dedup(sql)
        return result[0][0] if result else 0

    def total_size(self, on_disk_only: bool = False) -> int:
        """Return total size of all files in the catalog."""
        on_disk_filter = "WHERE on_disk = true" if on_disk_only else ""
        sql = f"SELECT COALESCE(SUM(size), 0) FROM files {on_disk_filter}"
        result = self._query_with_dedup(sql)
        return result[0][0] if result else 0

    def get_stats(self) -> dict:
        """Return catalog statistics in a single query.

        Returns:
            Dictionary with keys: total_count, on_disk_count, total_size, on_disk_size
        """
        sql = """
            SELECT
                COUNT(*) as total_count,
                SUM(CASE WHEN on_disk THEN 1 ELSE 0 END) as on_disk_count,
                COALESCE(SUM(size), 0) as total_size,
                COALESCE(SUM(CASE WHEN on_disk THEN size ELSE 0 END), 0) as on_disk_size
            FROM files
        """
        result = self._query_with_dedup(sql)
        row = result[0] if result else (0, 0, 0, 0)
        return {
            "total_count": row[0],
            "on_disk_count": row[1],
            "total_size": row[2],
            "on_disk_size": row[3],
        }

    def query(self, sql: str) -> list[tuple]:
        """
        Execute a raw SQL query on the catalog.

        Args:
            sql: SQL query string. Use 'files' as the table name.

        Returns:
            List of result tuples.
        """
        return self._query_with_dedup(sql)

    def iter_query(self, sql: str) -> Iterator[tuple]:
        """Like query(), but yields rows as they arrive."""
        return self._iter_query_with_dedup(sql)

    def consolidate(self, archive_dir: Optional[str] = None) -> dict[str, int]:
        """
        Merge base + deltas into new base files for each experiment.

        Args:
            archive_dir: Optional directory to move old files to (instead of deleting).

        Returns:
            Dictionary with consolidation stats.
        """
        stats = {"experiments": 0, "files_removed": 0, "files_archived": 0}
        timestamp = datetime.now().strftime("%Y-%m-%dT%H%M%S.%f")

        for exp_dir in self._exp_dirs():
            # Get all parquet files
            all_files = list(exp_dir.glob("*.parquet"))
            if len(all_files) <= 1:
                continue  # Nothing to consolidate

            # The materialized current state is exactly the new base; copying
            # it keeps memory flat instead of one Python dict per row.
            self.refresh_current(exp_dir)
            current = self._current_versions(exp_dir.name).get(exp_dir.name)
            if not current:
                continue

            new_base = exp_dir / f"base_{timestamp}.parquet"
            temp_path = new_base.with_suffix('.parquet.tmp')
            self._copy_to_parquet(
                f"SELECT {COLUMNS}, on_disk, NULL::VARCHAR AS status "
                f"FROM read_parquet('{current[-1][1]}')",
                temp_path, memory_limit="4GB", threads=4)
            temp_path.rename(new_base)  # Atomic rename

            # Handle old files
            old_files = [f for f in all_files if f != new_base]

            if archive_dir:
                archive_exp_dir = Path(archive_dir) / exp_dir.name
                archive_exp_dir.mkdir(parents=True, exist_ok=True)
                for f in old_files:
                    shutil.move(str(f), str(archive_exp_dir / f.name))
                    stats["files_archived"] += 1
            else:
                for f in old_files:
                    f.unlink()
                    stats["files_removed"] += 1

            self.refresh_current(exp_dir)  # now a single base: a plain copy
            stats["experiments"] += 1

        return stats

    def list_snapshots(self, exp_hash: Optional[str] = None) -> list[dict]:
        """
        List all snapshot files in the catalog.

        Args:
            exp_hash: Optional experiment hash to filter by.

        Returns:
            List of snapshot info dictionaries.
        """
        snapshots = []

        dirs_to_scan = [self.catalog_dir / exp_hash] if exp_hash else self._exp_dirs()

        for exp_dir in dirs_to_scan:
            if not exp_dir.is_dir():
                continue

            for pq_file in sorted(exp_dir.glob("*.parquet")):
                file_type = "base" if pq_file.name.startswith("base_") else "delta"
                timestamp = pq_file.stem.split("_", 1)[1] if "_" in pq_file.stem else ""

                # Get file stats (row count from the footer, not the whole table)
                stat = pq_file.stat()
                num_rows = pq.ParquetFile(pq_file).metadata.num_rows

                snapshots.append({
                    "experiment": exp_dir.name,
                    "type": file_type,
                    "timestamp": timestamp,
                    "file": str(pq_file),
                    "size_bytes": stat.st_size,
                    "record_count": num_rows,
                })

        return snapshots

    def close(self):
        """No-op for API compatibility."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
