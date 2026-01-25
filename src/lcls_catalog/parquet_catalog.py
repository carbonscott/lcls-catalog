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

import hashlib
import os
import re
import shutil
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import DirSummary, FileEntry


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

    def __init__(self, catalog_dir: str):
        """
        Initialize a Parquet catalog.

        Args:
            catalog_dir: Directory to store Parquet files.
        """
        self.catalog_dir = Path(catalog_dir)
        self.catalog_dir.mkdir(parents=True, exist_ok=True)

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
            return self._write_base(exp_dir, timestamp, current_files)

        # Compute delta
        return self._write_delta(exp_dir, timestamp, current_files, previous_state)

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

    def _get_all_current_state(self) -> dict[str, dict]:
        """Load current state from all experiments."""
        all_state = {}
        for exp_dir in self.catalog_dir.iterdir():
            if exp_dir.is_dir():
                state = self.load_current_state(exp_dir)
                all_state.update(state)
        return all_state

    def _query_with_dedup(self, sql: str) -> list[tuple]:
        """Execute SQL query with deduplication across base + deltas.

        Optimization: Only run ROW_NUMBER() dedup on experiments that have
        delta files. Experiments with only base files are read directly.
        This reduces query time from ~37s to ~5s for typical workloads.
        """
        # Find which experiments have delta files (need dedup)
        all_exps = set(p.parent.name for p in self.catalog_dir.glob("*/*.parquet"))
        exps_with_deltas = set(
            p.parent.name for p in self.catalog_dir.glob("*/delta_*.parquet")
        )
        exps_base_only = all_exps - exps_with_deltas

        # Column list for consistent SELECT
        columns = """path, parent_path, filename, size, mtime, owner, group_name,
                     permissions, checksum, experiment, run, indexed_at"""

        if not exps_with_deltas:
            # Fast path: no deltas anywhere, skip dedup entirely
            pattern = str(self.catalog_dir / "*" / "*.parquet")
            simple_cte = f"""
                WITH files AS (
                    SELECT {columns}, COALESCE(on_disk, true) as on_disk
                    FROM read_parquet('{pattern}', union_by_name=true)
                )
            """
            return duckdb.execute(simple_cte + sql).fetchall()

        if not exps_base_only:
            # All experiments have deltas, use original global dedup
            pattern = str(self.catalog_dir / "*" / "*.parquet")
            dedup_cte = f"""
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY path ORDER BY indexed_at DESC) as _rn
                    FROM read_parquet('{pattern}', union_by_name=true)
                ),
                files AS (
                    SELECT {columns},
                           CASE
                               WHEN on_disk IS NOT NULL THEN on_disk
                               WHEN status IS NOT NULL THEN status != 'removed'
                               ELSE true
                           END as on_disk
                    FROM ranked
                    WHERE _rn = 1
                )
            """
            return duckdb.execute(dedup_cte + sql).fetchall()

        # Selective dedup: combine base-only (no dedup) + experiments with deltas (dedup)
        base_only_patterns = [
            str(self.catalog_dir / exp / "*.parquet") for exp in exps_base_only
        ]
        delta_exp_patterns = [
            str(self.catalog_dir / exp / "*.parquet") for exp in exps_with_deltas
        ]

        selective_cte = f"""
            WITH
            base_only AS (
                SELECT {columns}, COALESCE(on_disk, true) as on_disk
                FROM read_parquet({base_only_patterns}, union_by_name=true)
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY path ORDER BY indexed_at DESC) as _rn
                FROM read_parquet({delta_exp_patterns}, union_by_name=true)
            ),
            deduped AS (
                SELECT {columns},
                       CASE
                           WHEN on_disk IS NOT NULL THEN on_disk
                           WHEN status IS NOT NULL THEN status != 'removed'
                           ELSE true
                       END as on_disk
                FROM ranked
                WHERE _rn = 1
            ),
            files AS (
                SELECT * FROM base_only
                UNION ALL
                SELECT * FROM deduped
            )
        """
        return duckdb.execute(selective_cte + sql).fetchall()

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

    def find(
        self,
        pattern: str,
        size_gt: Optional[int] = None,
        size_lt: Optional[int] = None,
        experiment: Optional[str] = None,
        exclude: Optional[list[str]] = None,
        on_disk_only: bool = False,
        removed_only: bool = False,
        skip_symlinks: bool = False,
    ) -> list[FileEntry]:
        """
        Search for files matching a pattern.

        Args:
            pattern: SQL LIKE pattern for path (e.g., "%.h5", "%mfx%").
            size_gt: Minimum file size in bytes.
            size_lt: Maximum file size in bytes.
            experiment: Filter by experiment ID.
            exclude: List of patterns to exclude (NOT LIKE).
            on_disk_only: If True, only return files currently on disk.
            removed_only: If True, only return files that have been removed.
            skip_symlinks: If True, exclude symbolic links from results.

        Returns:
            List of matching FileEntry objects.
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
        rows = self._query_with_dedup(sql)
        return [FileEntry(*row) for row in rows]

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

        for exp_dir in self.catalog_dir.iterdir():
            if not exp_dir.is_dir():
                continue

            # Get all parquet files
            all_files = list(exp_dir.glob("*.parquet"))
            if len(all_files) <= 1:
                continue  # Nothing to consolidate

            # Reconstruct current state
            state = self.load_current_state(exp_dir)
            if not state:
                continue

            # Write new base with all records
            records = []
            for rec in state.values():
                rec["status"] = None  # Base files don't have status
                records.append(rec)
            new_base = exp_dir / f"base_{timestamp}.parquet"
            temp_path = new_base.with_suffix('.parquet.tmp')
            table = pa.Table.from_pylist(records, schema=self.SCHEMA)
            pq.write_table(table, temp_path)
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

        dirs_to_scan = [self.catalog_dir / exp_hash] if exp_hash else self.catalog_dir.iterdir()

        for exp_dir in dirs_to_scan:
            if not exp_dir.is_dir():
                continue

            for pq_file in sorted(exp_dir.glob("*.parquet")):
                file_type = "base" if pq_file.name.startswith("base_") else "delta"
                timestamp = pq_file.stem.split("_", 1)[1] if "_" in pq_file.stem else ""

                # Get file stats
                stat = pq_file.stat()
                table = pq.read_table(pq_file)

                snapshots.append({
                    "experiment": exp_dir.name,
                    "type": file_type,
                    "timestamp": timestamp,
                    "file": str(pq_file),
                    "size_bytes": stat.st_size,
                    "record_count": table.num_rows,
                })

        return snapshots

    def close(self):
        """No-op for API compatibility."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
