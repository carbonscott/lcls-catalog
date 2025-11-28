"""Parquet-based catalog with parallel write support."""

import hashlib
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import DirSummary, FileEntry


def _process_file(args: tuple) -> Optional[dict]:
    """Process a single file and return its metadata. Runs in worker process."""
    fpath_str, compute_checksum, experiment, purge_date = args
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
        import re
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
            "archive_uri": None,
            "experiment": experiment,
            "run": run,
            "purge_date": purge_date,
        }
    except (OSError, PermissionError):
        return None


class ParquetCatalog:
    """A catalog using Parquet files with DuckDB for queries."""

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
        ("archive_uri", pa.string()),
        ("experiment", pa.string()),
        ("run", pa.int32()),
        ("purge_date", pa.string()),
    ])

    def __init__(self, catalog_dir: str):
        """
        Initialize a Parquet catalog.

        Args:
            catalog_dir: Directory to store Parquet files.
        """
        self.catalog_dir = Path(catalog_dir)
        self.catalog_dir.mkdir(parents=True, exist_ok=True)

    def _get_parquet_pattern(self) -> str:
        """Get glob pattern for all Parquet files."""
        return str(self.catalog_dir / "*.parquet")

    def _get_parquet_path(self, purge_date: str, root: str) -> Path:
        """Get path for a specific snapshot's Parquet file."""
        # Use hash of root path for unique filenames
        path_hash = hashlib.md5(root.encode()).hexdigest()[:8]
        return self.catalog_dir / f"{path_hash}_{purge_date}.parquet"

    def snapshot(
        self,
        root: str,
        experiment: Optional[str] = None,
        compute_checksum: bool = False,
        purge_date: Optional[str] = None,
        workers: int = 1,
        batch_size: int = 10000,
    ) -> int:
        """
        Walk a directory tree and capture metadata for all files.

        Uses streaming writes to handle large directories without memory issues.

        Args:
            root: Root directory to snapshot.
            experiment: Optional experiment identifier.
            compute_checksum: Whether to compute SHA-256 checksums.
            purge_date: Date string for this snapshot (defaults to today).
            workers: Number of parallel workers for processing.
            batch_size: Number of files to process per batch (default: 10000).

        Returns:
            Number of files cataloged.
        """
        if purge_date is None:
            purge_date = datetime.now().strftime("%Y-%m-%d")

        root_path = Path(root).resolve()
        output_path = self._get_parquet_path(purge_date, str(root_path))
        temp_path = output_path.with_suffix('.parquet.tmp')
        total_count = 0

        try:
            # Stream batches to temp file
            with pq.ParquetWriter(temp_path, self.SCHEMA) as writer:
                batch = []

                for dirpath, _, filenames in os.walk(root_path):
                    for fname in filenames:
                        fpath = str(Path(dirpath) / fname)
                        batch.append((fpath, compute_checksum, experiment, purge_date))

                        if len(batch) >= batch_size:
                            records = self._process_batch(batch, workers, compute_checksum)
                            if records:
                                table = pa.Table.from_pylist(records, schema=self.SCHEMA)
                                writer.write_table(table)
                                total_count += len(records)
                            batch = []

                # Process final batch
                if batch:
                    records = self._process_batch(batch, workers, compute_checksum)
                    if records:
                        table = pa.Table.from_pylist(records, schema=self.SCHEMA)
                        writer.write_table(table)
                        total_count += len(records)

            # Atomic rename only on success
            if total_count > 0:
                temp_path.rename(output_path)
            else:
                temp_path.unlink()

        except Exception:
            # Clean up temp file on failure
            if temp_path.exists():
                temp_path.unlink()
            raise

        return total_count

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

    def _query(self, sql: str) -> list[tuple]:
        """Execute a SQL query on the Parquet files."""
        pattern = self._get_parquet_pattern()
        # Replace placeholder with actual pattern
        sql = sql.replace("FILES", f"'{pattern}'")
        return duckdb.execute(sql).fetchall()

    def ls(self, path: str) -> list[FileEntry]:
        """
        List files in a directory.

        Args:
            path: Directory path to list.

        Returns:
            List of FileEntry objects for files in that directory.
        """
        sql = f"""
            SELECT path, parent_path, filename, size, mtime, owner, group_name,
                   permissions, checksum, archive_uri, experiment, run, purge_date
            FROM FILES
            WHERE parent_path = '{path}'
            ORDER BY filename
        """
        rows = self._query(sql)
        return [FileEntry(*row) for row in rows]

    def ls_dirs(self, path: str) -> list[DirSummary]:
        """
        List immediate subdirectories under a path with aggregated stats.

        Args:
            path: Parent directory path.

        Returns:
            List of DirSummary objects for each subdirectory.
        """
        path = path.rstrip("/")
        prefix = path + "/"
        prefix_len = len(prefix)

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
            FROM FILES
            WHERE parent_path LIKE '{prefix}%'
              AND parent_path != '{path}'
            GROUP BY dirname
            HAVING dirname != ''
            ORDER BY dirname
        """
        rows = self._query(sql)
        return [DirSummary(*row) for row in rows]

    def find(
        self,
        pattern: str,
        size_gt: Optional[int] = None,
        size_lt: Optional[int] = None,
        experiment: Optional[str] = None,
        exclude: Optional[list[str]] = None,
    ) -> list[FileEntry]:
        """
        Search for files matching a pattern.

        Args:
            pattern: SQL LIKE pattern for path (e.g., "%.h5", "%mfx%").
            size_gt: Minimum file size in bytes.
            size_lt: Maximum file size in bytes.
            experiment: Filter by experiment ID.
            exclude: List of patterns to exclude (NOT LIKE).

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

        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT path, parent_path, filename, size, mtime, owner, group_name,
                   permissions, checksum, archive_uri, experiment, run, purge_date
            FROM FILES
            WHERE {where_clause}
            ORDER BY path
        """
        rows = self._query(sql)
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

        dirs = self.ls_dirs(path)
        files = self.ls(path)

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

    def count(self) -> int:
        """Return total number of files in the catalog."""
        sql = "SELECT COUNT(*) FROM FILES"
        result = self._query(sql)
        return result[0][0] if result else 0

    def total_size(self) -> int:
        """Return total size of all files in the catalog."""
        sql = "SELECT COALESCE(SUM(size), 0) FROM FILES"
        result = self._query(sql)
        return result[0][0] if result else 0

    def query(self, sql: str) -> list[tuple]:
        """
        Execute a raw SQL query on the catalog.

        Args:
            sql: SQL query string. Use 'files' as the table name.

        Returns:
            List of result tuples.
        """
        pattern = self._get_parquet_pattern()
        # Replace 'files' table reference with parquet glob pattern
        sql = sql.replace("files", f"'{pattern}'")
        sql = sql.replace("FILES", f"'{pattern}'")
        return duckdb.execute(sql).fetchall()

    def close(self):
        """No-op for API compatibility with Catalog."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
