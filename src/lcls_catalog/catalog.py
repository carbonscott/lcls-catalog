"""Core Catalog class for managing filesystem metadata."""

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

from .schema import init_schema


@dataclass
class FileEntry:
    """Represents a file entry in the catalog."""

    path: str
    parent_path: str
    filename: str
    size: Optional[int]
    mtime: Optional[int]
    owner: Optional[str]
    group_name: Optional[str]
    permissions: Optional[int]
    checksum: Optional[str]
    archive_uri: Optional[str]
    experiment: Optional[str]
    run: Optional[int]
    purge_date: Optional[str]

    @property
    def size_human(self) -> str:
        """Return human-readable file size."""
        if self.size is None:
            return "N/A"
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(self.size) < 1024:
                return f"{self.size:.1f} {unit}"
            self.size /= 1024
        return f"{self.size:.1f} PB"


@dataclass
class DirSummary:
    """Summary of a directory's contents."""

    dirname: str
    total_size: int
    file_count: int

    @property
    def size_human(self) -> str:
        """Return human-readable total size."""
        size = self.total_size
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size) < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"


class Catalog:
    """A catalog for storing and querying filesystem metadata."""

    def __init__(self, db_path: str):
        """
        Initialize a catalog.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    @property
    def conn(self) -> sqlite3.Connection:
        """Lazily connect to the database."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            init_schema(self._conn)
        return self._conn

    def close(self):
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def snapshot(
        self,
        root: str,
        experiment: Optional[str] = None,
        compute_checksum: bool = False,
        purge_date: Optional[str] = None,
    ) -> int:
        """
        Walk a directory tree and capture metadata for all files.

        Args:
            root: Root directory to snapshot.
            experiment: Optional experiment identifier.
            compute_checksum: Whether to compute SHA-256 checksums.
            purge_date: Date string for this snapshot (defaults to today).

        Returns:
            Number of files cataloged.
        """
        if purge_date is None:
            purge_date = datetime.now().strftime("%Y-%m-%d")

        root_path = Path(root).resolve()
        count = 0
        cursor = self.conn.cursor()

        for dirpath, _, filenames in os.walk(root_path):
            for fname in filenames:
                fpath = Path(dirpath) / fname
                try:
                    stat = fpath.lstat()

                    checksum = None
                    if compute_checksum and fpath.is_file():
                        checksum = self._compute_checksum(fpath)

                    run = self._extract_run_number(str(fpath))

                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO files
                        (path, parent_path, filename, size, mtime, owner, group_name,
                         permissions, checksum, archive_uri, experiment, run, purge_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(fpath),
                            str(fpath.parent),
                            fname,
                            stat.st_size,
                            int(stat.st_mtime),
                            str(stat.st_uid),
                            str(stat.st_gid),
                            stat.st_mode,
                            checksum,
                            None,
                            experiment,
                            run,
                            purge_date,
                        ),
                    )
                    count += 1
                except (OSError, PermissionError):
                    continue

        self.conn.commit()
        return count

    def _compute_checksum(self, filepath: Path) -> str:
        """Compute SHA-256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _extract_run_number(self, path: str) -> Optional[int]:
        """Extract run number from path if present (e.g., run0001 -> 1)."""
        import re

        match = re.search(r"run(\d+)", path)
        if match:
            return int(match.group(1))
        return None

    def ls(self, path: str) -> list[FileEntry]:
        """
        List files in a directory.

        Args:
            path: Directory path to list.

        Returns:
            List of FileEntry objects for files in that directory.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT path, parent_path, filename, size, mtime, owner, group_name,
                   permissions, checksum, archive_uri, experiment, run, purge_date
            FROM files
            WHERE parent_path = ?
            ORDER BY filename
            """,
            (path,),
        )
        return [FileEntry(*row) for row in cursor.fetchall()]

    def ls_dirs(self, path: str) -> list[DirSummary]:
        """
        List immediate subdirectories under a path with aggregated stats.

        Args:
            path: Parent directory path.

        Returns:
            List of DirSummary objects for each subdirectory.
        """
        cursor = self.conn.cursor()
        path = path.rstrip("/")
        prefix = path + "/"
        prefix_len = len(prefix)

        cursor.execute(
            """
            SELECT
                CASE
                    WHEN INSTR(SUBSTR(parent_path, ?), '/') > 0
                    THEN SUBSTR(SUBSTR(parent_path, ?), 1, INSTR(SUBSTR(parent_path, ?), '/') - 1)
                    ELSE SUBSTR(parent_path, ?)
                END as dirname,
                SUM(size) as total_size,
                COUNT(*) as file_count
            FROM files
            WHERE parent_path LIKE ? || '%'
              AND parent_path != ?
            GROUP BY dirname
            HAVING dirname != ''
            ORDER BY dirname
            """,
            (prefix_len + 1, prefix_len + 1, prefix_len + 1, prefix_len + 1, prefix, path),
        )
        return [DirSummary(*row) for row in cursor.fetchall()]

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
        query = """
            SELECT path, parent_path, filename, size, mtime, owner, group_name,
                   permissions, checksum, archive_uri, experiment, run, purge_date
            FROM files
            WHERE path LIKE ?
        """
        params: list = [pattern]

        if exclude:
            for exc in exclude:
                query += " AND path NOT LIKE ?"
                params.append(exc)
        if size_gt is not None:
            query += " AND size > ?"
            params.append(size_gt)
        if size_lt is not None:
            query += " AND size < ?"
            params.append(size_lt)
        if experiment is not None:
            query += " AND experiment = ?"
            params.append(experiment)

        query += " ORDER BY path"

        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return [FileEntry(*row) for row in cursor.fetchall()]

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
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files")
        return cursor.fetchone()[0]

    def total_size(self) -> int:
        """Return total size of all files in the catalog."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COALESCE(SUM(size), 0) FROM files")
        return cursor.fetchone()[0]

    def query(self, sql: str) -> list[tuple]:
        """
        Execute a raw SQL query on the catalog.

        Args:
            sql: SQL query string. Use 'files' as the table name.

        Returns:
            List of result tuples.
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
