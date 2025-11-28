"""Data classes for LCLS catalog entries."""

from dataclasses import dataclass
from typing import Optional


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
    indexed_at: Optional[str]

    @property
    def size_human(self) -> str:
        """Return human-readable file size."""
        if self.size is None:
            return "N/A"
        size = self.size
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size) < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"


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
