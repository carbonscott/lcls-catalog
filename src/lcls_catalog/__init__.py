"""LCLS Data Catalog - Persistent file metadata tracking with incremental updates."""

from .catalog import FileEntry, DirSummary
from .parquet_catalog import ParquetCatalog

__version__ = "0.2.0"
__all__ = ["ParquetCatalog", "FileEntry", "DirSummary"]
