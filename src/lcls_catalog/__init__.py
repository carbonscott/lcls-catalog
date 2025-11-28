"""LCLS Data Catalog - A lightweight catalog for managing purged data metadata."""

from .catalog import Catalog

__version__ = "0.1.0"
__all__ = ["Catalog"]

# Optional Parquet support
try:
    from .parquet_catalog import ParquetCatalog
    __all__.append("ParquetCatalog")
except ImportError:
    pass
