# lcls-catalog

A lightweight catalog for browsing and searching LCLS data metadata after purge.

## Installation

```bash
pip install -e .

# With Parquet support (parallel processing)
pip install -e ".[parquet]"
```

## Quick Start

### Create a snapshot before purge

```bash
# SQLite (default)
lcls-catalog snapshot /path/to/experiment -e xpp12345 -o catalog.db

# Parquet with parallel workers
lcls-catalog snapshot /path/to/experiment -e xpp12345 -o catalog/ --format parquet --workers 4
```

### Browse after purge

```bash
# List directories with stats
lcls-catalog ls catalog.db /path/to/experiment --dirs

# List files in a directory
lcls-catalog ls catalog.db /path/to/experiment/scratch/run0001

# Search for files
lcls-catalog find catalog.db "%.h5"
lcls-catalog find catalog.db "image_%" --size-gt 1GB

# Show tree structure
lcls-catalog tree catalog.db /path/to/experiment --depth 3

# Show catalog stats
lcls-catalog stats catalog.db
```

For Parquet catalogs, use the directory path instead of `.db` file:

```bash
lcls-catalog find catalog/ "%.h5"
lcls-catalog stats catalog/
```

## Python API

```python
from lcls_catalog import Catalog

with Catalog("catalog.db") as cat:
    # Create snapshot
    cat.snapshot("/path/to/experiment", experiment="xpp12345")

    # Browse
    for f in cat.ls("/path/to/experiment/scratch/run0001"):
        print(f"{f.filename}: {f.size} bytes")

    # Search
    results = cat.find("%.h5", size_gt=1_000_000)
```

### Parquet API (parallel processing)

```python
from lcls_catalog import ParquetCatalog

with ParquetCatalog("catalog/") as cat:
    # Parallel snapshot with 4 workers
    cat.snapshot("/path/to/experiment", experiment="xpp12345", workers=4)

    # Same browse/search API
    results = cat.find("%.h5")
```

## How Parquet Backend Works

### Multiple Snapshots, One Catalog

Each snapshot creates a unique parquet file based on the source path:

```
catalog/
├── a3f2b8c1_2024-06-01.parquet   # /path/to/exp1
├── 7d9e4f2a_2024-06-01.parquet   # /path/to/exp2
└── b2c4d6e8_2024-06-01.parquet   # /path/to/exp3
```

- **Safe for concurrent snapshots** - Multiple processes can snapshot different paths simultaneously
- **No overwrites** - Each path gets its own file (filename includes hash of source path)
- **Unified queries** - DuckDB reads all `*.parquet` files together as one table

### Parallel Processing

The `--workers` option uses different strategies based on workload:

| Workload | Executor | Why |
|----------|----------|-----|
| Without `--checksum` | ThreadPoolExecutor | I/O-bound `lstat()` calls, low overhead |
| With `--checksum` | ProcessPoolExecutor | CPU-bound SHA-256, bypasses GIL |

```bash
# I/O-bound (threads) - good for metadata-only snapshots
lcls-catalog snapshot /data -o catalog/ --format parquet --workers 16

# CPU-bound (processes) - good for checksum computation
lcls-catalog snapshot /data -o catalog/ --format parquet --workers 16 --checksum
```

## Running Tests

```bash
pip install -e ".[dev]"
pytest
```
