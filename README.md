# lcls-catalog

A lightweight catalog for browsing and searching LCLS data metadata after purge.

## Installation

### With uv (recommended)

```bash
# Run directly - no install needed
uv run lcls-catalog --help

# Or install explicitly
uv pip install -e .
```

### With pip

```bash
pip install -e .
```

## Quick Start

### Create a snapshot before purge

```bash
lcls-catalog snapshot /path/to/experiment -e xpp12345 -o catalog/

# With parallel workers
lcls-catalog snapshot /path/to/experiment -e xpp12345 -o catalog/ --workers 4
```

### Browse after purge

```bash
# List directories with stats
lcls-catalog ls catalog/ /path/to/experiment --dirs

# List files in a directory
lcls-catalog ls catalog/ /path/to/experiment/scratch/run0001

# Search for files (output: path<tab>bytes by default)
lcls-catalog find catalog/ "%.h5"
lcls-catalog find catalog/ "%.h5" -H              # human-readable sizes
lcls-catalog find catalog/ "image_%" --size-gt 1GB

# Show tree structure
lcls-catalog tree catalog/ /path/to/experiment --depth 3

# Show catalog stats
lcls-catalog stats catalog/
```

## Python API

```python
from lcls_catalog import ParquetCatalog

with ParquetCatalog("catalog/") as cat:
    # Create snapshot (with optional parallel workers)
    cat.snapshot("/path/to/experiment", experiment="xpp12345", workers=4)

    # Browse
    for f in cat.ls("/path/to/experiment/scratch/run0001"):
        print(f"{f.filename}: {f.size} bytes")

    # Search
    results = cat.find("%.h5", size_gt=1_000_000)
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
lcls-catalog snapshot /data -o catalog/ --workers 16

# CPU-bound (processes) - good for checksum computation
lcls-catalog snapshot /data -o catalog/ --workers 16 --checksum
```

## Running Tests

```bash
# With uv
uv run --extra dev pytest

# With pip
pip install -e ".[dev]"
pytest
```
