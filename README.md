# lcls-catalog

A lightweight catalog for browsing and searching LCLS data metadata after purge.

## Quick Start

```bash
# From the repo directory
source ./env.sh

lcat stats                       # Show catalog statistics
lcat find "%.h5" -H              # Find HDF5 files
lcat find "%.h5" --size-gt 1GB   # Find large files
lcat query "SELECT ..."          # Run SQL query (tables: files, dirs)
lcat snapshot /path -e exp       # Create snapshot
lcat refresh                     # Rebuild materialized state if behind
```

Queries are capped at 8GB of memory, 8 threads and 90 s by default
(`LCAT_MEMORY_LIMIT`, `LCAT_THREADS`, `LCAT_TIMEOUT`); see SKILLS.md.

**Tip:** Add to your `~/.bashrc` for persistent access:
```bash
source /path/to/your/lcls-catalog/env.sh
```

See [SKILLS.md](SKILLS.md) for detailed command reference and workflows.

## Python API

```python
from lcls_catalog import ParquetCatalog

with ParquetCatalog("catalog/") as cat:
    cat.snapshot("/path", experiment="exp", workers=4)
    results = cat.find("%.h5", size_gt=1_000_000)
```

## Running Tests

```bash
uv run --extra dev pytest
```
