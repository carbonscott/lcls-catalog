# lcls-catalog Skills Guide

A practical guide for using the `lcls-catalog` CLI tool to manage and query LCLS experiment data metadata.

## Table of Contents

1. [Introduction](#introduction)
2. [Environment Setup](#environment-setup)
3. [Command Reference](#command-reference)
4. [Common Workflows](#common-workflows)
5. [Tips and Troubleshooting](#tips-and-troubleshooting)

---

## Introduction

### What is lcls-catalog?

`lcls-catalog` is a lightweight catalog tool for browsing and searching LCLS data metadata. It creates persistent snapshots of file metadata (paths, sizes, timestamps, etc.) that you can query even after the original files have been purged from disk.

### Why use it?

- **Before purge**: Index your experiment data to preserve metadata
- **After purge**: Search and browse what files existed, their sizes, and locations
- **Data management**: Find large files, track changes, and answer questions like "what HDF5 files were over 1GB?"

### LCLS Data Structure

LCLS data is organized by **hutches** (beamline areas) and **experiments**:

```
/sdf/data/lcls/ds/
├── amo/           # Hutch: AMO
│   ├── amo12345/  # Experiment
│   └── amo67890/
├── cxi/           # Hutch: CXI
├── mec/           # Hutch: MEC
├── mfx/           # Hutch: MFX
└── ...
```

---

## Environment Setup

### 1. Source the environment file

Before using `lcls-catalog`, source the environment file:

```bash
source /sdf/scratch/users/c/cwang31/proj-lcls-catalog/env.sh
```

### 2. Environment variables

This sets three key variables:

| Variable | Purpose | Example Value |
|----------|---------|---------------|
| `LCLS_CATALOG_APP_DIR` | Path to lcls-catalog project | `/sdf/scratch/users/c/cwang31/proj-lcls-catalog` |
| `CATALOG_DATA_DIR` | Directory for catalog parquet files | `/sdf/data/lcls/ds/prj/prjdat21/results/cwang31/lcls_parquet/` |
| `UV_CACHE_DIR` | UV package cache location | `.../.uv-cache` |

### 3. Running commands

All commands follow this pattern:

```bash
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog <command> [options]
```

You can verify it works with:

```bash
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog --help
```

---

## Command Reference

### 1. snapshot - Create catalog snapshots

Capture file metadata from a directory tree. Run this **before** data is purged.

**Syntax:**
```bash
lcls-catalog snapshot <path> -e <experiment> -o <output_dir> [options]
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `-o, --output` | Output catalog directory | `catalog` |
| `-e, --experiment` | Experiment identifier | None |
| `--checksum` | Compute SHA-256 checksums (slower) | Off |
| `--workers` | Number of parallel workers | 1 |
| `--batch-size` | Files per batch | 10000 |

**Examples:**

```bash
# Basic snapshot of an experiment
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot \
  /sdf/data/lcls/ds/cxi/cxi12345 \
  -e cxi12345 \
  -o "$CATALOG_DATA_DIR"

# With parallel workers (faster for large directories)
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot \
  /sdf/data/lcls/ds/mfx/mfx67890 \
  -e mfx67890 \
  -o "$CATALOG_DATA_DIR" \
  --workers 4
```

**Example output:**
```
Cataloged /sdf/data/lcls/ds/cxi/cxi12345: 15234 added
```

---

### 2. ls - List files or directories

List files in a specific directory or show subdirectory statistics.

**Syntax:**
```bash
lcls-catalog ls <catalog_dir> <path> [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-d, --dirs` | List subdirectories with stats instead of files |
| `--on-disk` | Only show files currently on disk |

**Examples:**

```bash
# List files in a directory
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog ls \
  "$CATALOG_DATA_DIR" \
  /sdf/data/lcls/ds/cxi/cxi12345/scratch

# List subdirectories with file counts and sizes
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog ls \
  "$CATALOG_DATA_DIR" \
  /sdf/data/lcls/ds/cxi/cxi12345 \
  --dirs
```

**Example output (with --dirs):**
```
scratch                                      1523 files      45.2 GB
xtc                                          8234 files     823.1 GB
hdf5                                         2156 files     156.8 GB
```

---

### 3. find - Search for files

Search for files by pattern with optional filters. This is one of the most useful commands.

**Syntax:**
```bash
lcls-catalog find <catalog_dir> <pattern> [options]
```

**Pattern syntax:** Uses SQL LIKE patterns where `%` is the wildcard (like `*` in shell).

**Options:**

| Option | Description |
|--------|-------------|
| `--size-gt SIZE` | Minimum size (e.g., `1GB`, `500MB`) |
| `--size-lt SIZE` | Maximum size |
| `-e, --experiment` | Filter by experiment |
| `--exclude PATTERN` | Exclude paths matching pattern (repeatable) |
| `--on-disk` | Only show files currently on disk |
| `--removed` | Only show files that have been removed |
| `--show-status` | Show [removed] status for removed files |
| `-H, --human-readable` | Print sizes in human-readable format |
| `--no-symlinks` | Exclude symbolic links |

**Examples:**

```bash
# Find all HDF5 files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find \
  "$CATALOG_DATA_DIR" \
  "%.h5"

# Find large HDF5 files (>1GB) with human-readable sizes
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find \
  "$CATALOG_DATA_DIR" \
  "%.h5" \
  --size-gt 1GB \
  -H

# Find all XTC files for a specific experiment
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find \
  "$CATALOG_DATA_DIR" \
  "%.xtc2" \
  -e cxi12345

# Find files that have been removed (purged)
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find \
  "$CATALOG_DATA_DIR" \
  "%.h5" \
  --removed \
  --show-status
```

**Example output:**
```
/sdf/data/lcls/ds/cxi/cxi12345/scratch/data001.h5	2.3 GB
/sdf/data/lcls/ds/cxi/cxi12345/scratch/data002.h5	1.8 GB
/sdf/data/lcls/ds/cxi/cxi12345/scratch/data003.h5	3.1 GB
```

---

### 4. tree - Show directory tree

Display a tree structure of directories with file counts.

**Syntax:**
```bash
lcls-catalog tree <catalog_dir> <path> [--depth N]
```

**Example:**

```bash
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog tree \
  "$CATALOG_DATA_DIR" \
  /sdf/data/lcls/ds/cxi/cxi12345 \
  --depth 3
```

**Example output:**
```
/sdf/data/lcls/ds/cxi/cxi12345/
├── scratch/
│   ├── run0001/  (523 files, 45.2 GB)
│   └── run0002/  (612 files, 52.1 GB)
├── xtc/
│   └── smalldata/  (156 files, 12.3 GB)
└── hdf5/  (2156 files, 156.8 GB)
```

---

### 5. stats - Show catalog statistics

Display overall statistics for the catalog.

**Syntax:**
```bash
lcls-catalog stats <catalog_dir>
```

**Example:**

```bash
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog stats "$CATALOG_DATA_DIR"
```

**Example output:**
```
Total files indexed:  1,234,567
  Currently on disk:  987,654
  Removed:            246,913
Total size indexed:   45.2 TB
  Currently on disk:  38.1 TB
```

---

### 6. query - Run SQL queries

Execute raw SQL queries against the catalog. The table name is `files`.

**Syntax:**
```bash
lcls-catalog query <catalog_dir> "<sql>"
```

**Available columns:**
- `path`, `parent_path`, `filename`
- `size`, `mtime`
- `owner`, `group_name`, `permissions`
- `checksum` (if computed)
- `experiment`, `run`
- `on_disk` (boolean)
- `indexed_at`

**Examples:**

```bash
# Show first 10 files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog query \
  "$CATALOG_DATA_DIR" \
  "SELECT path, size FROM files LIMIT 10"

# Count files by experiment
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog query \
  "$CATALOG_DATA_DIR" \
  "SELECT experiment, COUNT(*) as count, SUM(size)/1e9 as gb FROM files GROUP BY experiment ORDER BY gb DESC LIMIT 20"

# Find files modified in the last 30 days
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog query \
  "$CATALOG_DATA_DIR" \
  "SELECT path, size FROM files WHERE mtime > now() - INTERVAL '30 days'"

# Find largest files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog query \
  "$CATALOG_DATA_DIR" \
  "SELECT path, size/1e9 as gb FROM files ORDER BY size DESC LIMIT 20"
```

---

### 7. consolidate - Merge snapshots

Merge base and delta snapshot files into new consolidated base files. Run this periodically to reduce the number of parquet files.

**Syntax:**
```bash
lcls-catalog consolidate <catalog_dir> [--archive <dir>]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--archive DIR` | Archive old files instead of deleting them |

**Example:**

```bash
# Consolidate and delete old files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog consolidate "$CATALOG_DATA_DIR"

# Consolidate and archive old files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog consolidate \
  "$CATALOG_DATA_DIR" \
  --archive /backup/old_snapshots
```

**Example output:**
```
Consolidated 15 experiments
  Removed 42 old files
```

---

### 8. snapshots - List snapshot files

List all snapshot files in the catalog.

**Syntax:**
```bash
lcls-catalog snapshots <catalog_dir> [-e <experiment>]
```

**Example:**

```bash
# List all snapshots
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshots "$CATALOG_DATA_DIR"

# Filter by experiment hash
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshots \
  "$CATALOG_DATA_DIR" \
  -e a3f2b8c1
```

**Example output:**
```
cxi12345/
  base  2024-06-01T10:30:00    15234 records      1234.5 KB
  delta 2024-06-15T14:20:00      523 records        45.2 KB

mfx67890/
  base  2024-06-02T09:15:00    23456 records      2345.6 KB
```

---

## Common Workflows

### Workflow 1: Index an experiment before purge

```bash
# 1. Set up environment
source /sdf/scratch/users/c/cwang31/proj-lcls-catalog/env.sh

# 2. Snapshot the experiment with parallel workers
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot \
  /sdf/data/lcls/ds/cxi/cxi12345 \
  -e cxi12345 \
  -o "$CATALOG_DATA_DIR" \
  --workers 4
```

### Workflow 2: Find large HDF5 files

```bash
# Find all HDF5 files over 1GB
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find \
  "$CATALOG_DATA_DIR" \
  "%.h5" \
  --size-gt 1GB \
  -H
```

### Workflow 3: Check what was removed after purge

```bash
# First, re-run snapshot to detect changes
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot \
  /sdf/data/lcls/ds/cxi/cxi12345 \
  -e cxi12345 \
  -o "$CATALOG_DATA_DIR"

# Then find removed files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find \
  "$CATALOG_DATA_DIR" \
  "%" \
  --removed \
  -H
```

### Workflow 4: Batch index all experiments

Use the provided batch script:

```bash
# Index all experiments with default settings
$LCLS_CATALOG_APP_DIR/examples/index_all_parquet.sh -o "$CATALOG_DATA_DIR"

# Custom: 64 parallel experiments, 8 workers each, only CXI and MFX hutches
$LCLS_CATALOG_APP_DIR/examples/index_all_parquet.sh \
  -o "$CATALOG_DATA_DIR" \
  -p 64 \
  -w 8 \
  -H "cxi mfx"
```

### Workflow 5: Generate a report of data by experiment

```bash
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog query \
  "$CATALOG_DATA_DIR" \
  "SELECT
     experiment,
     COUNT(*) as files,
     SUM(size)/1e12 as tb,
     SUM(CASE WHEN on_disk THEN size ELSE 0 END)/1e12 as tb_on_disk
   FROM files
   GROUP BY experiment
   ORDER BY tb DESC"
```

---

## Tips and Troubleshooting

### Performance tips

1. **Use `--workers`** for faster snapshots on large directories:
   ```bash
   lcls-catalog snapshot /path -o catalog/ --workers 8
   ```

2. **Skip checksums** unless you specifically need them - they're CPU-intensive:
   ```bash
   # Fast (no checksums - default)
   lcls-catalog snapshot /path -o catalog/

   # Slow (with checksums)
   lcls-catalog snapshot /path -o catalog/ --checksum
   ```

3. **Use `--no-symlinks`** in find to skip symbolic links if they're causing issues.

### Understanding base vs delta files

- **Base files** (`base_*.parquet`): Complete snapshot of all files at a point in time
- **Delta files** (`delta_*.parquet`): Changes since the last snapshot (added/modified/removed)

When you run `consolidate`, base + deltas are merged into a new base file.

### Common issues

**"No files matching pattern"**
- Check your pattern syntax - use `%` not `*` as wildcard
- Make sure the catalog has been populated with `snapshot` first

**Slow queries**
- Run `consolidate` to merge delta files if you have many
- Check if you're querying a very large catalog

**Environment not set**
```
Error: LCLS_CATALOG_APP_DIR environment variable is not set
```
Solution: Source the environment file first:
```bash
source /sdf/scratch/users/c/cwang31/proj-lcls-catalog/env.sh
```

### Quick reference card

```bash
# Setup
source /sdf/scratch/users/c/cwang31/proj-lcls-catalog/env.sh

# Create snapshot
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot /path -e exp -o "$CATALOG_DATA_DIR" --workers 4

# List files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog ls "$CATALOG_DATA_DIR" /path

# List directories
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog ls "$CATALOG_DATA_DIR" /path --dirs

# Find files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find "$CATALOG_DATA_DIR" "%.h5" -H

# Find large files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find "$CATALOG_DATA_DIR" "%.h5" --size-gt 1GB -H

# Show stats
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog stats "$CATALOG_DATA_DIR"

# Run SQL query
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog query "$CATALOG_DATA_DIR" "SELECT * FROM files LIMIT 10"
```
