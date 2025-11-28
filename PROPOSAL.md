## Executive Summary

When LCLS performs routine data purges, users lose visibility into what data existed and where it resided. This proposal describes a lightweight catalog system that preserves the directory hierarchy and file metadata after purge, enabling users to browse, search, and identify data for restoration—without requiring the actual files to remain on disk.

---

## Problem Statement

Current data purge operations at LCLS result in complete removal of files from the filesystem. After a purge:

- `ls` returns empty directories or nothing at all
- Users cannot remember exact paths, filenames, or directory structures
- Identifying what to restore from tape/cold storage becomes guesswork
- Institutional knowledge of data organization is lost

Commercial solutions like Dropbox and iCloud solve this with "placeholder files" that appear in the filesystem but fetch content on-demand. However, this approach requires deep filesystem integration (FUSE, kernel drivers) that adds complexity and operational risk in HPC environments.

---

## Proposed Solution

Build a **Data Catalog** that captures filesystem metadata before purge and provides a virtual browsing experience afterward.

### Core Concept

```
┌─────────────────────────────────────────────────────────────┐
│                     BEFORE PURGE                            │
│  /cds/data/lcls/exp/xpp/xpp12345/                          │
│  ├── scratch/                                               │
│  │   ├── run0001/                                          │
│  │   │   ├── image_0001.h5  (2.1 GB)                       │
│  │   │   ├── image_0002.h5  (2.1 GB)                       │
│  │   │   └── ...                                           │
│  │   └── run0002/                                          │
│  └── results/                                               │
│       └── analysis_v3.npz  (450 MB)                        │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼  [Catalog Snapshot]
                           │
┌─────────────────────────────────────────────────────────────┐
│                     AFTER PURGE                             │
│                                                             │
│  Filesystem:  (empty)                                       │
│                                                             │
│  Catalog:     Fully browsable tree with metadata            │
│               - Original paths preserved                    │
│               - File sizes, timestamps, ownership           │
│               - Archive location references                 │
│               - Checksums for verification                  │
└─────────────────────────────────────────────────────────────┘
```

### What We Capture

| Field | Description | Example |
|-------|-------------|---------|
| `path` | Full original path | `/cds/data/lcls/exp/xpp/xpp12345/scratch/run0001/image_0001.h5` |
| `size` | File size in bytes | `2251799813` |
| `mtime` | Last modification time | `2024-03-15T14:32:01Z` |
| `owner` | Unix owner | `xpp12345` |
| `group` | Unix group | `ps-data` |
| `permissions` | File mode | `0644` |
| `checksum` | SHA-256 or xxHash | `a3f2b8c1...` |
| `archive_uri` | Location in cold storage | `hpss://archive/lcls/2024/xpp12345/tape001.tar#offset=12345` |
| `experiment` | LCLS experiment ID | `xpp12345` |
| `run` | Run number (if applicable) | `1` |
| `purge_date` | When the file was purged | `2024-06-01` |

---

## Technical Design

### Storage Backend: SQLite

SQLite is the recommended backend for the initial implementation:

- **Zero infrastructure** — single file, no server process
- **SQL queryable** — familiar interface for ad-hoc queries
- **Proven at scale** — handles billions of rows with proper indexing
- **Portable** — catalog file can be copied, backed up, shared

#### Schema

```sql
CREATE TABLE files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    parent_path TEXT NOT NULL,  -- for tree navigation
    filename TEXT NOT NULL,
    size INTEGER,
    mtime INTEGER,  -- Unix timestamp
    owner TEXT,
    group_name TEXT,
    permissions INTEGER,
    checksum TEXT,
    archive_uri TEXT,
    experiment TEXT,
    run INTEGER,
    purge_date TEXT,
    UNIQUE(path, purge_date)
);

-- Indexes for common access patterns
CREATE INDEX idx_parent ON files(parent_path);
CREATE INDEX idx_experiment ON files(experiment);
CREATE INDEX idx_filename ON files(filename);
CREATE INDEX idx_mtime ON files(mtime);
CREATE INDEX idx_size ON files(size);

-- Full-text search on paths
CREATE VIRTUAL TABLE files_fts USING fts5(path, filename);
```

### Alternative: Parquet for Analytics

For large-scale analytics across many experiments, Parquet files offer advantages:

- Columnar storage — efficient for aggregations
- Native support in pandas, polars, DuckDB
- Compresses well for archival

The system could export to Parquet periodically for analytical workloads while using SQLite for interactive browsing.

---

## User Interface Options

### 1. Command-Line Browser (Recommended MVP)

A TUI (terminal user interface) that mimics filesystem navigation:

```
$ lcls-catalog browse xpp12345

╭─ xpp12345 (purged: 2024-06-01) ─────────────────────────────╮
│ 📁 scratch/                              45.2 TB   1,234 files│
│ 📁 results/                               2.1 GB      47 files│
│ 📁 calib/                               128.5 MB      12 files│
│                                                              │
│ [Enter] Open   [s] Search   [r] Request Restore   [q] Quit  │
╰──────────────────────────────────────────────────────────────╯
```

Features:
- Navigate with arrow keys like a file manager
- Search by filename, size range, date range
- Select files/directories for restore request
- Export selection to manifest file

### 2. Web Interface

A lightweight web UI (e.g., Streamlit, marimo, or FastAPI + htmx) for users who prefer graphical interaction:

- Tree view with expandable directories
- Sort by size, date, name
- Filter by file extension, size threshold
- Bulk selection for restore
- Integration with existing LCLS web portals

### 3. Python API

For programmatic access and integration with analysis workflows:

```python
from lcls_catalog import Catalog

cat = Catalog("xpp12345")

# Browse like a filesystem
for entry in cat.ls("/scratch/run0001"):
    print(f"{entry.filename}: {entry.size_human}")

# Search
large_files = cat.find(size_gt="1GB", extension=".h5")

# Request restore
cat.request_restore([
    "/scratch/run0001/image_0001.h5",
    "/scratch/run0001/image_0002.h5",
])
```

---

## Integration Points

### Pre-Purge Hook

The catalog snapshot should be triggered automatically before any purge operation:

```bash
#!/bin/bash
# pre-purge-hook.sh

EXPERIMENT=$1
DATA_PATH="/cds/data/lcls/exp/${EXPERIMENT:0:3}/${EXPERIMENT}"

# Capture metadata
lcls-catalog snapshot \
    --path "$DATA_PATH" \
    --experiment "$EXPERIMENT" \
    --output "/catalog/snapshots/${EXPERIMENT}_$(date +%Y%m%d).db"

# Proceed with purge
# ...
```

### Archive System Integration

The catalog should be populated with archive URIs after data is moved to cold storage:

```python
# After archiving to HPSS/tape
catalog.update_archive_locations(
    manifest="archive_manifest.csv",  # path -> archive_uri mapping
)
```

### Restore Workflow

When users request restoration:

1. User selects files in catalog UI
2. System generates restore manifest (list of archive URIs)
3. Manifest submitted to existing restore queue/system
4. Upon restore completion, catalog updated to reflect availability

---

## Implementation Phases

### Phase 1: MVP (2-3 weeks)

- [ ] SQLite schema and Python ingestion script
- [ ] Basic CLI for browsing (`ls`, `find`, `tree`)
- [ ] Manual snapshot trigger before purge
- [ ] Export selection to restore manifest

### Phase 2: Integration (2-3 weeks)

- [ ] Pre-purge hook integration
- [ ] Archive URI population from HPSS/tape system
- [ ] Restore request submission (integration with existing queue)

### Phase 3: User Experience (2-4 weeks)

- [ ] TUI browser with rich interface
- [ ] Web UI for graphical browsing
- [ ] Search improvements (fuzzy matching, regex)
- [ ] Usage analytics and reporting

### Phase 4: Scale & Polish (ongoing)

- [ ] Multi-experiment catalog aggregation
- [ ] Parquet export for analytics
- [ ] API for programmatic access
- [ ] Integration with Jupyter/analysis environments

---

## Resource Requirements

### Infrastructure

- **Storage**: ~1-10 GB per experiment catalog (depends on file count)
- **Compute**: Minimal — SQLite runs anywhere
- **Network**: None for basic operation

### Development

- **Initial MVP**: 1 developer, 2-3 weeks
- **Full implementation**: 1-2 developers, 2-3 months

### Dependencies

- Python 3.9+
- SQLite 3.35+ (for advanced features)
- Optional: textual (TUI), FastAPI (web), polars (analytics)

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Snapshot missed before purge | Data hierarchy lost | Automate via pre-purge hook; fail-safe to block purge if snapshot fails |
| Catalog grows too large | Slow queries | Partition by experiment; archive old catalogs; use appropriate indexes |
| Archive URIs become stale | Restore fails | Periodic validation; integration with archive system of record |
| User adoption low | Investment wasted | Start with power users; gather feedback; iterate on UX |

---

## Success Metrics

- **Discoverability**: Users can locate files for restore without support tickets
- **Time to restore**: Reduction in time from "I need that data" to restore request submitted
- **Coverage**: % of purged experiments with catalog snapshots
- **User satisfaction**: Qualitative feedback from experiment teams

---

## Conclusion

This Data Catalog system addresses a real pain point in LCLS data management with minimal infrastructure overhead. By preserving the directory hierarchy and metadata before purge, users retain the ability to browse and discover their data even after it leaves the filesystem. The phased approach allows for quick wins while building toward a comprehensive solution.

---

## Appendix: Quick Start Prototype

A minimal working prototype in ~50 lines of Python:

```python
#!/usr/bin/env python3
"""lcls_catalog_prototype.py - Minimal data catalog prototype"""

import sqlite3
import os
from pathlib import Path
from datetime import datetime

def create_catalog(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            parent_path TEXT,
            filename TEXT,
            size INTEGER,
            mtime REAL,
            owner TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parent ON files(parent_path)")
    return conn

def snapshot(conn: sqlite3.Connection, root: str):
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            fpath = Path(dirpath) / fname
            try:
                stat = fpath.stat()
                conn.execute(
                    "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?)",
                    (str(fpath), dirpath, fname, stat.st_size, stat.st_mtime, str(stat.st_uid))
                )
            except (OSError, PermissionError):
                continue
    conn.commit()

def ls(conn: sqlite3.Connection, path: str):
    cur = conn.execute(
        "SELECT filename, size FROM files WHERE parent_path = ?", (path,)
    )
    for row in cur:
        print(f"{row[0]:40} {row[1]:>15,} bytes")

def find(conn: sqlite3.Connection, pattern: str):
    cur = conn.execute(
        "SELECT path, size FROM files WHERE filename LIKE ?", (f"%{pattern}%",)
    )
    for row in cur:
        print(f"{row[0]:60} {row[1]:>15,} bytes")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: lcls_catalog.py <snapshot|ls|find> <path|pattern>")
        sys.exit(1)
    
    conn = create_catalog("catalog.db")
    cmd, arg = sys.argv[1], sys.argv[2]
    
    if cmd == "snapshot":
        snapshot(conn, arg)
    elif cmd == "ls":
        ls(conn, arg)
    elif cmd == "find":
        find(conn, arg)
```

Usage:
```bash
# Create snapshot
python lcls_catalog.py snapshot /cds/data/lcls/exp/xpp/xpp12345

# Browse after purge
python lcls_catalog.py ls /cds/data/lcls/exp/xpp/xpp12345/scratch/run0001

# Search for files
python lcls_catalog.py find "image_00"
```
