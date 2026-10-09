# lcls-catalog Setup Guide

## Environment Setup

Source the environment file before running lcls-catalog:

```bash
source /sdf/group/lcls/ds/dm/apps/dev/tools/lcls-catalog/env.sh
```

That directory is the deployed checkout of this repository, and the nightly job runs from it. Use it, or a checkout of the current `main`: older code writes deltas without updating `_state/`, which slows that experiment's queries until the next nightly run.

This sets:
- `LCLS_CATALOG_APP_DIR` - Path to the lcls-catalog project
- `CATALOG_DATA_DIR` - Directory for catalog parquet files (the deployment's untracked `env.local` points it at the shared catalog)
- `UV_CACHE_DIR` - uv cache location

## Running lcls-catalog

### Direct command

```bash
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog --help
```

### Common operations

```bash
# Create a snapshot of an experiment
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot /path/to/experiment -e exp_name -o "$CATALOG_DATA_DIR"

# List files in catalog
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog ls "$CATALOG_DATA_DIR" /path/to/dir

# Search for files
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog find "$CATALOG_DATA_DIR" "%.h5"

# Show catalog statistics
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog stats "$CATALOG_DATA_DIR"
```

### Using `lcat` shorthand

The `lcat` function (defined in `env.sh`) provides a simpler interface:

```bash
lcat stats                                  # Show catalog statistics
lcat find "%.h5" -H                         # Find HDF5 files
lcat find "%.h5" --size-gt 1GB -H           # Find large files
lcat query "SELECT * FROM files LIMIT 10"   # Run SQL query
lcat ls /path/to/dir                        # List files
lcat snapshot /path -e exp_name             # Create snapshot (auto -o)
```

The wrapper automatically uses `$CATALOG_DATA_DIR` for read commands and adds `-o $CATALOG_DATA_DIR` for snapshots.

## Batch Indexing

The nightly cron job indexes every experiment with `scripts/run_catalog_index.sh`; see [CRON.md](CRON.md) for the schedule, failure alerts and monitoring. To run the same script by hand:

```bash
# Submit a Slurm job now, exactly as cron does
scripts/catalog-cron.sh submit

# One hutch, on the current machine, with a separate lock file
scripts/catalog-cron.sh test --hutches "prj"
```

### Indexer options

| Flag | Description | Default |
|------|-------------|---------|
| `--hutches "..."` | Hutches to process | amo cxi mec mfx tmo ued rix xcs det mob prj |
| `--parallel N` | Max concurrent experiments | 128 |
| `--workers N` | Threads per experiment | 4 |
| `--big-workers N` | Threads per experiment over 100 MB of current state | 16 |
| `--exp-timeout D` | Stop one experiment after D (`timeout` syntax) | 11h |
| `--dry-run` | Count files only, write nothing | off |
| `--lock-file PATH` | Lock file | `/tmp/catalog_index.lock` |

The run exits 1 if any experiment logged a `Warning:` line.

`examples/index_all_parquet.sh` is an older standalone version of this script; the nightly job does not use it.

## Running on Slurm (Milano Nodes)

`scripts/catalog_index.sbatch` is the nightly job itself: it runs the indexer, then ends. Submit it with `scripts/catalog-cron.sh submit` (above).

To index inside an allocation you already hold:
```bash
srun --jobid=<JOBID> --ntasks=1 --cpus-per-task=32 bash -c "source env.sh && scripts/run_catalog_index.sh --hutches prj"
```

### Killing a task WITHOUT releasing the allocation

This is important: `srun` runs tasks on allocated nodes. Killing `srun` stops the task but keeps the allocation alive.

```bash
# Find your srun processes
ps aux | grep srun | grep $USER

# Kill srun by PID (allocation remains)
kill <PID>

# Verify allocation still exists
squeue --me
```

**Key distinction:**
- `kill <srun_pid>` → stops the task, keeps allocation
- `scancel <jobid>` → releases the entire allocation

### Example workflow

```bash
# Check your allocation
squeue --me
#     JOBID         NAME      STATE  NODES  NODELIST
# 16893372        interactive  RUNNING      1  sdfmilan238

# Run indexing on the allocated node
srun --jobid=16893372 --ntasks=1 --cpus-per-task=32 bash -c "source env.sh && scripts/run_catalog_index.sh --hutches prj"

# Oops, need to stop it (find PID first)
ps aux | grep srun | grep $USER
# cwang31  1678887  ... srun --jobid=16893372 ...

# Kill the task (allocation stays)
kill 1678887

# Allocation still there
squeue --me
# 16893372        interactive  RUNNING  ...
```

## Directory Layout

```
$CATALOG_DATA_DIR/
├── <exp>/base_*.parquet, delta_*.parquet  # Catalog snapshot files
├── _state/current/    # Deduplicated current state per experiment (what queries read)
├── _state/dirs/       # Recursive directory totals per experiment (the `dirs` table)
├── catalog_index.log  # Indexing log
├── cron.log           # Output of the cron submissions
└── slurm_*.log        # Slurm job logs
```
