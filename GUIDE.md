# lcls-catalog Setup Guide

## Environment Setup

Source the environment file before running lcls-catalog:

```bash
source /sdf/scratch/users/c/cwang31/proj-lcls-catalog/env.sh
```

This sets:
- `LCLS_CATALOG_APP_DIR` - Path to the lcls-catalog project
- `CATALOG_DATA_DIR` - Directory for catalog parquet files
- `UV_CACHE_DIR` - Persistent uv cache location

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

## Batch Indexing

Index all LCLS experiments using the batch script:

```bash
# Basic usage (requires -o flag)
$LCLS_CATALOG_APP_DIR/examples/index_all_parquet.sh -o "$CATALOG_DATA_DIR"

# With custom parallelism
$LCLS_CATALOG_APP_DIR/examples/index_all_parquet.sh -o "$CATALOG_DATA_DIR" -p 64 -w 8

# Index specific hutches only
$LCLS_CATALOG_APP_DIR/examples/index_all_parquet.sh -o "$CATALOG_DATA_DIR" -H "cxi mfx"
```

### Batch script options

| Flag | Description | Default |
|------|-------------|---------|
| `-o` | Output directory (required) | - |
| `-p` | Max parallel experiments | 128 |
| `-w` | Workers per experiment | 4 |
| `-H` | Hutches to process | all |

## Running on Slurm (Milano/Ada Nodes)

For large-scale indexing, use a Slurm allocation to get more CPUs.

### Using a placeholder job

Submit a placeholder job that holds resources:
```bash
sbatch scripts/catalog_index.sbatch
```

Or use an existing hetjob allocation:
```bash
# Run on milano (het-group=0)
srun --het-group=0 --jobid=<JOBID> bash -c "source env.sh && scripts/run_catalog_index.sh"

# Run on ada (het-group=1)
srun --het-group=1 --jobid=<JOBID> bash -c "source env.sh && scripts/run_catalog_index.sh"
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
# 16893372+0  placeholder    RUNNING      1  sdfmilan238

# Run indexing on the allocated node
srun --het-group=0 --jobid=16893372 bash -c "source env.sh && scripts/run_catalog_index.sh --hutches prj"

# Oops, need to stop it (find PID first)
ps aux | grep srun | grep $USER
# cwang31  1678887  ... srun --het-group=0 ...

# Kill the task (allocation stays)
kill 1678887

# Allocation still there
squeue --me
# 16893372+0  placeholder    RUNNING  ...
```

## Directory Layout

```
$CATALOG_DATA_DIR/
├── *.parquet          # Catalog snapshot files
├── catalog_index.log  # Indexing log
├── slurm_*.log        # Slurm job logs
└── .uv-cache/         # UV package cache
```
