# lcls-catalog Cron Job

## Architecture

```
sdfcron001 (cron at 1am daily)
    └── sbatch → milano node (12hr, 32 CPUs, 128G)
                    └── run_catalog_index.sh (parallel indexing)
```

The cron job on sdfcron001 submits a Slurm batch job to milano nodes where the actual indexing work runs.

### What a run does

- Snapshots every experiment of every hutch in `run_catalog_index.sh`, up to 128 at once, **largest first**. Size is that of the experiment's materialized current state, so the multi-hour snapshots start immediately.
- Experiments over 100 MB of current state (about 3.5M files) get 16 threads (`--big-workers`); the rest get 4 (`--workers`).
- Each snapshot streams its walk to a temporary Parquet file, diffs it against `_state/current` in DuckDB, writes a delta, and updates `_state/current` and `_state/dirs` for that experiment. Memory stays flat whatever the file count.
- A snapshot running longer than 11h (`--exp-timeout`) is stopped and logged, so it shows up before the 12h Slurm limit ends the job.
- Every experiment gets one line in `catalog_index.log` with its result, duration and thread count; failures and timeouts start with `Warning:`.

## Quick Reference

| Setting | Value |
|---------|-------|
| Cron node | `sdfcron001` |
| Schedule | Daily at 1:00 AM |
| Slurm partition | `milano` |
| Slurm account | `lcls:prjdat21` |
| Time limit | 12 hours (11 per experiment) |
| CPUs | 32 |
| Memory | 128G |

## Managing the Cron Job

```bash
# Check status (cron + running slurm jobs)
./scripts/catalog-cron.sh status

# Enable daily cron job
./scripts/catalog-cron.sh enable

# Disable cron job
./scripts/catalog-cron.sh disable

# Manually submit a job now
./scripts/catalog-cron.sh submit
```

## Testing

Test locally without affecting cron or Slurm jobs:

```bash
# Dry run - scan only, don't write catalog
./scripts/catalog-cron.sh test --dry-run

# Test with single hutch
./scripts/catalog-cron.sh test --hutches "prj"

# Test with specific settings
./scripts/catalog-cron.sh test --hutches "prj" --workers 2 --parallel 4
```

Test mode uses a separate lock file (`/tmp/catalog_index_test.lock`) so it never interferes with running cron jobs.

## Key Paths

| Resource | Location |
|----------|----------|
| Scripts | `scripts/` |
| Catalog data | `$CATALOG_DATA_DIR` (parquet files) |
| Index log | `$CATALOG_DATA_DIR/catalog_index.log` |
| Cron log | `$CATALOG_DATA_DIR/cron.log` |
| Slurm logs | `$CATALOG_DATA_DIR/slurm_*.log` |

## Environment Variables

Set in `env.sh`:

| Variable | Description |
|----------|-------------|
| `LCLS_CATALOG_APP_DIR` | Path to lcls-catalog project |
| `CATALOG_DATA_DIR` | Output directory for parquet files |
| `UV_CACHE_DIR` | UV cache location |

## Lock Files

| Lock File | Used By |
|-----------|---------|
| `/tmp/catalog_index.lock` | Cron/Slurm jobs |
| `/tmp/catalog_index_test.lock` | Local testing |

## Monitoring

```bash
# Check slurm job status
squeue -u $USER -n catalog-index

# Watch slurm log
tail -f $CATALOG_DATA_DIR/slurm_*.log

# Check catalog stats
source env.sh
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog stats "$CATALOG_DATA_DIR"

# Last night's failures and timeouts
grep "^$(date +%F).*Warning" $CATALOG_DATA_DIR/catalog_index.log

# Last night's slowest experiments
grep "^$(date +%F)" $CATALOG_DATA_DIR/catalog_index.log | sed -nE 's/^.*: ([a-z]+\/[^ ]+) .*\(([0-9]+)s, .*/\2s \1/p' | sort -rn | head
```

## Troubleshooting

**Job not starting?**
```bash
# Check queue
squeue -u $USER

# Check if lock file exists
ls -la /tmp/catalog_index.lock
```

**Cron not running?**
```bash
# Verify cron entry
ssh sdfcron001 "crontab -l"

# Check cron log
tail -20 $CATALOG_DATA_DIR/cron.log
```
