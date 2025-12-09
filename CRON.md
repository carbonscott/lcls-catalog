# lcls-catalog Cron Job

## Architecture

```
sdfcron001 (cron at 2am daily)
    └── sbatch → milano node (6hr, 32 CPUs)
                    └── run_catalog_index.sh (parallel indexing)
```

The cron job on sdfcron001 submits a Slurm batch job to milano nodes where the actual indexing work runs.

## Quick Reference

| Setting | Value |
|---------|-------|
| Cron node | `sdfcron001` |
| Schedule | Daily at 2:00 AM |
| Slurm partition | `milano` |
| Slurm account | `lcls:prjdat21` |
| Time limit | 6 hours |
| CPUs | 32 |

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
