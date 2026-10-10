#!/bin/bash
#SBATCH --job-name=catalog-index
#SBATCH --partition=milano
#SBATCH --account=lcls:prjdat21
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=32
# The streaming snapshot peaked at 26 GB on 2026-10-09. The old code was
# OOM-killed at the 125G that milano gave it by default.
#SBATCH --mem=128G
#SBATCH --output=slurm_%j.log
# Mail the submitting user (or $CATALOG_ALERT_EMAIL, see catalog-cron.sh)
# when the job fails, times out or runs out of memory. Any Warning: line in
# catalog_index.log makes run_catalog_index.sh fail the job.
#SBATCH --mail-type=FAIL

# LCLS Catalog Indexing - Slurm Batch Job
#
# Submits to milano partition for CPU-intensive parallel indexing.
# All arguments are passed through to run_catalog_index.sh.
#
# Usage:
#   sbatch catalog_index.sbatch [-- OPTIONS]
#   sbatch catalog_index.sbatch -- --hutches "prj"
#   sbatch catalog_index.sbatch -- --dry-run

# Use absolute paths (SCRIPT_DIR doesn't work in Slurm - script is copied to /var/spool)
PROJECT_DIR="${LCLS_CATALOG_APP_DIR:?LCLS_CATALOG_APP_DIR not set - source env.sh first}"

# Source environment
source "$PROJECT_DIR/env.sh"

echo "Starting catalog indexing on $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Arguments: $*"

# Run the indexer with any passed arguments; its exit code is the job's
rc=0
"$PROJECT_DIR/scripts/run_catalog_index.sh" "$@" || rc=$?

echo "Job complete (exit $rc)"
exit $rc
