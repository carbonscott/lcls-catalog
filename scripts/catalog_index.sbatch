#!/bin/bash
#SBATCH --job-name=catalog-index
#SBATCH --partition=milano
#SBATCH --account=lcls:prjdat21
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=32
#SBATCH --output=slurm_%j.log

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

# Run the indexer with any passed arguments
"$PROJECT_DIR/scripts/run_catalog_index.sh" "$@"

echo "Job complete"
