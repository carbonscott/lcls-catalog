#!/bin/bash
#SBATCH --job-name=catalog-index
#SBATCH --partition=milano
#SBATCH --account=lcls:prjdat21
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=32
#SBATCH --output=/sdf/data/lcls/ds/prj/prjdat21/results/cwang31/lcls_parquet/slurm_%j.log

# LCLS Catalog Indexing - Slurm Batch Job
#
# Submits to milano partition for CPU-intensive parallel indexing.
# All arguments are passed through to run_catalog_index.sh.
#
# Usage:
#   sbatch catalog_index.sbatch [-- OPTIONS]
#   sbatch catalog_index.sbatch -- --hutches "prj"
#   sbatch catalog_index.sbatch -- --dry-run

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source environment
source "$SCRIPT_DIR/../env.sh"

echo "Starting catalog indexing on $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Arguments: $*"

# Run the indexer with any passed arguments
"$SCRIPT_DIR/run_catalog_index.sh" "$@"

echo "Job complete"
