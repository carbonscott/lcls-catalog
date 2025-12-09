# lcls-catalog environment setup
# Source this file before running lcls-catalog with uv:
#   source /sdf/scratch/users/c/cwang31/proj-lcls-catalog/env.sh

# Path to the lcls-catalog project (for uv run --project)
export LCLS_CATALOG_APP_DIR=/sdf/scratch/users/c/cwang31/proj-lcls-catalog

# Directory where catalog parquet files are stored
export CATALOG_DATA_DIR=/sdf/data/lcls/ds/prj/prjdat21/results/cwang31/lcls_parquet/

# UV cache directory (persistent across sessions)
export UV_CACHE_DIR=/sdf/data/lcls/ds/prj/prjdat21/results/cwang31/lcls_parquet/.uv-cache

# Cron configuration (optional - uncomment to override defaults)
# export CRON_NODE=sdfcron001           # Node where cron runs
# export CRON_SCHEDULE="0 2 * * *"      # Schedule: daily at 2am
# export CRON_LOG="$CATALOG_DATA_DIR/cron.log"
