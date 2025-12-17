# lcls-catalog environment setup
# Source this file before running lcls-catalog with uv:
#   source /sdf/scratch/users/c/cwang31/proj-lcls-catalog/env.sh

# Add uv to PATH (needed for Slurm nodes)
export PATH="$HOME/.local/bin:$PATH"

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

# LCLS environment (provides sbatch in PATH for cron)
export PSCONDA_SH=/sdf/group/lcls/ds/ana/sw/conda1/manage/bin/psconda.sh

# Convenience wrapper for lcls-catalog commands
# Usage: lcat <command> [args...]
# - For read commands (query, find, ls, tree, stats, consolidate, snapshots):
#   Automatically uses $CATALOG_DATA_DIR as the catalog directory
# - For snapshot: Automatically adds -o $CATALOG_DATA_DIR
lcat() {
    local cmd="${1:-}"
    if [[ -z "$cmd" ]]; then
        uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog --help
        return
    fi
    shift
    case "$cmd" in
        query|find|ls|tree|stats|consolidate|snapshots)
            uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog "$cmd" "$CATALOG_DATA_DIR" "$@"
            ;;
        snapshot)
            uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog "$cmd" "$@" -o "$CATALOG_DATA_DIR"
            ;;
        *)
            uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog "$cmd" "$@"
            ;;
    esac
}
