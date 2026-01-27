#!/bin/bash
#
# run_catalog_index.sh - Index LCLS experiments to Parquet catalog
#
# This script indexes all experiments across hutches. Designed to run on
# milano nodes via Slurm, but can also run locally for testing.
#
# Usage:
#   ./run_catalog_index.sh [OPTIONS]
#
# Options:
#   --dry-run         Scan directories but don't write catalog files
#   --hutches "..."   Space-separated list of hutches to process
#   --workers N       Threads per experiment (default: 4)
#   --parallel N      Max concurrent experiments (default: 128)
#   --lock-file PATH  Override default lock file
#
# Environment variables (required):
#   LCLS_CATALOG_APP_DIR  Path to lcls-catalog project
#   CATALOG_DATA_DIR      Output directory for parquet files

set -euo pipefail

# --- Environment checks ---
if [[ -z "${LCLS_CATALOG_APP_DIR:-}" ]]; then
    echo "Error: LCLS_CATALOG_APP_DIR not set" >&2
    exit 1
fi

if [[ -z "${CATALOG_DATA_DIR:-}" ]]; then
    echo "Error: CATALOG_DATA_DIR not set" >&2
    exit 1
fi

# --- Default values ---
LCLS_DATA="${LCLS_DATA:-/sdf/data/lcls/ds}"
HUTCHES="amo cxi mec mfx tmo ued rix xcs det mob prj"
WORKERS=4
MAX_PARALLEL=128
DRY_RUN=false
LOCK_FILE="/tmp/catalog_index.lock"

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --hutches)
            HUTCHES="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --parallel)
            MAX_PARALLEL="$2"
            shift 2
            ;;
        --lock-file)
            LOCK_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# --- Derived paths ---
OUTPUT_DIR="$CATALOG_DATA_DIR"
LOG_FILE="$CATALOG_DATA_DIR/catalog_index.log"

mkdir -p "$OUTPUT_DIR"

# --- Logging helper ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# --- File locking ---
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    log "Another indexing job is running (lock: $LOCK_FILE), exiting"
    exit 0
fi

# --- Snapshot function ---
run_snapshot() {
    local exp_path="$1"
    local exp="$2"
    local hutch="$3"

    if [[ "$DRY_RUN" == "true" ]]; then
        # Dry run: just count files
        file_count=$(find "$exp_path" -type f 2>/dev/null | wc -l)
        echo "$(date '+%Y-%m-%d %H:%M:%S'): [DRY-RUN] $hutch/$exp - $file_count files" >> "$LOG_FILE"
        return 0
    fi

    output=$(uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot "$exp_path" \
      -e "$exp" \
      -o "$OUTPUT_DIR" \
      --workers "$WORKERS" 2>&1) || {
        echo "$(date '+%Y-%m-%d %H:%M:%S'): Warning: Error indexing $hutch/$exp: $output" >> "$LOG_FILE"
        return 1
    }

    echo "$(date '+%Y-%m-%d %H:%M:%S'): $hutch/$exp - $output" >> "$LOG_FILE"
    return 0
}

export -f run_snapshot
export OUTPUT_DIR WORKERS LOG_FILE LCLS_CATALOG_APP_DIR DRY_RUN

# --- Main ---
log "=========================================="
log "Starting LCLS catalog indexing"
log "Output directory: $OUTPUT_DIR"
log "Hutches: $HUTCHES"
log "Max parallel: $MAX_PARALLEL, Workers per exp: $WORKERS"
log "Dry run: $DRY_RUN"
log "=========================================="

job_count=0

for hutch in $HUTCHES; do
    log "=== Processing hutch: $hutch ==="

    hutch_dir="$LCLS_DATA/$hutch"

    if [[ ! -d "$hutch_dir" ]]; then
        log "Warning: Hutch directory $hutch_dir does not exist, skipping"
        continue
    fi

    for exp_path in "$hutch_dir"/*/; do
        [[ -d "$exp_path" ]] || continue

        exp=$(basename "$exp_path")

        # Run snapshot in background
        run_snapshot "$exp_path" "$exp" "$hutch" &

        job_count=$((job_count + 1))

        # Limit concurrent jobs
        if [[ $job_count -ge $MAX_PARALLEL ]]; then
            wait -n || true
            job_count=$((job_count - 1))
        fi
    done
done

# Wait for remaining jobs
wait

log "=========================================="
log "Indexing complete"
log "=========================================="

# Show stats (skip for dry run)
if [[ "$DRY_RUN" != "true" ]]; then
    log "Final catalog statistics:"
    uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog stats "$OUTPUT_DIR" 2>&1 | tee -a "$LOG_FILE"
fi
