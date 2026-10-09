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
#   --big-workers N   Threads per large experiment (default: 16)
#   --parallel N      Max concurrent experiments (default: 128)
#   --exp-timeout D   Give up on one experiment after D (default: 11h, `timeout` syntax)
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
BIG_WORKERS=16
# Large = materialized current state over 100 MB, about 3.5M files. These
# take hours, so they get more threads and are started first.
BIG_BYTES=${BIG_BYTES:-$((100 * 1024 * 1024))}
MAX_PARALLEL=128
# Under the 12h Slurm limit, so a snapshot that cannot finish is logged
# instead of vanishing with the job.
EXP_TIMEOUT=11h
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
        --big-workers)
            BIG_WORKERS="$2"
            shift 2
            ;;
        --exp-timeout)
            EXP_TIMEOUT="$2"
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
    local workers="$4"
    local start=$SECONDS

    if [[ "$DRY_RUN" == "true" ]]; then
        # Dry run: just count files
        file_count=$(find "$exp_path" -type f 2>/dev/null | wc -l)
        echo "$(date '+%Y-%m-%d %H:%M:%S'): [DRY-RUN] $hutch/$exp - $file_count files" >> "$LOG_FILE"
        return 0
    fi

    local rc=0
    output=$(timeout "$EXP_TIMEOUT" uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog snapshot "$exp_path" \
      -e "$exp" \
      -o "$OUTPUT_DIR" \
      --workers "$workers" 2>&1) || rc=$?
    local took="$((SECONDS - start))s, $workers workers"
    if [[ $rc -eq 124 ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S'): Warning: Timed out indexing $hutch/$exp after $EXP_TIMEOUT ($took)" >> "$LOG_FILE"
        return 1
    elif [[ $rc -ne 0 ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S'): Warning: Error indexing $hutch/$exp ($took): $output" >> "$LOG_FILE"
        return 1
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S'): $hutch/$exp - $output ($took)" >> "$LOG_FILE"
    return 0
}

# Size of an experiment's materialized current state (0 if none yet).
current_size() {
    local f size=0
    for f in "$OUTPUT_DIR/_state/current/$1__"*.parquet; do
        [[ -f "$f" ]] && size=$(stat -c %s "$f")
    done
    echo "$size"
}

export -f run_snapshot
export OUTPUT_DIR LOG_FILE LCLS_CATALOG_APP_DIR DRY_RUN EXP_TIMEOUT

# --- Main ---
# Lines already in the log, so the end-of-run check reads only this run.
LOG_START=0
if [[ -f "$LOG_FILE" ]]; then
    LOG_START=$(wc -l < "$LOG_FILE")
fi

log "=========================================="
log "Starting LCLS catalog indexing"
log "Output directory: $OUTPUT_DIR"
log "Hutches: $HUTCHES"
log "Max parallel: $MAX_PARALLEL, Workers per exp: $WORKERS ($BIG_WORKERS if over $((BIG_BYTES / 1024 / 1024)) MB), timeout per exp: $EXP_TIMEOUT"
log "Dry run: $DRY_RUN"
log "=========================================="

entries=()
for hutch in $HUTCHES; do
    hutch_dir="$LCLS_DATA/$hutch"

    if [[ ! -d "$hutch_dir" ]]; then
        log "Warning: Hutch directory $hutch_dir does not exist, skipping"
        continue
    fi

    for exp_path in "$hutch_dir"/*/; do
        [[ -d "$exp_path" ]] || continue
        exp=$(basename "$exp_path")
        entries+=("$(current_size "$exp") $hutch $exp $exp_path")
    done
done
log "Experiments: ${#entries[@]}, largest first"

job_count=0

while read -r size hutch exp exp_path; do
    workers=$WORKERS
    if [[ $size -gt $BIG_BYTES ]]; then
        workers=$BIG_WORKERS
    fi

    # Run snapshot in background
    run_snapshot "$exp_path" "$exp" "$hutch" "$workers" &

    job_count=$((job_count + 1))

    # Limit concurrent jobs
    if [[ $job_count -ge $MAX_PARALLEL ]]; then
        wait -n || true
        job_count=$((job_count - 1))
    fi
done < <(printf '%s\n' "${entries[@]}" | sort -rn)

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

# Any warning fails the run, so Slurm mails it (catalog_index.sbatch)
# instead of it sitting unread in the log.
warnings=$(tail -n +"$((LOG_START + 1))" "$LOG_FILE" \
    | grep -cE 'Warning:|could not refresh current state' || true)
if [[ $warnings -gt 0 ]]; then
    log "Finished with $warnings warning line(s), see $LOG_FILE"
    exit 1
fi
