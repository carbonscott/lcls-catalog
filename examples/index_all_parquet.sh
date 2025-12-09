#!/bin/bash
# Index all LCLS experiment folders using Parquet format with parallel workers
#
# Optimization strategy:
# - Run multiple experiments in parallel (process-level parallelism)
# - Each experiment uses fewer threads (I/O parallelism within process)
# - This spreads load across different directory trees for better metadata server utilization

set -e  # Exit on error

# --- Environment variable checks ---
if [[ -z "$LCLS_CATALOG_APP_DIR" ]]; then
    echo "Error: LCLS_CATALOG_APP_DIR environment variable is not set" >&2
    echo "Please set it to the path of the lcls-catalog project:" >&2
    echo "  export LCLS_CATALOG_APP_DIR=/path/to/proj-lcls-catalog" >&2
    exit 1
fi

LCLS_DATA="${LCLS_DATA:-/sdf/data/lcls/ds}"

# --- Default values ---
MAX_PARALLEL=128
WORKERS=4
HUTCHES="amo cxi mec mfx tmo ued rix det mob prj"
OUTPUT_DIR=""

# --- Usage ---
usage() {
    cat <<EOF
Usage: $(basename "$0") -o OUTPUT_DIR [-p MAX_PARALLEL] [-w WORKERS] [-H HUTCHES]

Index LCLS experiment folders to Parquet catalog format.

Required:
  -o OUTPUT_DIR    Directory to write parquet catalog files

Optional:
  -p MAX_PARALLEL  Number of experiments to process concurrently (default: $MAX_PARALLEL)
  -w WORKERS       Threads per experiment (default: $WORKERS)
  -H HUTCHES       Space-separated list of hutches to process
                   (default: "$HUTCHES")

Environment variables:
  LCLS_CATALOG_APP_DIR  Path to lcls-catalog project (required)
  LCLS_DATA         Path to LCLS data directory (default: /sdf/data/lcls/ds)

Examples:
  $(basename "$0") -o ./catalogs/lcls_parquet
  $(basename "$0") -o ./catalogs/lcls_parquet -p 64 -w 8
  $(basename "$0") -o ./catalogs/lcls_parquet -H "amo cxi"
EOF
    exit 1
}

# --- Parse arguments ---
while getopts "o:p:w:H:" opt; do
    case $opt in
        o) OUTPUT_DIR="$OPTARG" ;;
        p) MAX_PARALLEL="$OPTARG" ;;
        w) WORKERS="$OPTARG" ;;
        H) HUTCHES="$OPTARG" ;;
        *) usage ;;
    esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
    echo "Error: -o OUTPUT_DIR is required" >&2
    usage
fi

# Derive log file from output directory
LOG_FILE="$OUTPUT_DIR/indexing.log"

mkdir -p "$OUTPUT_DIR"

echo "========================================" | tee -a "$LOG_FILE"
echo "Starting LCLS Parquet indexing at $(date)" | tee -a "$LOG_FILE"
echo "Output directory: $OUTPUT_DIR" | tee -a "$LOG_FILE"
echo "Max parallel experiments: $MAX_PARALLEL" | tee -a "$LOG_FILE"
echo "Workers per experiment: $WORKERS" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Function to run a single snapshot
run_snapshot() {
    local exp_path="$1"
    local exp="$2"
    local hutch="$3"

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
export OUTPUT_DIR WORKERS LOG_FILE LCLS_CATALOG_APP_DIR

job_count=0

for hutch in $HUTCHES; do
    echo "" | tee -a "$LOG_FILE"
    echo "=== Processing hutch: $hutch ===" | tee -a "$LOG_FILE"

    hutch_dir="$LCLS_DATA/$hutch"

    if [[ ! -d "$hutch_dir" ]]; then
        echo "Warning: Hutch directory $hutch_dir does not exist, skipping" | tee -a "$LOG_FILE"
        continue
    fi

    for exp_path in "$hutch_dir"/*/; do
        [[ -d "$exp_path" ]] || continue

        exp=$(basename "$exp_path")

        echo "$(date '+%Y-%m-%d %H:%M:%S'): Starting $hutch/$exp" | tee -a "$LOG_FILE"

        # Run snapshot in background
        run_snapshot "$exp_path" "$exp" "$hutch" &

        job_count=$((job_count + 1))

        # Limit concurrent jobs
        if [[ $job_count -ge $MAX_PARALLEL ]]; then
            wait -n  # Wait for any one job to finish
            job_count=$((job_count - 1))
        fi
    done
done

# Wait for remaining jobs
wait

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "Indexing complete at $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Final catalog statistics:" | tee -a "$LOG_FILE"
uv run --project "$LCLS_CATALOG_APP_DIR" lcls-catalog stats "$OUTPUT_DIR" | tee -a "$LOG_FILE"
