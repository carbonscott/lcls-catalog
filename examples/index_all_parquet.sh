#!/bin/bash
# Index all LCLS experiment folders using Parquet format with parallel workers
#
# Optimization strategy:
# - Run multiple experiments in parallel (process-level parallelism)
# - Each experiment uses fewer threads (I/O parallelism within process)
# - This spreads load across different directory trees for better metadata server utilization

set -e  # Exit on error

# Configuration - modify these paths for your environment
LCLS_DATA="/sdf/data/lcls/ds"
OUTPUT_DIR="./catalogs/lcls_parquet"
LOG_FILE="./catalogs/indexing_parquet.log"

# Parallelism settings
MAX_PARALLEL=4   # Number of experiments to process concurrently
WORKERS=4        # Threads per experiment (4 experiments × 4 workers = 16 total)

# Hutches to process (lowercase only, excluding uppercase symlinks)
HUTCHES="amo cxi mec mfx tmo ued rix det mob prj"

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

    output=$(lcls-catalog snapshot "$exp_path" \
      -e "$exp" \
      -o "$OUTPUT_DIR" \
      --format parquet \
      --workers "$WORKERS" 2>&1) || {
        echo "$(date '+%Y-%m-%d %H:%M:%S'): Warning: Error indexing $hutch/$exp: $output" >> "$LOG_FILE"
        return 1
    }

    echo "$(date '+%Y-%m-%d %H:%M:%S'): $hutch/$exp - $output" >> "$LOG_FILE"
    return 0
}

export -f run_snapshot
export OUTPUT_DIR WORKERS LOG_FILE

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
lcls-catalog stats "$OUTPUT_DIR" | tee -a "$LOG_FILE"
