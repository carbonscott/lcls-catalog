#!/bin/bash
#
# catalog-cron.sh - Manage lcls-catalog cron job
#
# Usage:
#   ./catalog-cron.sh status              Show cron and slurm job status
#   ./catalog-cron.sh enable              Add cron entry on sdfcron001
#   ./catalog-cron.sh disable             Remove cron entry from sdfcron001
#   ./catalog-cron.sh submit [OPTIONS]    Manually submit sbatch job
#   ./catalog-cron.sh test [OPTIONS]      Run locally without slurm (testing)
#
# Test options:
#   --dry-run         Scan only, don't write catalog
#   --hutches "..."   Process specific hutches only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Source env.sh if CATALOG_DATA_DIR not set
if [[ -z "${CATALOG_DATA_DIR:-}" ]]; then
    source "$PROJECT_DIR/env.sh"
fi

# Configuration (override via environment variables)
CRON_NODE="${CRON_NODE:-sdfcron001}"
CRON_SCHEDULE="${CRON_SCHEDULE:-0 1 * * *}"
CRON_LOG="${CRON_LOG:-$CATALOG_DATA_DIR/cron.log}"
CRON_MARKER="catalog-cron.sh"

usage() {
    cat <<EOF
Usage: $(basename "$0") COMMAND [OPTIONS]

Commands:
  status              Show cron status and running slurm jobs
  enable [OPTIONS]    Add cron entry on $CRON_NODE
  disable             Remove cron entry from $CRON_NODE
  submit [OPTIONS]    Submit sbatch job (pass OPTIONS to indexer)
  test [OPTIONS]      Run locally without slurm (for testing)

Enable options:
  --schedule "..."    Cron schedule (default: "$CRON_SCHEDULE")

Test/Submit options:
  --dry-run           Scan only, don't write catalog
  --hutches "..."     Process specific hutches only
  --workers N         Threads per experiment
  --parallel N        Max concurrent experiments

Environment variables:
  CRON_NODE           Cron node (default: sdfcron001)
  CRON_SCHEDULE       Cron schedule (default: 0 2 * * *)
  CRON_LOG            Log file path (default: \$CATALOG_DATA_DIR/cron.log)

Examples:
  # Check if cron is enabled and view running jobs
  $(basename "$0") status

  # Enable daily cron job (default: 2am)
  $(basename "$0") enable

  # Enable with custom schedule (3am daily)
  $(basename "$0") enable --schedule "0 3 * * *"

  # Disable cron job
  $(basename "$0") disable

  # Manually submit a Slurm job now
  $(basename "$0") submit

  # Submit job for specific hutches only
  $(basename "$0") submit --hutches "amo cxi"

  # Test locally with dry-run (no catalog writes)
  $(basename "$0") test --dry-run

  # Test locally with single hutch
  $(basename "$0") test --hutches "prj"
EOF
    exit 1
}

cmd_status() {
    echo "=== Cron Status (on $CRON_NODE) ==="
    if ssh "$CRON_NODE" "crontab -l 2>/dev/null" | grep -q "$CRON_MARKER"; then
        echo "Cron: ENABLED"
        ssh "$CRON_NODE" "crontab -l" | grep "$CRON_MARKER"
    else
        echo "Cron: DISABLED"
    fi

    echo ""
    echo "=== Running Slurm Jobs ==="
    squeue -u "$USER" -n catalog-index -o "%.10i %.20j %.10T %.10M %.6D %R" 2>/dev/null || echo "No jobs running"

    echo ""
    echo "=== Recent Log Entries ==="
    if [[ -f "$CRON_LOG" ]]; then
        tail -5 "$CRON_LOG"
    else
        echo "(no log file yet)"
    fi
}

cmd_enable() {
    local schedule="$CRON_SCHEDULE"

    # Parse enable-specific options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --schedule)
                schedule="$2"
                shift 2
                ;;
            *)
                echo "Unknown option for enable: $1" >&2
                exit 1
                ;;
        esac
    done

    local cron_entry="$schedule $SCRIPT_DIR/catalog-cron.sh submit >> $CRON_LOG 2>&1"

    echo "Enabling cron on $CRON_NODE..."
    echo "Schedule: $schedule"
    echo "Entry: $cron_entry"

    # Add entry if not already present
    ssh "$CRON_NODE" bash -c "'
        if crontab -l 2>/dev/null | grep -q \"$CRON_MARKER\"; then
            echo \"Cron entry already exists - removing old entry first\"
            crontab -l | grep -v \"$CRON_MARKER\" | crontab -
        fi
        (crontab -l 2>/dev/null; echo \"$cron_entry\") | crontab -
        echo \"Cron entry added\"
        echo \"Current crontab:\"
        crontab -l | grep \"$CRON_MARKER\" || true
    '"
}

cmd_disable() {
    echo "Disabling cron on $CRON_NODE..."

    ssh "$CRON_NODE" bash -c "'
        if crontab -l 2>/dev/null | grep -q \"$CRON_MARKER\"; then
            crontab -l | grep -v \"$CRON_MARKER\" | crontab -
            echo \"Cron entry removed\"
        else
            echo \"No cron entry found\"
        fi
    '"
}

cmd_submit() {
    echo "Submitting sbatch job..."

    # Source env to ensure UV_CACHE_DIR etc are set
    source "$PROJECT_DIR/env.sh"

    # Add slurm to PATH (cron has minimal PATH that doesn't include slurm)
    export PATH="/opt/slurm/slurm-curr/bin:$PATH"

    job_id=$(sbatch --parsable --output="$CATALOG_DATA_DIR/slurm_%j.log" "$SCRIPT_DIR/catalog_index.sbatch" "$@")
    echo "Submitted job: $job_id"
    echo "Monitor with: squeue -j $job_id"
    echo "Log file: ${CATALOG_DATA_DIR}/slurm_${job_id}.log"
}

cmd_test() {
    echo "Running local test (no slurm)..."
    echo "Using separate lock file to avoid interfering with cron jobs"

    # Source env
    source "$PROJECT_DIR/env.sh"

    # Run with test lock file
    "$SCRIPT_DIR/run_catalog_index.sh" --lock-file /tmp/catalog_index_test.lock "$@"
}

# --- Main ---
if [[ $# -lt 1 ]]; then
    usage
fi

COMMAND="$1"
shift

case "$COMMAND" in
    status)
        cmd_status
        ;;
    enable)
        cmd_enable "$@"
        ;;
    disable)
        cmd_disable
        ;;
    submit)
        cmd_submit "$@"
        ;;
    test)
        cmd_test "$@"
        ;;
    *)
        echo "Unknown command: $COMMAND" >&2
        usage
        ;;
esac
