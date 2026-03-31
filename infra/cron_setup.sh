#!/usr/bin/env bash
# Install nightly cron jobs on the EMR master node.
# Run this script once after SSHing into the master node.
#
# Usage:
#   ssh hadoop@<master-dns> 'bash -s' < infra/cron_setup.sh
#
# What it schedules (runs at 2am nightly):
#   1. Submit clean job to existing cluster, wait for completion
#   2. Submit aggregate job to existing cluster, wait for completion
#   3. Sync processed Parquet back to verify output

set -euo pipefail

SCRIPT_DIR="/home/hadoop"
LOG_DIR="/home/hadoop/logs"
PROJECT_DIR="/home/hadoop/CityBite"
ENV_FILE="$PROJECT_DIR/.env"

mkdir -p "$LOG_DIR"

# Install python-dotenv if not present (needed by submit_emr.py)
pip3 install --quiet python-dotenv 2>/dev/null || true

# Source cluster ID from .env so cron picks it up without a shell profile
CLUSTER_ID=$(grep EMR_CLUSTER_ID "$ENV_FILE" 2>/dev/null | cut -d= -f2 | tr -d ' ')
if [ -z "$CLUSTER_ID" ]; then
  echo "ERROR: EMR_CLUSTER_ID not found in $ENV_FILE" >&2
  exit 1
fi

# Build the nightly pipeline command:
#   - clean: raw JSON → enriched Parquet (partitioned by city)
#   - aggregate: enriched → business_scores + grid_aggregates Parquet + RDS via seed_rds.py
PIPELINE_CMD="cd $PROJECT_DIR && \
  python pipeline/submit_emr.py --job clean --cluster-id $CLUSTER_ID --wait && \
  python pipeline/submit_emr.py --job aggregate --cluster-id $CLUSTER_ID --wait && \
  python ml/seed_rds.py --input data/processed/"

CRON_JOB="0 2 * * * $PIPELINE_CMD >> $LOG_DIR/pipeline_\$(date +\%Y\%m\%d).log 2>&1"

# Add job only if not already present
if crontab -l 2>/dev/null | grep -qF "submit_emr.py"; then
  echo "Cron job already exists — skipping."
else
  (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
  echo "Cron job installed."
fi

echo ""
echo "Current crontab:"
crontab -l

echo ""
echo "Log output will be written to: $LOG_DIR/pipeline_<YYYYMMDD>.log"
