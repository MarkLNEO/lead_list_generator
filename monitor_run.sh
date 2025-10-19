#!/bin/bash
# Monitor active pipeline run

RUN_DIR=${1:-$(ls -t runs/ | head -1)}
RUN_PATH="runs/$RUN_DIR"

if [ ! -d "$RUN_PATH" ]; then
    echo "Run directory not found: $RUN_PATH"
    exit 1
fi

echo "========================================="
echo "Monitoring Run: $RUN_DIR"
echo "========================================="
echo ""

# Function to show colored output
green() { echo -e "\033[0;32m$1\033[0m"; }
yellow() { echo -e "\033[1;33m$1\033[0m"; }
red() { echo -e "\033[0;31m$1\033[0m"; }

# Check if still running
PID=$(ps aux | grep "[l]ead_pipeline.py" | awk '{print $2}' | head -1)
if [ -n "$PID" ]; then
    green "✓ Pipeline running (PID: $PID)"
else
    yellow "⚠ Pipeline not running (completed or failed)"
fi
echo ""

# Show last 10 log lines
echo "--- Recent Log Output ---"
tail -10 "$RUN_PATH/run.log" 2>/dev/null || echo "No log yet"
echo ""

# Show metrics if available
if [ -f "$RUN_PATH/metrics.json" ]; then
    echo "--- Metrics ---"
    jq -r '
        "Companies: \(.metrics.companies_discovered) discovered, \(.metrics.companies_enriched) enriched, \(.metrics.companies_rejected) rejected",
        "Contacts: \(.metrics.contacts_discovered) discovered, \(.metrics.contacts_verified) verified, \(.metrics.contacts_enriched) enriched",
        "Duration: \((.metrics.end_time - .metrics.start_time) / 60 | floor)m \((.metrics.end_time - .metrics.start_time) % 60 | floor)s"
    ' "$RUN_PATH/metrics.json" 2>/dev/null || echo "Metrics not available yet"
    echo ""
fi

# Show errors
ERROR_COUNT=$(grep -c "ERROR" "$RUN_PATH/run.log" 2>/dev/null || echo "0")
if [ "$ERROR_COUNT" -gt 0 ]; then
    red "⚠ $ERROR_COUNT errors found"
    echo "Recent errors:"
    grep "ERROR" "$RUN_PATH/run.log" | tail -3
    echo ""
fi

# Show phase
CURRENT_PHASE=$(grep "===" "$RUN_PATH/run.log" | tail -1 | sed 's/.*===//;s/===.*//' | xargs)
if [ -n "$CURRENT_PHASE" ]; then
    yellow "Current Phase: $CURRENT_PHASE"
else
    yellow "Phase: Initializing..."
fi

echo ""
echo "Monitor continuously: watch -n 10 ./monitor_run.sh $RUN_DIR"
echo "View live log: tail -f $RUN_PATH/run.log"
