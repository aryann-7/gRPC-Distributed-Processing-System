#!/usr/bin/env bash
# =========================================================================
# CS324 Add Dynamic Worker Script (Linux/macOS)
# =========================================================================
# Usage:
#   ./add_worker.sh <workerId> [port] [bootstrapHost] [bootstrapPort]
# Examples:
#   ./add_worker.sh 104
#   ./add_worker.sh 104 1104
#   ./add_worker.sh 105 1105 localhost 1099
# =========================================================================

set -e

W_ID=$1
if [ -z "$W_ID" ]; then
    echo "Usage: ./add_worker.sh <workerId> [port] [bootstrapHost] [bootstrapPort]"
    echo "Example: ./add_worker.sh 104 1104"
    exit 1
fi

W_PORT=$2
if [ -z "$W_PORT" ]; then
    if [ "$W_ID" -ge 100 ]; then
        W_PORT=$((1000 + W_ID))
    else
        W_PORT=$((1100 + W_ID))
    fi
fi

BS_HOST=${3:-localhost}
BS_PORT=${4:-1099}

echo "========================================================================="
echo "Dynamically joining Worker-$W_ID to cluster..."
echo "Host: localhost, Port: $W_PORT, Bootstrap: $BS_HOST:$BS_PORT"
echo "========================================================================="

java -cp bin worker.WorkerMain "$W_ID" localhost "$W_PORT" "$BS_HOST" "$BS_PORT" &

echo "Worker-$W_ID started in background (PID $!)."
