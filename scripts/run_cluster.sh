#!/usr/bin/env bash
# =========================================================================
# CS324 Distributed Computing Cluster Build and Run Script (Linux/macOS)
# =========================================================================
# Usage:
#   ./run_cluster.sh [num_workers] [num_clients]
# Examples:
#   ./run_cluster.sh         (defaults to 3 workers, 2 clients)
#   ./run_cluster.sh 5       (5 workers, 2 clients)
#   ./run_cluster.sh 6 3     (6 workers, 3 clients)
# =========================================================================

set -e

NUM_WORKERS=${1:-3}
NUM_CLIENTS=${2:-2}

# Validate worker count (range 3 to 10)
if [ "$NUM_WORKERS" -lt 3 ]; then
    echo "[WARNING] Minimum recommended workers is 3. Adjusting to 3."
    NUM_WORKERS=3
fi
if [ "$NUM_WORKERS" -gt 10 ]; then
    echo "[WARNING] Maximum supported in demo range is 10. Adjusting to 10."
    NUM_WORKERS=10
fi

echo "========================================================================="
echo "Cluster Configuration: $NUM_WORKERS Workers (IDs 101-$((100 + NUM_WORKERS))), $NUM_CLIENTS Clients"
echo "========================================================================="

echo "[1/6] Cleaning previous build artifacts..."
rm -rf bin
mkdir -p bin

echo "[2/6] Compiling Java source files..."
javac -d bin $(find src -name "*.java")
echo "Compilation successful."

echo "[3/6] Starting RMI Registry on port 1099..."
rmiregistry -J-cp -Jbin 1099 &
RMI_PID=$!
sleep 2

echo "[4/6] Launching Bootstrap Server..."
java -cp bin bootstrap.BootstrapServer &
BOOTSTRAP_PID=$!
sleep 2

echo "[5/6] Launching $NUM_WORKERS Worker Nodes..."
WORKER_PIDS=()
for ((i=1; i<=NUM_WORKERS; i++)); do
    W_ID=$((100 + i))
    W_PORT=$((1100 + i))
    echo "Starting Worker-$W_ID on port $W_PORT..."
    java -cp bin worker.WorkerMain $W_ID localhost $W_PORT localhost 1099 &
    WORKER_PIDS+=($!)
    sleep 1
done

sleep 2

echo "[6/6] Launching $NUM_CLIENTS Client GUI instances..."
CLIENT_PIDS=()
for ((c=1; c<=NUM_CLIENTS; c++)); do
    echo "Starting Client GUI #$c..."
    java -cp bin client.ClientMain $c &
    CLIENT_PIDS+=($!)
    sleep 1
done

echo "========================================================================="
echo "Cluster launched. Press Ctrl+C to terminate all processes."
echo "========================================================================="

trap "kill $RMI_PID $BOOTSTRAP_PID ${WORKER_PIDS[@]} ${CLIENT_PIDS[@]} 2>/dev/null || true" EXIT
wait
