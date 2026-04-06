#!/bin/bash
# run_cluster.sh
#
# Starts a 3-node Raft AFS cluster, then runs the coordinator + workers.
#
# Layout assumed:
#   server/                  — AFS + Raft server (go run ./server ...)
#   coordinator_worker/main.go — coordinator / worker binary
#   afs_data/server{1,2,3}/input/  — input datasets (pre-seeded)
#
# Usage:
#   ./run_cluster.sh

set -euo pipefail

# ── 1. Pre-Flight Cleanup ─────────────────────────────────────────────────────
echo "=== Cleaning up previous state ==="
pkill -f "go run ./server" || true
pkill -f "prime_app"       || true

# Aggressively free ports
fuser -k 50051/tcp 50052/tcp 50053/tcp 2>/dev/null || true
for p in $(seq 6001 6010); do fuser -k $p/tcp 2>/dev/null || true; done

# Destroy phantom files and caches
rm -f afs_data/server*/input/snapshot_latest.json
rm -f afs_data/server*/input/primes.txt
rm -rf /tmp/afs*
sleep 1

# ── 2. Build Binary ───────────────────────────────────────────────────────────
echo "=== Compiling binary ==="
go build -o prime_app coordinator_worker/main.go

# ── Config ────────────────────────────────────────────────────────────────────
NUM_WORKERS=3
BASE_WORKER_PORT=6000
WORKER_ADDRS=""

AFS_NODE1="localhost:50051"
AFS_NODE2="localhost:50052"
AFS_NODE3="localhost:50053"
AFS_ADDRS="$AFS_NODE1,$AFS_NODE2,$AFS_NODE3"

DATA1="./afs_data/server1"
DATA2="./afs_data/server2"
DATA3="./afs_data/server3"

# ── 3. Start 3-node Raft AFS cluster ─────────────────────────────────────────
echo "=== Starting 3-node Raft AFS cluster ==="

go run ./server \
    --self="$AFS_NODE1" --peers="$AFS_NODE2,$AFS_NODE3" \
    --data="$DATA1" --port=50051 --clean &

go run ./server \
    --self="$AFS_NODE2" --peers="$AFS_NODE1,$AFS_NODE3" \
    --data="$DATA2" --port=50052 --clean &

go run ./server \
    --self="$AFS_NODE3" --peers="$AFS_NODE1,$AFS_NODE2" \
    --data="$DATA3" --port=50053 --clean &

# Allow the cluster to boot and elect a Raft leader before the coordinator connects.
echo "Waiting for Raft leader election (5s)..."
sleep 5

# ── 4. Start prime-checking workers ──────────────────────────────────────────
echo "=== Starting $NUM_WORKERS Worker(s) ==="

for i in $(seq 1 $NUM_WORKERS); do
    PORT=$((BASE_WORKER_PORT + i))
    ./prime_app --mode=worker --id=$i --port=:$PORT &

    if [ -z "$WORKER_ADDRS" ]; then
        WORKER_ADDRS="localhost:$PORT"
    else
        WORKER_ADDRS="$WORKER_ADDRS,localhost:$PORT"
    fi
done

sleep 2
echo "=== Workers started on: $WORKER_ADDRS ==="

# ── 5. Discover input files from the seeded input directory ──────────────────
INPUT_FILES=$(ls "$DATA1/input/input_dataset_"*.txt 2>/dev/null \
    | xargs -n1 basename \
    | tr '\n' ',' \
    | sed 's/,$//')

if [ -z "$INPUT_FILES" ]; then
    echo "ERROR: No input_dataset_*.txt files found in $DATA1/input/"
    exit 1
fi

echo "Found input files: $INPUT_FILES"

# ── 6. Run coordinator ────────────────────────────────────────────────────────
echo "=== Starting Coordinator ==="

./prime_app --mode=coordinator \
    --afs="$AFS_ADDRS" \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS"

# ── 7. Cleanup on exit ────────────────────────────────────────────────────────
trap 'echo "Cleaning up..."; pkill -f "go run ./server" || true; pkill -f "prime_app" || true; rm -f prime_app' EXIT