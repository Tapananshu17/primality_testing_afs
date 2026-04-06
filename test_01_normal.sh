#!/bin/bash
# test_01_normal.sh
#
# TEST 01: Happy Path
#
# Verifies that the full system works end-to-end:
#   • 3-node Raft AFS cluster boots and elects a leader
#   • 3 prime-checking workers process all input chunks
#   • Coordinator writes primes.txt to AFS via the Raft leader
#   • primes.txt is readable back from AFS

set -euo pipefail

echo "=== TEST 01: The Happy Path ==="

# ── Config ────────────────────────────────────────────────────────────────────
AFS_NODE1="localhost:50051"
AFS_NODE2="localhost:50052"
AFS_NODE3="localhost:50053"
AFS_ADDRS="$AFS_NODE1,$AFS_NODE2,$AFS_NODE3"

DATA1="afs_data/server1"
DATA2="afs_data/server2"
DATA3="afs_data/server3"

NUM_WORKERS=3
BASE_WORKER_PORT=6000
WORKER_ADDRS=""

# ── 1. Nuclear cleanup ────────────────────────────────────────────────────────
echo "--- Cleaning up previous state ---"
# Kill the go run commands
pkill -f "go run ./server"      || true
pkill -f "mode=worker"          || true
pkill -f "mode=coordinator"     || true

# Aggressively kill the compiled child processes holding the ports
fuser -k 50051/tcp 50052/tcp 50053/tcp 2>/dev/null || true
fuser -k 6001/tcp 6002/tcp 6003/tcp 2>/dev/null || true
sleep 1
# ── 2. Start Raft AFS cluster with --clean to wipe stale Raft state ──────────
# --clean removes raft_log.json, raft_state.json, and the output/ directory on
# each node so a prior test run's committed term cannot skew this election.
echo "--- Starting 3-node Raft AFS cluster (clean) ---"

go run ./server \
    --self="$AFS_NODE1" --peers="$AFS_NODE2,$AFS_NODE3" \
    --data="$DATA1" --port=50051 --clean &

go run ./server \
    --self="$AFS_NODE2" --peers="$AFS_NODE1,$AFS_NODE3" \
    --data="$DATA2" --port=50052 --clean &

go run ./server \
    --self="$AFS_NODE3" --peers="$AFS_NODE1,$AFS_NODE2" \
    --data="$DATA3" --port=50053 --clean &

echo "Waiting for Raft leader election (5s)..."
sleep 5

# ── 3. Start workers ──────────────────────────────────────────────────────────
echo "--- Starting $NUM_WORKERS workers ---"

for i in $(seq 1 $NUM_WORKERS); do
    PORT=$((BASE_WORKER_PORT + i))
    go run coordinator_worker/main.go --mode=worker --id=$i --port=:$PORT &

    if [ -z "$WORKER_ADDRS" ]; then
        WORKER_ADDRS="localhost:$PORT"
    else
        WORKER_ADDRS="$WORKER_ADDRS,localhost:$PORT"
    fi
done

sleep 2
echo "Workers started on: $WORKER_ADDRS"

# ── 4. Resolve input files ────────────────────────────────────────────────────
INPUT_FILES=$(ls "$DATA1/input/input_dataset_"*.txt 2>/dev/null \
    | xargs -n1 basename \
    | tr '\n' ',' \
    | sed 's/,$//')

if [ -z "$INPUT_FILES" ]; then
    echo "FAIL: No input_dataset_*.txt files found in $DATA1/input/"
    exit 1
fi

echo "Input files: $INPUT_FILES"

# ── 5. Run coordinator (foreground — blocks until done) ───────────────────────
echo "--- Running coordinator ---"
go run coordinator_worker/main.go --mode=coordinator \
    --afs="$AFS_ADDRS" \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS"

# ── 6. Validate ───────────────────────────────────────────────────────────────
# In Raft, once the coordinator's StoreFile is committed, the entry is applied
# to every node.  We check the leader's data directory (server1 is most likely
# to be leader after a clean boot, but any node's output dir will do once Raft
# has applied the entry).
echo "=== Validating Output ==="

PRIME_FILE=""
for DATA_DIR in "$DATA1" "$DATA2" "$DATA3"; do
    if [ -f "$DATA_DIR/output/primes.txt" ]; then
        PRIME_FILE="$DATA_DIR/output/primes.txt"
        break
    fi
done

if [ -n "$PRIME_FILE" ]; then
    PRIME_COUNT=$(wc -l < "$PRIME_FILE")
    echo "SUCCESS: Found $PRIME_COUNT primes in $PRIME_FILE"
else
    echo "FAIL: primes.txt was not found in any AFS node output directory!"
    exit 1
fi

# ── 7. Cleanup ────────────────────────────────────────────────────────────────
trap 'pkill -f "go run ./server" || true; pkill -f "mode=worker" || true' EXIT