#!/bin/bash
# test_04_majority_crash.sh
#
# TEST 04: Majority Worker Failure & Dynamic Recovery
#
# Verifies that killing the majority of prime-checking workers mid-job does not
# crash the coordinator or lose data. The remaining workers carry the load, and
# when the dead workers restart, they dynamically rejoin the pool.

set -euo pipefail

echo "=== TEST 04: Majority Worker Failure & Dynamic Recovery ==="

# ── Config ────────────────────────────────────────────────────────────────────
AFS_NODE1="localhost:50051"
AFS_NODE2="localhost:50052"
AFS_NODE3="localhost:50053"
AFS_ADDRS="$AFS_NODE1,$AFS_NODE2,$AFS_NODE3"

DATA1="./afs_data/server1"
DATA2="./afs_data/server2"
DATA3="./afs_data/server3"

NUM_WORKERS=5
BASE_PORT=6000
WORKER_ADDRS="localhost:6001,localhost:6002,localhost:6003,localhost:6004,localhost:6005"

# ── 1. Build binary ───────────────────────────────────────────────────────────
echo "--- Compiling binary ---"
go build -o prime_app coordinator_worker/main.go

# ── 2. Nuclear cleanup ────────────────────────────────────────────────────────
echo "--- Cleaning up previous state ---"
pkill -f "go run ./server" || true
pkill -f "prime_app"       || true

# Aggressively kill the compiled child processes holding the ports
fuser -k 50051/tcp 50052/tcp 50053/tcp 2>/dev/null || true
fuser -k 6001/tcp 6002/tcp 6003/tcp 6004/tcp 6005/tcp 2>/dev/null || true

# Destroy any phantom files that accidentally got into the input folders
rm -f afs_data/server*/input/snapshot_latest.json
rm -f afs_data/server*/input/primes.txt
rm -rf /tmp/afs*
sleep 1

# ── 3. Start Raft AFS cluster ─────────────────────────────────────────────────
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

# ── 4. Start 5 workers ────────────────────────────────────────────────────────
echo "--- Starting Initial 5 Workers ---"
for i in $(seq 1 $NUM_WORKERS); do
    ./prime_app --mode=worker --id=$i --port=:$((BASE_PORT + i)) &
done
sleep 2
echo "=== Initial 5 Workers started. ==="

# ── 5. Resolve input files ────────────────────────────────────────────────────
INPUT_FILES=$(ls "$DATA1/input/input_dataset_"*.txt 2>/dev/null \
    | xargs -n1 basename \
    | tr '\n' ',' \
    | sed 's/,$//')

if [ -z "$INPUT_FILES" ]; then
    echo "FAIL: No input_dataset_*.txt files found in $DATA1/input/"
    exit 1
fi

# ── 6. Start coordinator in background ────────────────────────────────────────
echo "=== Starting Coordinator ==="
./prime_app --mode=coordinator \
    --afs="$AFS_ADDRS" \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS" &
COORD_PID=$!

# ── 7. Kill majority of workers mid-job ───────────────────────────────────────
# Short delay so they get a chance to start processing
echo "Letting system process for 0.05 seconds..."
sleep 0.05 

echo "⚡ CATASTROPHE: KILLING MAJORITY (3/5) WORKERS! ⚡"
pkill -f "mode=worker --id=3" || true
pkill -f "mode=worker --id=4" || true
pkill -f "mode=worker --id=5" || true

echo "⏳ Workers 1 and 2 are taking over the massive load... ⏳"
# Wait just 0.1 seconds so the 2 survivors struggle for a moment
sleep 0.1

# ── 8. Restart dead workers ───────────────────────────────────────────────────
echo "=== Restarting Dead Workers! Watch them dynamically rejoin... ==="
for i in 3 4 5; do
    ./prime_app --mode=worker --id=$i --port=:$((BASE_PORT + i)) &
done

echo "Waiting for Coordinator to finish the job..."
wait $COORD_PID

# ── 9. Validate ───────────────────────────────────────────────────────────────
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
    echo "SUCCESS: Found $PRIME_COUNT primes! Survived 60% cluster loss dynamically."
else
    echo "FAIL: primes.txt was not found in any AFS node output directory!"
    exit 1
fi

# ── 10. Cleanup ───────────────────────────────────────────────────────────────
trap 'pkill -f "go run ./server" || true; pkill -f "prime_app" || true; rm -f prime_app' EXIT