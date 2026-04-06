#!/bin/bash
# test_03_coord_crash.sh


set -euo pipefail

echo "=== TEST 03: Coordinator Crash & Recovery ==="

AFS_NODE1="localhost:50051"
AFS_NODE2="localhost:50052"
AFS_NODE3="localhost:50053"
AFS_ADDRS="$AFS_NODE1,$AFS_NODE2,$AFS_NODE3"

DATA1="./afs_data/server1"
DATA2="./afs_data/server2"
DATA3="./afs_data/server3"

NUM_WORKERS=3
WORKER_ADDRS="localhost:6001,localhost:6002,localhost:6003"

echo "--- Compiling binary ---"
go build -o prime_app coordinator_worker/main.go

echo "--- Cleaning up previous state ---"
pkill -f "go run ./server" || true
pkill -f "prime_app"       || true

fuser -k 50051/tcp 50052/tcp 50053/tcp 2>/dev/null || true
fuser -k 6001/tcp 6002/tcp 6003/tcp 2>/dev/null || true

rm -f afs_data/server*/input/snapshot_latest.json
rm -f afs_data/server*/input/primes.txt
rm -rf /tmp/afs*

sleep 1


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

echo "--- Starting Workers ---"
for i in 1 2 3; do
    ./prime_app --mode=worker --id=$i --port=:$((6000 + i)) &
done
sleep 2

INPUT_FILES=$(ls "$DATA1/input/input_dataset_"*.txt 2>/dev/null \
    | xargs -n1 basename \
    | tr '\n' ',' \
    | sed 's/,$//')

if [ -z "$INPUT_FILES" ]; then
    echo "FAIL: No input_dataset_*.txt files found in $DATA1/input/"
    exit 1
fi

echo "--- Starting initial coordinator (will be killed) ---"
./prime_app --mode=coordinator \
    --afs="$AFS_ADDRS" \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS" &
COORD_PID=$!

echo "Letting the coordinator process for 0.5 seconds..."
sleep 0.5

echo "!!! KILLING COORDINATOR NOW !!!"
kill -9 $COORD_PID 2>/dev/null || true

echo "Waiting 2 seconds before recovery..."
sleep 2

echo "=== Restarting Coordinator (with --recover) ==="
./prime_app --mode=coordinator \
    --afs="$AFS_ADDRS" \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS" \
    --recover=true

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
    echo "SUCCESS: Found $PRIME_COUNT primes after full coordinator crash & recovery!"
else
    echo "FAIL: primes.txt was not found in any AFS node output directory!"
    exit 1
fi

trap 'pkill -f "go run ./server" || true; pkill -f "prime_app" || true; rm -f prime_app' EXIT