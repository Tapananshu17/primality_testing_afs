#!/bin/bash
pkill -f "mode=worker" || true
echo "=== TEST 01: The Happy Path ==="

# 1. ENFORCE CLEAN STATE
echo "Cleaning up previous state..."
rm -f afs_data/server1/output/primes.txt
rm -f afs_data/server1/output/snapshot_latest.json

NUM_WORKERS=3  # Use at least 3 to prove distributed logic works
BASE_PORT=6000
WORKER_ADDRS=""

for i in $(seq 1 $NUM_WORKERS); do
    PORT=$((BASE_PORT + i))
    go run coordinator_worker/main.go --mode=worker --id=$i --port=:$PORT &
    
    if [ "$WORKER_ADDRS" == "" ]; then
        WORKER_ADDRS="localhost:$PORT"
    else
        WORKER_ADDRS="$WORKER_ADDRS,localhost:$PORT"
    fi
done

sleep 2
echo "=== Workers started on: $WORKER_ADDRS ==="

# 2. RUN COORDINATOR (Foreground)
cd afs_data/server1/input
INPUT_FILES=$(ls input_dataset_*.txt | tr '\n' ',' | sed 's/,$//')
cd ../../../

go run coordinator_worker/main.go --mode=coordinator \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS"

# 3. ASSERTION / VALIDATION
echo "=== Validating Output ==="
if [ -f "afs_data/server1/output/primes.txt" ]; then
    PRIME_COUNT=$(wc -l < afs_data/server1/output/primes.txt)
    echo "SUCCESS: Found $PRIME_COUNT primes!"
    # Optional: If you know the dataset has exactly 10,000 primes, add an IF statement here to fail the test if the count is wrong.
else
    echo "FAIL: primes.txt was not created!"
    exit 1
fi

trap "pkill -f 'mode=worker'; pkill -f 'mode=coordinator'" EXIT