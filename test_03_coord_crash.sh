#!/bin/bash
echo "=== TEST 03: Coordinator Crash & Recovery ==="

# 1. BUILD BINARY & NUCLEAR CLEANUP
echo "Compiling binary..."
go build -o prime_app coordinator_worker/main.go

pkill -f "mode=worker" || true
pkill -f "prime_app" || true
rm -f afs_data/server1/output/primes.txt
rm -f afs_data/server1/output/snapshot_latest.json
rm -rf /tmp/prime_cache/* 
NUM_WORKERS=3
BASE_PORT=6000
WORKER_ADDRS=""

for i in $(seq 1 $NUM_WORKERS); do
    PORT=$((BASE_PORT + i))
    ./prime_app --mode=worker --id=$i --port=:$PORT &
    if [ "$WORKER_ADDRS" == "" ]; then
        WORKER_ADDRS="localhost:$PORT"
    else
        WORKER_ADDRS="$WORKER_ADDRS,localhost:$PORT"
    fi
done

sleep 2
echo "=== Workers started. ==="

cd afs_data/server1/input
INPUT_FILES=$(ls input_dataset_*.txt | tr '\n' ',' | sed 's/,$//')
cd ../../../

# 3. RUN INITIAL COORDINATOR IN BACKGROUND
echo "=== Starting Initial Coordinator ==="
./prime_app --mode=coordinator \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS" &   # <--- THIS AMPERSAND IS CRITICAL!
COORD_PID=$!

# 4. INTRODUCE COORDINATOR CHAOS
echo "Letting the system process and snapshot for 0.8 seconds..."
sleep 0.8

echo "  !!! KILLING COORDINATOR NOW !!! "
kill -9 $COORD_PID

# 5. INITIATE RECOVERY
echo "Waiting 2 seconds before recovery..."
sleep 2

echo "=== Restarting Coordinator in RECOVERY MODE ==="
./prime_app --mode=coordinator \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS" \
    --recover=true

# 6. ASSERTION / VALIDATION
echo "=== Validating Output ==="
if [ -f "afs_data/server1/output/primes.txt" ]; then
    PRIME_COUNT=$(wc -l < afs_data/server1/output/primes.txt)
    echo "SUCCESS: Found $PRIME_COUNT primes after a full coordinator wipe & recovery!"
else
    echo "FAIL: primes.txt was not created."
    exit 1
fi

trap "pkill -f 'prime_app'; rm -f prime_app" EXIT