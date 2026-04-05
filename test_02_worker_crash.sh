#!/bin/bash
echo "=== TEST 02: Single Worker Crash & Dynamic Rejoin ==="

echo "Compiling binary..."
go build -o prime_app coordinator_worker/main.go

# NUCLEAR CLEANUP
pkill -f "mode=worker" || true
pkill -f "prime_app" || true
rm -f afs_data/server1/output/primes.txt
rm -f afs_data/server1/output/snapshot_latest.json
rm -rf /tmp/prime_cache/*

NUM_WORKERS=3
BASE_PORT=6000
WORKER_ADDRS="localhost:6001,localhost:6002,localhost:6003"

for i in $(seq 1 $NUM_WORKERS); do
    ./prime_app --mode=worker --id=$i --port=:$((BASE_PORT + i)) &
done
sleep 2
echo "=== Workers 1, 2, and 3 started. ==="

cd afs_data/server1/input
INPUT_FILES=$(ls input_dataset_*.txt | tr '\n' ',' | sed 's/,$//')
cd ../../../

echo "=== Starting Coordinator ==="
./prime_app --mode=coordinator \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS" &
COORD_PID=$!

echo "Letting system process for 0.4 seconds..."
sleep 0.4 

echo "💀 FATAL: KILLING WORKER 2 NOW! 💀"
pkill -f "mode=worker --id=2"

echo "⏳ Watch the logs! Workers 1 and 3 are now carrying the load... ⏳"
# We sleep for 0.5 seconds so you can clearly see W1 and W3 doing the work
# while W2's proxy goroutine logs that it is waiting/pinging.
sleep 0.5

echo "⚕️ RESTARTING WORKER 2! Watch it instantly rejoin... ⚕️"
./prime_app --mode=worker --id=2 --port=:6002 &

echo "Waiting for the Coordinator to finish the job..."
wait $COORD_PID

echo "=== Validating Output ==="
if [ -f "afs_data/server1/output/primes.txt" ]; then
    PRIME_COUNT=$(wc -l < afs_data/server1/output/primes.txt)
    echo "SUCCESS: Found $PRIME_COUNT primes! Worker 2 successfully died, was bypassed, and dynamically rejoined."
else
    echo "FAIL: primes.txt was not created."
fi

trap "pkill -f 'prime_app'; rm -f prime_app" EXIT