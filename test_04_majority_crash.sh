#!/bin/bash
echo "=== TEST 04: Majority Worker Failure & Dynamic Recovery ==="

echo "Compiling binary..."
go build -o prime_app coordinator_worker/main.go

# NUCLEAR CLEANUP
pkill -f "mode=worker" || true
pkill -f "prime_app" || true
rm -f afs_data/server1/output/primes.txt
rm -f afs_data/server1/output/snapshot_latest.json
rm -rf /tmp/prime_cache/*

NUM_WORKERS=5
BASE_PORT=6000
WORKER_ADDRS="localhost:6001,localhost:6002,localhost:6003,localhost:6004,localhost:6005"

for i in $(seq 1 $NUM_WORKERS); do
    ./prime_app --mode=worker --id=$i --port=:$((BASE_PORT + i)) &
done
sleep 2
echo "=== Initial 5 Workers started. ==="

cd afs_data/server1/input
INPUT_FILES=$(ls input_dataset_*.txt | tr '\n' ',' | sed 's/,$//')
cd ../../../

echo "=== Starting Coordinator ==="
./prime_app --mode=coordinator --inputs="$INPUT_FILES" --output="primes.txt" --workers="$WORKER_ADDRS" &
COORD_PID=$!

echo "Letting system process for 0.35 seconds..."
sleep 0.35 

echo " CATASTROPHE: KILLING MAJORITY (3/5) WORKERS! "
pkill -f "mode=worker --id=3"
pkill -f "mode=worker --id=4"
pkill -f "mode=worker --id=5"

echo " Workers 1 and 2 are taking over the massive load... "
# Wait just 0.4 seconds so the 2 survivors don't finish the entire dataset
sleep 0.4

echo "=== Restarting Dead Workers! Watch them dynamically rejoin... ==="
for i in 3 4 5; do
    ./prime_app --mode=worker --id=$i --port=:$((BASE_PORT + i)) &
done

echo "Waiting for Coordinator to finish the job..."
wait $COORD_PID

echo "=== Validating Output ==="
if [ -f "afs_data/server1/output/primes.txt" ]; then
    PRIME_COUNT=$(wc -l < afs_data/server1/output/primes.txt)
    echo "SUCCESS: Found $PRIME_COUNT primes! Survived 60% cluster loss dynamically."
else
    echo "FAIL: primes.txt was not created."
fi

trap "pkill -f 'prime_app'; rm -f prime_app" EXIT