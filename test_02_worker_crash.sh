#!/bin/bash
pkill -f "mode=worker" || true
echo "=== TEST 02: Worker Crash Tolerance ==="

rm -f afs_data/server1/output/primes.txt
rm -f afs_data/server1/output/snapshot_latest.json

NUM_WORKERS=3
BASE_PORT=6000
WORKER_ADDRS=""

# We need an array to store the Process IDs (PIDs) of our workers so we can kill one later
declare -a WORKER_PIDS

for i in $(seq 1 $NUM_WORKERS); do
    PORT=$((BASE_PORT + i))
    go run coordinator_worker/main.go --mode=worker --id=$i --port=:$PORT &
    WORKER_PIDS[$i]=$!  # Capture the PID of the background process
    
    if [ "$WORKER_ADDRS" == "" ]; then
        WORKER_ADDRS="localhost:$PORT"
    else
        WORKER_ADDRS="$WORKER_ADDRS,localhost:$PORT"
    fi
done

sleep 2
echo "=== Workers started. Target for termination: Worker 2 (PID: ${WORKER_PIDS[2]}) ==="

cd afs_data/server1/input
INPUT_FILES=$(ls input_dataset_*.txt | tr '\n' ',' | sed 's/,$//')
cd ../../../

# 1. RUN COORDINATOR IN BACKGROUND
# We put it in the background (&) so the bash script can continue executing and trigger the chaos
go run coordinator_worker/main.go --mode=coordinator \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS" &
COORD_PID=$!

# 2. INTRODUCE CHAOS
echo "Letting the system compile and process for 1 seconds..."
sleep 1 

echo "💀 KILLING WORKER 2 NOW! 💀"
# Use pkill to hunt down the actual running binary, ignoring the go run wrapper
pkill -f "mode=worker --id=2"

# 3. WAIT FOR COMPLETION
# The bash script pauses here until the coordinator finishes its job
echo "Waiting for Coordinator to finish with remaining workers..."
wait $COORD_PID

# 4. ASSERTION / VALIDATION
echo "=== Validating Output ==="
if [ -f "afs_data/server1/output/primes.txt" ]; then
    PRIME_COUNT=$(wc -l < afs_data/server1/output/primes.txt)
    echo "SUCCESS: Found $PRIME_COUNT primes despite losing a worker!"
else
    echo "FAIL: primes.txt was not created. The cluster crashed."
    exit 1
fi

trap "pkill -f 'mode=worker'; pkill -f 'mode=coordinator'" EXIT