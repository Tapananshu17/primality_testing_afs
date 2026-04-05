#!/bin/bash

# Configuration
NUM_WORKERS=1
BASE_PORT=6000
WORKER_ADDRS=""

echo "=== Starting $NUM_WORKERS Workers ==="

for i in $(seq 1 $NUM_WORKERS); do
    PORT=$((BASE_PORT + i))
    
    # Start the worker in the background (&)
    go run coordinator_worker/main.go --mode=worker --id=$i --port=:$PORT &
    
    # Append to our comma-separated list for the coordinator
    if [ "$WORKER_ADDRS" == "" ]; then
        WORKER_ADDRS="localhost:$PORT"
    else
        WORKER_ADDRS="$WORKER_ADDRS,localhost:$PORT"
    fi
done

# Give the workers a second to initialize their gRPC servers
sleep 2

echo "=== Workers started on: $WORKER_ADDRS ==="
echo "=== Starting Coordinator ==="

# 1. CD into the directory where the files actually live
cd afs_data/server1/input

# 2. Get all matching filenames and join them with commas (e.g., "file1.txt,file2.txt")
INPUT_FILES=$(ls input_dataset_*.txt | tr '\n' ',' | sed 's/,$//')

# 3. CD back to the root directory
cd ../../../

echo "Found files: $INPUT_FILES"

# 4. Start the coordinator with the dynamically generated list!
# (Note: Since the script is in the root, we must point to main.go inside coordinator_worker)
go run coordinator_worker/main.go --mode=coordinator \
    --inputs="$INPUT_FILES" \
    --output="primes.txt" \
    --workers="$WORKER_ADDRS"

# --- CLEANUP MAGIC ---
trap "echo 'Cleaning up cluster...'; pkill -P $$" EXIT