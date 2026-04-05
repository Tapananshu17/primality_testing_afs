#!/bin/bash
echo "=== TEST 05: Throughput Scaling Benchmark ==="

echo "Compiling binary..."
go build -o prime_app coordinator_worker/main.go

cd afs_data/server1/input
INPUT_FILES=$(ls input_dataset_*.txt | tr '\n' ',' | sed 's/,$//')
cd ../../../

WORKER_COUNTS=(1 2 4 8)
RESULTS=()

for COUNT in "${WORKER_COUNTS[@]}"; do
    echo "---------------------------------------------------"
    echo "Running benchmark with $COUNT worker(s)..."
    
    # Clean state for a fresh benchmark
    pkill -f "mode=worker" || true
    pkill -f "prime_app" || true
    rm -f afs_data/server1/output/primes.txt
    rm -f afs_data/server1/output/snapshot_latest.json
    rm -rf /tmp/prime_cache/*
    
    BASE_PORT=6000
    WORKER_ADDRS=""
    for i in $(seq 1 $COUNT); do
        PORT=$((BASE_PORT + i))
        ./prime_app --mode=worker --id=$i --port=:$PORT > /dev/null 2>&1 &
        if [ "$WORKER_ADDRS" == "" ]; then
            WORKER_ADDRS="localhost:$PORT"
        else
            WORKER_ADDRS="$WORKER_ADDRS,localhost:$PORT"
        fi
    done
    sleep 2 

    # Run the coordinator in FOREGROUND and capture output to a file
    ./prime_app --mode=coordinator \
        --inputs="$INPUT_FILES" \
        --output="primes.txt" \
        --workers="$WORKER_ADDRS" > bench_output.txt 2>&1
        
    # Extract the "Time taken" using grep and awk
    TIME_TAKEN=$(grep "Time taken" bench_output.txt | awk -F': ' '{print $2}')
    
    if [ -z "$TIME_TAKEN" ]; then
        TIME_TAKEN="FAILED"
    fi
    
    echo "Completed in: $TIME_TAKEN"
    RESULTS+=("$COUNT Workers | $TIME_TAKEN")
done

# Print the final report table
echo ""
echo "====================================="
echo "      FINAL SCALING BENCHMARK        "
echo "====================================="
for res in "${RESULTS[@]}"; do
    echo "$res"
done
echo "====================================="

rm -f bench_output.txt
trap "pkill -f 'prime_app'; rm -f prime_app" EXIT