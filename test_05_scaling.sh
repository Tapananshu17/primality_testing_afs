#!/bin/bash
# test_05_scaling.sh


set -euo pipefail

echo "=== TEST 05: Throughput Scaling Benchmark ==="

AFS_NODE1="localhost:50051"
AFS_NODE2="localhost:50052"
AFS_NODE3="localhost:50053"
AFS_ADDRS="$AFS_NODE1,$AFS_NODE2,$AFS_NODE3"

DATA1="./afs_data/server1"
DATA2="./afs_data/server2"
DATA3="./afs_data/server3"

echo "--- Compiling binary ---"
go build -o prime_app coordinator_worker/main.go

INPUT_FILES=$(ls "$DATA1/input/input_dataset_"*.txt 2>/dev/null \
    | xargs -n1 basename \
    | tr '\n' ',' \
    | sed 's/,$//')

if [ -z "$INPUT_FILES" ]; then
    echo "FAIL: No input_dataset_*.txt files found in $DATA1/input/"
    exit 1
fi

WORKER_COUNTS=(1 2 4 8)
RESULTS=()

for COUNT in "${WORKER_COUNTS[@]}"; do
    echo "---------------------------------------------------"
    echo "Running benchmark with $COUNT worker(s)..."
    
    pkill -f "go run ./server" || true
    pkill -f "prime_app"       || true
    
    fuser -k 50051/tcp 50052/tcp 50053/tcp 2>/dev/null || true
    for p in $(seq 6001 6008); do fuser -k $p/tcp 2>/dev/null || true; done
    
    rm -f afs_data/server*/input/snapshot_latest.json
    rm -f afs_data/server*/input/primes.txt
    rm -rf /tmp/afs*
    sleep 1

    go run ./server --self="$AFS_NODE1" --peers="$AFS_NODE2,$AFS_NODE3" --data="$DATA1" --port=50051 --clean > /dev/null 2>&1 &
    go run ./server --self="$AFS_NODE2" --peers="$AFS_NODE1,$AFS_NODE3" --data="$DATA2" --port=50052 --clean > /dev/null 2>&1 &
    go run ./server --self="$AFS_NODE3" --peers="$AFS_NODE1,$AFS_NODE2" --data="$DATA3" --port=50053 --clean > /dev/null 2>&1 &
    
    echo "Waiting for Raft leader election (5s)..."
    sleep 5
    
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

    ./prime_app --mode=coordinator \
        --afs="$AFS_ADDRS" \
        --inputs="$INPUT_FILES" \
        --output="primes.txt" \
        --workers="$WORKER_ADDRS" > bench_output.txt 2>&1
        
    TIME_TAKEN=$(grep "Time taken" bench_output.txt | awk -F': ' '{print $2}' | tr -d '[:space:]')
    
    if [ -z "$TIME_TAKEN" ]; then
        TIME_TAKEN="FAILED"
        echo " Benchmark failed! Dumping coordinator log:"
        cat bench_output.txt
    fi
    
    echo "Completed in: $TIME_TAKEN"
    RESULTS+=("$COUNT Workers | $TIME_TAKEN")
done

echo ""
echo "====================================="
echo "      FINAL SCALING BENCHMARK        "
echo "====================================="
for res in "${RESULTS[@]}"; do
    echo "  $res"
done
echo "====================================="

rm -f bench_output.txt
trap 'pkill -f "go run ./server" || true; pkill -f "prime_app" || true; rm -f prime_app' EXIT