# Primality AFS (Raft Consensus Cluster)

A fault-tolerant Distributed File System (AFS) built with Go and gRPC, powered by the Raft consensus algorithm.  
It also includes a coordinator-worker architecture for distributed primality testing.

---

## 1. Setup Instructions

Initialize the module, compile protobufs, and install dependencies:

```bash
go mod init primality_afs

protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/afs.proto

go mod tidy
```

---

## 2. Running the Raft Cluster

Start three nodes in separate terminals. The `--clean` flag resets persisted state (useful for testing).

### Node 1
```bash
go run ./server \
  --self localhost:50051 \
  --peers localhost:50052,localhost:50053 \
  --data ./afs_data/server1 \
  --port 50051 \
  --clean
```

### Node 2
```bash
go run ./server \
  --self localhost:50052 \
  --peers localhost:50053,localhost:50051 \
  --data ./afs_data/server2 \
  --port 50052 \
  --clean
```

### Node 3
```bash
go run ./server \
  --self localhost:50053 \
  --peers localhost:50051,localhost:50052 \
  --data ./afs_data/server3 \
  --port 50053 \
  --clean
```

### Run via Script
```bash
chmod +x run_cluster.sh
./run_cluster.sh
```

---

## 3. Coordinator–Worker (Primality Testing)

```bash
# Clear cache
rm -rf /tmp/prime_cache

# Start coordinator + workers
go run ./coordinator_worker/

# Kill all worker processes (if needed)
pkill -f "mode=worker"
```

---

## 4. Testing & Demo Scenarios

Run the test suite:

```bash
./test_01_normal.sh
# OR
go run test.go
```

### Scenario 1: Basic Read
Reads an existing file from the primary node.

```
OK: read 23 bytes from input_dataset_001.txt
Content preview:
17
4
23...
```

---

### Scenario 2: Write + Replication Verification
Writes a file and verifies backups.

```
OK: StoreFile completed
OK: backup localhost:50052 verified
OK: backup localhost:50053 verified
```

---

### Scenario 3: Cache Hit (TestAuth)
Avoids re-downloading unchanged files.

```
INFO: first open (expect download)
OK: first open complete

INFO: second open (expect cache hit)
OK: second open took 360.833µs
```

---

### Scenario 4: Concurrent Writes
Multiple clients upload files simultaneously.

```
OK: client "concurrent_A" uploaded "primes.txt"
OK: client "concurrent_B" uploaded "input_dataset_002.txt"
```

---

### Scenario 5: Server Crash During Read (Failover)
Kill the primary during an active download.

**Action:**  
Upload a large file and start reading. Kill the primary (`Ctrl+C`).

**Expected:**  
- Backups elect a new leader  
- Client resumes automatically  

```
INFO: starting continuous read loop. KILL THE PRIMARY NOW!
downloading... (attempt 100/100)

OK: Failover triggered and handled mid-read. Took ~4–5s
```

---

### Scenario 6: Client Crash During Write
Ensures no partial files are stored.

```
OK: SUCCESS: File does not exist on server
(Error: file not found on backup: partial_test.txt)
```

---

### Scenario 7: Primary Failure Redirect
Reads/writes continue after leader change.

```
INFO: waiting 5s for backups to elect a new primary...
INFO: attempting write...

OK: write succeeded after primary failure
OK: read-back OK
```

---

## Notes

- `--clean` resets Raft state (useful for repeated testing)
- Each node must use a separate data directory
- Typical failover time: ~4–5 seconds