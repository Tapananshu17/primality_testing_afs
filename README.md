# Setup Instructions

## 1. Initialize the Go module

```bash
go mod init primality_afs
```

## 2. Compile the `.proto` file

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/afs.proto
```

## 3. Download gRPC dependencies

```bash
go mod tidy
```

---

# Steps to Run

### Terminal 1 (Primary)

```bash
go run ./server --port :50051 --backups localhost:50052,localhost:50053 --data ./afs_data/server1
```

### Terminal 2 (Backup 1)

```bash
go run ./backup --port :50052 --primary localhost:50051 --peers localhost:50053 --data ./afs_data/server2
```

### Terminal 3 (Backup 2)

```bash
go run ./backup --port :50053 --primary localhost:50051 --peers localhost:50052 --data ./afs_data/server3
```

### Run Test File

```bash
go run test.go
```

---

# Demo Scenarios

The demo runs the following scenarios in sequence:

1. Basic read from primary
2. Write + replication verification (read back from each backup directly)
3. Cache hit (TestAuth short-circuit)
4. Two clients writing concurrently
5. Primary failure simulation (kill primary manually when prompted)
6. Client crash during write (Whole file caching check)
7. Primary failure simulation

---

# Scenarios Output

## Scenario 1: Basic Read

```
OK: read 23 bytes from input_dataset_001.txt
Content preview:
17
4
23
10
17
33
13
19
```

## Scenario 2: Write + Replication

```
OK: StoreFile completed
OK: backup localhost:50052 verified
OK: backup localhost:50053 verified
```

## Scenario 3: Cache Hit

```
INFO: first open (expect download)
OK: first open complete
INFO: second open (expect cache hit)
OK: second open took 360.833µs
```

## Scenario 4: Concurrent Writes

```
OK: client "concurrent_A" uploaded "primes.txt"
OK: client "concurrent_B" uploaded "input_dataset_002.txt"
```

## Scenario 5: Server Crash During Read

```
INFO: starting continuous read loop. KILL THE PRIMARY NOW!
downloading... (attempt 100/100)
WARN: Loop finished before failover was detected.
```

## Scenario 6: Client Crash During Write

```
OK: SUCCESS: File does not exist on server (Error: rpc error: code = NotFound desc = file not found on backup: partial_test.txt)
```

## Scenario 7: Primary Failure & Client Redirect

```
INFO: waiting 5s for backups to elect a new primary...
INFO: attempting write...
OK: write succeeded after primary failure
OK: read-back OK: "written after primary failure t=1774790704"
```

