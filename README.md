# 1. Initialize the Go module
go mod init primality_afs

# 2. Compile the .proto file
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/afs.proto

# 3. Download gRPC dependencies
go mod tidy

# Steps to run:

   Terminal 1 (primary):
     go run ./server --port :50051 --backups localhost:50052,localhost:50053 --data ./afs_data/server1

   Terminal 2 (backup 1):
     go run ./backup --port :50052 --primary localhost:50051 --peers localhost:50053 --data ./afs_data/server2

   Terminal 3 (backup 2):
     go run ./backup --port :50053 --primary localhost:50051 --peers localhost:50052 --data ./afs_data/server3

 Then run this file:
   go run test.go

 The demo runs seven scenarios in sequence:
   1. Basic read from primary
   2. Write + replication verification (read back from each backup directly)
   3. Cache hit (TestAuth short-circuit)
   4. Two clients writing concurrently
   5. Server crash during read (kill primary manually mid-download to test graceful failover)
   6. Client crash during write (Whole file caching check)
   7. Primary failure redirect (verify new writes/reads route to the newly elected primary)
  

### Crash Testing Notes
 
Scenario 5: Server crash during read (Mid-operation Failover)
* **Action:** The test uploads a 100MB file and begins a continuous download loop. You will be prompted to manually kill the Primary server (`Ctrl+C` on Terminal 1) right in the middle of a read operation.
* **Expected Outcome:** The client's active download temporarily stalls as the connection drops. The backups detect the primary's timeout and elect a new primary. Within ~4-5 seconds, the client automatically catches the disconnect, queries the new primary, and finishes the read without crashing or corrupting data.
* **Expected Log:** `OK: Failover triggered and handled mid-read. Took 4.67s`

# coordinator worker
cd primality_testing_afs
rm -rf /tmp/prime_cache
go run ./coordinator_worker/