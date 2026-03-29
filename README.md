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

 The demo runs five scenarios in sequence:
   1. Basic read from primary
   2. Write + replication verification (read back from each backup directly)
   3. Cache hit (TestAuth short-circuit)
   4. Two clients writing concurrently
   5. Primary failure simulation (kill primary manually when prompted)
