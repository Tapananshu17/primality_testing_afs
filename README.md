# 1. Initialize the Go module
go mod init primality_afs

# 2. Compile the .proto file
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/afs.proto

# 3. Download gRPC dependencies
go mod tidy

# To run
go run server/main.go
go run test.go
