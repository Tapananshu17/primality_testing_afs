// server/main.go  –  Primary AFS server
//
// Start as:
//   go run ./server  \
//       --port :50051 \
//       --backups localhost:50052,localhost:50053 \
//       --data  ./afs_data/server1

package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "primality_afs/proto"
)

const (
	chunkSize    = 64 * 1024
	metaFile     = "server_metadata.json"
	heartbeatInt = 1 * time.Second
)

type primaryServer struct {
	pb.UnimplementedAFSServer

	mu       sync.RWMutex
	metadata map[string]int32

	dataDir   string
	inputDir  string
	outputDir string

	selfAddr    string
	backupAddrs []string
	backups     []pb.ReplicaClient
}

func newPrimaryServer(selfAddr string, backupAddrs []string, dataDir string) *primaryServer {
	s := &primaryServer{
		metadata:    make(map[string]int32),
		dataDir:     dataDir,
		inputDir:    filepath.Join(dataDir, "input"),
		outputDir:   filepath.Join(dataDir, "output"),
		selfAddr:    selfAddr,
		backupAddrs: backupAddrs,
	}

	os.MkdirAll(s.inputDir, 0755)
	os.MkdirAll(s.outputDir, 0755)

	s.loadMetadata()
	s.connectToBackups()

	return s
}

func (s *primaryServer) loadMetadata() {
	s.mu.Lock()
	defer s.mu.Unlock()

	metaPath := filepath.Join(s.outputDir, metaFile)
	if data, err := os.ReadFile(metaPath); err == nil {
		json.Unmarshal(data, &s.metadata)
		return
	}

	entries, _ := os.ReadDir(s.inputDir)
	for _, e := range entries {
		if !e.IsDir() {
			s.metadata[e.Name()] = 1
		}
	}

	s.saveMetadataLocked()
}

func (s *primaryServer) saveMetadataLocked() {
	metaPath := filepath.Join(s.outputDir, metaFile)
	data, _ := json.MarshalIndent(s.metadata, "", "  ")
	os.WriteFile(metaPath, data, 0644)
}

func (s *primaryServer) connectToBackups() {
	for _, addr := range s.backupAddrs {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Printf("[primary] WARNING: could not initiate connection to backup %s: %v", addr, err)
			s.backups = append(s.backups, nil)
			continue
		}

		log.Printf("[primary] dial initiated for backup %s", addr)
		s.backups = append(s.backups, pb.NewReplicaClient(conn))
	}
}

func (s *primaryServer) runHeartbeats() {
	ticker := time.NewTicker(heartbeatInt)
	defer ticker.Stop()

	for range ticker.C {
		for i, stub := range s.backups {
			if stub == nil {
				continue
			}

			go func(i int, stub pb.ReplicaClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				if _, err := stub.Heartbeat(ctx, &pb.HeartbeatRequest{
					PrimaryAddr: s.selfAddr,
					Term:        1,
				}); err != nil {
					log.Printf("[primary] heartbeat to backup[%d] failed: %v", i, err)
				}
			}(i, stub)
		}
	}
}

func (s *primaryServer) filePath(filename string) string {
	base := filepath.Base(filename)
	if base == "primes.txt" {
		return filepath.Join(s.outputDir, base)
	}
	return filepath.Join(s.inputDir, base)
}

func (s *primaryServer) GetPrimary(_ context.Context, _ *pb.Empty) (*pb.PrimaryInfo, error) {
	return &pb.PrimaryInfo{Address: s.selfAddr, IsPrimary: true}, nil
}

func (s *primaryServer) TestAuth(_ context.Context, req *pb.TestAuthRequest) (*pb.TestAuthResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	base := filepath.Base(req.Filename)
	serverVer, exists := s.metadata[base]
	if !exists {
		return &pb.TestAuthResponse{IsValid: false, ServerVersion: 0}, nil
	}

	return &pb.TestAuthResponse{
		IsValid:       req.Version == serverVer,
		ServerVersion: serverVer,
	}, nil
}

func (s *primaryServer) FetchFile(req *pb.FileRequest, stream pb.AFS_FetchFileServer) error {
	base := filepath.Base(req.Filename)
	path := s.filePath(base)

	f, err := os.Open(path)
	if err != nil {
		return status.Errorf(codes.NotFound, "file not found: %s", base)
	}
	defer f.Close()

	s.mu.RLock()
	version := s.metadata[base]
	s.mu.RUnlock()

	buf := make([]byte, chunkSize)
	first := true

	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		chunk := &pb.FileChunk{Content: buf[:n]}
		if first {
			chunk.Version = version
			first = false
		}

		if sendErr := stream.Send(chunk); sendErr != nil {
			return sendErr
		}
	}

	return nil
}

func (s *primaryServer) StoreFile(stream pb.AFS_StoreFileServer) error {
	var filename string
	var fileData []byte

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if filename == "" {
			filename = filepath.Base(chunk.Filename)
		}
		fileData = append(fileData, chunk.Content...)
	}

	if filename == "" {
		return status.Error(codes.InvalidArgument, "empty stream or missing filename")
	}

	s.mu.Lock()
	cur := s.metadata[filename]
	var newVersion int32
	if cur == 0 {
		newVersion = 1
	} else {
		newVersion = cur + 1
	}
	s.mu.Unlock()

	totalNodes := 1 + len(s.backups)
	majority := totalNodes/2 + 1

	type result struct{ ok bool }
	results := make(chan result, len(s.backups))

	for _, stub := range s.backups {
		if stub == nil {
			results <- result{ok: false}
			continue
		}

		go func(stub pb.ReplicaClient) {
			results <- result{ok: s.replicateTo(stub, filename, fileData, newVersion)}
		}(stub)
	}

	acks := 1
	for range s.backups {
		if r := <-results; r.ok {
			acks++
		}
	}

	if acks < majority {
		log.Printf("[primary] replication quorum NOT met (%d/%d) for %s", acks, majority, filename)
		return status.Errorf(codes.Unavailable,
			"replication quorum not met (%d/%d acks)", acks, majority)
	}

	log.Printf("[primary] replication quorum met (%d/%d) for %s", acks, majority, filename)

	if err := s.commitLocal(filename, fileData); err != nil {
		return status.Errorf(codes.Internal, "local commit failed: %v", err)
	}

	s.mu.Lock()
	s.metadata[filename] = newVersion
	s.saveMetadataLocked()
	s.mu.Unlock()

	return stream.SendAndClose(&pb.StoreResponse{
		Success:    true,
		NewVersion: newVersion,
	})
}

func (s *primaryServer) replicateTo(stub pb.ReplicaClient, filename string, data []byte, version int32) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := stub.ReplicateFile(ctx)
	if err != nil {
		log.Printf("[primary] ReplicateFile open to backup failed: %v", err)
		return false
	}

	first := true
	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := &pb.ReplicateChunk{
			Filename: filename,
			Content:  data[off:end],
		}

		if first {
			chunk.Version = version
			first = false
		}

		if err := stream.Send(chunk); err != nil {
			log.Printf("[primary] send chunk to backup failed: %v", err)
			return false
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil || !resp.Success {
		log.Printf("[primary] backup ReplicateFile failed: err=%v success=%v", err, resp.GetSuccess())
		return false
	}

	return true
}

func (s *primaryServer) commitLocal(filename string, data []byte) error {
	finalPath := s.filePath(filename)
	tmpPath := finalPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, finalPath)
}

func main() {
	portFlag := flag.String("port", ":50051", "listen address for this primary node")
	backupFlag := flag.String("backups", "", "comma-separated backup addresses")
	dataFlag := flag.String("data", "./afs_data/server1", "root data directory")
	flag.Parse()

	var backupAddrs []string
	if *backupFlag != "" {
		for _, a := range strings.Split(*backupFlag, ",") {
			if a = strings.TrimSpace(a); a != "" {
				backupAddrs = append(backupAddrs, a)
			}
		}
	}

	selfAddr := "localhost" + *portFlag

	srv := newPrimaryServer(selfAddr, backupAddrs, *dataFlag)
	go srv.runHeartbeats()

	lis, err := net.Listen("tcp", *portFlag)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterAFSServer(grpcSrv, srv)

	log.Printf("[primary] listening on %s  backups=%v", *portFlag, backupAddrs)

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}