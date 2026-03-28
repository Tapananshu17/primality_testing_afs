// backup/main.go  –  Backup AFS server
//
// Start as:
//   go run ./backup \
//       --port    :50052 \
//       --primary localhost:50051 \
//       --peers   localhost:50053 \
//       --data    ./afs_data/server2

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
	chunkSize       = 64 * 1024
	metaFile        = "server_metadata.json"
	electionTimeout = 4 * time.Second
	heartbeatInt    = 1 * time.Second
)

type backupServer struct {
	pb.UnimplementedAFSServer
	pb.UnimplementedReplicaServer

	mu       sync.RWMutex
	metadata map[string]int32

	inputDir  string
	outputDir string
	selfAddr  string

	primaryMu     sync.RWMutex
	primaryAddr   string
	primaryClient pb.AFSClient

	lastHeartbeat time.Time
	isPrimary     bool

	peerAddrs []string
}

func newBackupServer(selfAddr, primaryAddr string, peerAddrs []string, dataDir string) *backupServer {
	s := &backupServer{
		metadata:      make(map[string]int32),
		inputDir:      filepath.Join(dataDir, "input"),
		outputDir:     filepath.Join(dataDir, "output"),
		selfAddr:      selfAddr,
		primaryAddr:   primaryAddr,
		peerAddrs:     peerAddrs,
		lastHeartbeat: time.Now(),
	}

	os.MkdirAll(s.inputDir, 0755)
	os.MkdirAll(s.outputDir, 0755)

	s.loadMetadata()
	s.connectToPrimary(primaryAddr)
	return s
}

func (s *backupServer) loadMetadata() {
	s.mu.Lock()
	defer s.mu.Unlock()

	metaPath := filepath.Join(s.outputDir, metaFile)
	if data, err := os.ReadFile(metaPath); err == nil {
		json.Unmarshal(data, &s.metadata)
	}
}

func (s *backupServer) saveMetadataLocked() {
	metaPath := filepath.Join(s.outputDir, metaFile)
	data, _ := json.MarshalIndent(s.metadata, "", "  ")
	os.WriteFile(metaPath, data, 0644)
}

func (s *backupServer) connectToPrimary(addr string) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[backup %s] warning: could not connect to primary %s: %v", s.selfAddr, addr, err)
		return
	}

	s.primaryMu.Lock()
	s.primaryAddr = addr
	s.primaryClient = pb.NewAFSClient(conn)
	s.primaryMu.Unlock()

	log.Printf("[backup %s] connected to primary %s", s.selfAddr, addr)
}

func (s *backupServer) watchHeartbeat() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.RLock()
		alreadyPrimary := s.isPrimary
		elapsed := time.Since(s.lastHeartbeat)
		s.mu.RUnlock()

		if alreadyPrimary {
			continue
		}

		if elapsed > electionTimeout {
			log.Printf("[backup %s] primary timeout (%.1fs) — starting election", s.selfAddr, elapsed.Seconds())
			s.electSelf()
		}
	}
}

func (s *backupServer) electSelf() {
	votes := 1
	totalNodes := 1 + len(s.peerAddrs)

	var wg sync.WaitGroup
	var voteMu sync.Mutex

	for _, peerAddr := range s.peerAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			info, err := pb.NewAFSClient(conn).GetPrimary(ctx, &pb.Empty{})
			if err != nil || !info.IsPrimary {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(peerAddr)
	}

	wg.Wait()

	majority := totalNodes/2 + 1
	if votes < majority {
		log.Printf("[backup %s] election failed: only %d/%d votes", s.selfAddr, votes, majority)
		return
	}

	log.Printf("[backup %s] elected as NEW PRIMARY (%d/%d votes)", s.selfAddr, votes, majority)

	s.mu.Lock()
	s.isPrimary = true
	s.mu.Unlock()

	for _, peerAddr := range s.peerAddrs {
		go func(addr string) {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			pb.NewReplicaClient(conn).Heartbeat(ctx, &pb.HeartbeatRequest{
				PrimaryAddr: s.selfAddr,
				Term:        2,
			})
		}(peerAddr)
	}

	go s.runHeartbeats()
}

func (s *backupServer) runHeartbeats() {
	ticker := time.NewTicker(heartbeatInt)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.RLock()
		stillPrimary := s.isPrimary
		s.mu.RUnlock()

		if !stillPrimary {
			return
		}

		for _, peerAddr := range s.peerAddrs {
			go func(addr string) {
				conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				pb.NewReplicaClient(conn).Heartbeat(ctx, &pb.HeartbeatRequest{
					PrimaryAddr: s.selfAddr,
					Term:        2,
				})
			}(peerAddr)
		}
	}
}

func (s *backupServer) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	s.lastHeartbeat = time.Now()

	if req.PrimaryAddr != "" && req.PrimaryAddr != s.primaryAddr {
		log.Printf("[backup %s] new primary announced: %s", s.selfAddr, req.PrimaryAddr)
		s.isPrimary = false
		s.mu.Unlock()
		s.connectToPrimary(req.PrimaryAddr)
		return &pb.HeartbeatResponse{Ok: true}, nil
	}

	s.mu.Unlock()
	return &pb.HeartbeatResponse{Ok: true}, nil
}

func (s *backupServer) ReplicateFile(stream pb.Replica_ReplicateFileServer) error {
	var filename string
	var data []byte
	var version int32

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
		if chunk.Version != 0 {
			version = chunk.Version
		}

		data = append(data, chunk.Content...)
	}

	if filename == "" {
		return status.Error(codes.InvalidArgument, "empty replication stream")
	}

	finalPath := s.localPath(filename)
	tmpPath := finalPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	s.mu.Lock()
	s.metadata[filename] = version
	s.saveMetadataLocked()
	s.mu.Unlock()

	log.Printf("[backup %s] replicated %s v%d (%d bytes)", s.selfAddr, filename, version, len(data))
	return stream.SendAndClose(&pb.ReplicateResponse{Success: true, Version: version})
}

func (s *backupServer) GetPrimary(_ context.Context, _ *pb.Empty) (*pb.PrimaryInfo, error) {
	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()

	if isPrimary {
		return &pb.PrimaryInfo{Address: s.selfAddr, IsPrimary: true}, nil
	}

	s.primaryMu.RLock()
	addr := s.primaryAddr
	s.primaryMu.RUnlock()

	return &pb.PrimaryInfo{Address: addr, IsPrimary: false}, nil
}

func (s *backupServer) TestAuth(_ context.Context, req *pb.TestAuthRequest) (*pb.TestAuthResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	base := filepath.Base(req.Filename)
	ver, exists := s.metadata[base]
	if !exists {
		return &pb.TestAuthResponse{IsValid: false, ServerVersion: 0}, nil
	}

	return &pb.TestAuthResponse{IsValid: req.Version == ver, ServerVersion: ver}, nil
}

func (s *backupServer) FetchFile(req *pb.FileRequest, stream pb.AFS_FetchFileServer) error {
	base := filepath.Base(req.Filename)
	path := s.localPath(base)

	f, err := os.Open(path)
	if err != nil {
		return status.Errorf(codes.NotFound, "file not found on backup: %s", base)
	}
	defer f.Close()

	s.mu.RLock()
	version := s.metadata[base]
	s.mu.RUnlock()

	buf := make([]byte, chunkSize)
	first := true

	for {
		n, readErr := f.Read(buf)
		if readErr != nil && readErr != io.EOF {
			return readErr
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

func (s *backupServer) StoreFile(stream pb.AFS_StoreFileServer) error {
	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()

	if isPrimary {
		return s.handleWriteAsPrimary(stream)
	}

	s.primaryMu.RLock()
	addr := s.primaryAddr
	s.primaryMu.RUnlock()

	return status.Errorf(codes.FailedPrecondition, "not primary; redirect to %s", addr)
}

func (s *backupServer) handleWriteAsPrimary(stream pb.AFS_StoreFileServer) error {
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

	totalNodes := 1 + len(s.peerAddrs)
	majority := totalNodes/2 + 1
	results := make(chan bool, len(s.peerAddrs))

	for _, peerAddr := range s.peerAddrs {
		go func(addr string) {
			results <- s.replicateToPeer(addr, filename, fileData, newVersion)
		}(peerAddr)
	}

	acks := 1
	for range s.peerAddrs {
		if <-results {
			acks++
		}
	}

	if acks < majority {
		return status.Errorf(codes.Unavailable, "replication quorum not met (%d/%d acks)", acks, majority)
	}

	finalPath := s.localPath(filename)
	tmpPath := finalPath + ".tmp"

	if err := os.WriteFile(tmpPath, fileData, 0644); err != nil {
		return status.Errorf(codes.Internal, "local write failed: %v", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return status.Errorf(codes.Internal, "local rename failed: %v", err)
	}

	s.mu.Lock()
	s.metadata[filename] = newVersion
	s.saveMetadataLocked()
	s.mu.Unlock()

	log.Printf("[elected-primary %s] committed %s v%d", s.selfAddr, filename, newVersion)
	return stream.SendAndClose(&pb.StoreResponse{Success: true, NewVersion: newVersion})
}

func (s *backupServer) replicateToPeer(addr, filename string, data []byte, version int32) bool {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := pb.NewReplicaClient(conn).ReplicateFile(ctx)
	if err != nil {
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
			return false
		}
	}

	resp, err := stream.CloseAndRecv()
	return err == nil && resp.Success
}

func (s *backupServer) localPath(filename string) string {
	base := filepath.Base(filename)
	if base == "primes.txt" {
		return filepath.Join(s.outputDir, base)
	}
	return filepath.Join(s.inputDir, base)
}

func main() {
	portFlag := flag.String("port", ":50052", "listen address for this backup")
	primaryFlag := flag.String("primary", "localhost:50051", "address of the current primary")
	peersFlag := flag.String("peers", "", "comma-separated addresses of OTHER backups")
	dataFlag := flag.String("data", "./afs_data/server2", "root data directory for this backup")
	flag.Parse()

	selfAddr := "localhost" + *portFlag

	var peerAddrs []string
	if *peersFlag != "" {
		for _, a := range strings.Split(*peersFlag, ",") {
			if a = strings.TrimSpace(a); a != "" {
				peerAddrs = append(peerAddrs, a)
			}
		}
	}

	srv := newBackupServer(selfAddr, *primaryFlag, peerAddrs, *dataFlag)
	go srv.watchHeartbeat()

	lis, err := net.Listen("tcp", *portFlag)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterAFSServer(grpcSrv, srv)
	pb.RegisterReplicaServer(grpcSrv, srv)

	log.Printf("[backup] listening on %s  primary=%s  peers=%v", *portFlag, *primaryFlag, peerAddrs)

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}