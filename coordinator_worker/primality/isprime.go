// server/main.go
//
// AFS server node running Raft consensus.
//
// Usage:
//   go run ./server \
//     --self   localhost:50051 \
//     --peers  localhost:50052,localhost:50053 \
//     --data   /tmp/afs_node1 \
//     --port   50051

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "primality_afs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─── File Store ───────────────────────────────────────────────────────────────

type FileStore struct {
	mu       sync.RWMutex
	dataDir  string
	versions map[string]int32
}

func NewFileStore(dataDir string) *FileStore {
	inputDir := filepath.Join(dataDir, "input")
	outputDir := filepath.Join(dataDir, "output")
	
	// Ensure directories exist
	os.MkdirAll(inputDir, 0755)
	os.MkdirAll(outputDir, 0755)
	
	fs := &FileStore{
		dataDir:  dataDir,
		versions: make(map[string]int32),
	}

	// Legacy Support: Pre-load metadata for input files so the coordinator 
	// can immediately authenticate and fetch them, mirroring old_main.go.
	entries, err := os.ReadDir(inputDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				fs.versions[e.Name()] = 1
			}
		}
	}

	return fs
}

func (fs *FileStore) resolveReadPath(name string) string {
	outPath := filepath.Join(fs.dataDir, "output", name)
	if _, err := os.Stat(outPath); err == nil {
		return outPath
	}
	return filepath.Join(fs.dataDir, "input", name)
}

func (fs *FileStore) pathForWrite(name string) string {
	return filepath.Join(fs.dataDir, "output", name)
}

func (fs *FileStore) Write(name string, data []byte, version int32) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_ = os.WriteFile(fs.pathForWrite(name), data, 0644)
	fs.versions[name] = version
}

func (fs *FileStore) Read(name string) ([]byte, int32, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	data, err := os.ReadFile(fs.resolveReadPath(name))
	if err != nil {
		return nil, 0, false
	}
	return data, fs.versions[name], true
}

func (fs *FileStore) Version(name string) (int32, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	
	if _, err := os.Stat(fs.resolveReadPath(name)); os.IsNotExist(err) {
		return 0, false
	}
	
	v := fs.versions[name]
	return v, true
}

// ─── AFS gRPC service ─────────────────────────────────────────────────────────

type AFSService struct {
	pb.UnimplementedAFSServer
	store *FileStore
	raft  *RaftNode
	self  string
}

const chunkSize = 64 * 1024

func (s *AFSService) GetPrimary(_ context.Context, _ *pb.Empty) (*pb.PrimaryInfo, error) {
	isLeader := s.raft.isLeader()
	addr := s.self
	if !isLeader {
		addr = s.raft.LeaderAddr()
	}
	return &pb.PrimaryInfo{Address: addr, IsPrimary: isLeader}, nil
}

func (s *AFSService) TestAuth(_ context.Context, req *pb.TestAuthRequest) (*pb.TestAuthResponse, error) {
	serverVer, ok := s.store.Version(req.Filename)
	if !ok {
		return &pb.TestAuthResponse{IsValid: false, ServerVersion: 0}, nil
	}
	return &pb.TestAuthResponse{
		IsValid:       serverVer == req.Version,
		ServerVersion: serverVer,
	}, nil
}

func (s *AFSService) FetchFile(req *pb.FileRequest, stream pb.AFS_FetchFileServer) error {
	data, version, ok := s.store.Read(req.Filename)
	if !ok {
		return status.Errorf(codes.NotFound, "file not found: %s", req.Filename)
	}

	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := &pb.FileChunk{Content: data[off:end]}
		if off == 0 {
			chunk.Version = version
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (s *AFSService) StoreFile(stream pb.AFS_StoreFileServer) error {
	if !s.raft.isLeader() {
		leader := s.raft.LeaderAddr()
		return status.Errorf(codes.FailedPrecondition, "not primary; redirect to %s", leader)
	}

	var filename string
	var buf []byte

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if filename == "" && chunk.Filename != "" {
			filename = chunk.Filename
		}
		buf = append(buf, chunk.Content...)
	}

	if filename == "" {
		return status.Error(codes.InvalidArgument, "filename missing")
	}

	// Delegate to Raft to replicate the file bytes across the cluster
	version, err := s.raft.Submit(filename, buf)
	if err != nil {
		return err
	}

	return stream.SendAndClose(&pb.StoreResponse{Success: true, NewVersion: version})
}

// ─── Raft gRPC service ────────────────────────────────────────────────────────

type RaftService struct {
	pb.UnimplementedRaftServer
	node *RaftNode
}

func (rs *RaftService) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return rs.node.AppendEntries(ctx, req)
}

func (rs *RaftService) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return rs.node.RequestVote(ctx, req)
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	selfFlag  := flag.String("self",  "localhost:50051", "this node's address (host:port)")
	peersFlag := flag.String("peers", "",                "comma-separated addresses of other nodes")
	dataFlag  := flag.String("data",  "./afs_data/server1", "data directory")
	portFlag  := flag.String("port",  "50051",           "gRPC listen port")
	cleanFlag := flag.Bool("clean",   false,             "wipe data directory on startup (for testing)")
	flag.Parse()

	listenAddr := fmt.Sprintf(":%s", *portFlag)
	peers := parsePeers(*peersFlag, *selfFlag)

	log.Printf("[server] self=%s  peers=%v  data=%s", *selfFlag, peers, *dataFlag)

	if *cleanFlag {
		logPath := filepath.Join(*dataFlag, "raft_log.json")
		statePath := filepath.Join(*dataFlag, "raft_state.json")
		outPath := filepath.Join(*dataFlag, "output")
		
		_ = os.Remove(logPath)
		_ = os.Remove(statePath)
		_ = os.RemoveAll(outPath)
		
		log.Printf("[server] cleaned Raft state and output directory in %s", *dataFlag)
	}
	
	os.MkdirAll(*dataFlag, 0755)
	store := NewFileStore(*dataFlag)

	// applyFn is triggered by Raft once an entry is safely committed by the majority
	applyFn := func(filename string, data []byte, version int32) {
		store.Write(filename, data, version)
		log.Printf("[server] applied: %s v%d (%d bytes)", filename, version, len(data))
	}

	raftNode := NewRaftNode(*selfFlag, peers, *dataFlag, applyFn)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(256 * 1024 * 1024),
		grpc.MaxSendMsgSize(256 * 1024 * 1024),
	)

	pb.RegisterAFSServer(grpcServer, &AFSService{store: store, raft: raftNode, self: *selfFlag})
	pb.RegisterRaftServer(grpcServer, &RaftService{node: raftNode})

	go func() {
		log.Printf("[server] listening on %s", listenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	raftNode.Run() 
}

func parsePeers(peersFlag, self string) []string {
	var result []string
	scanner := bufio.NewScanner(strings.NewReader(peersFlag))
	scanner.Split(bufio.ScanWords)
	for _, p := range strings.Split(peersFlag, ",") {
		p = strings.TrimSpace(p)
		if p != "" && p != self {
			result = append(result, p)
		}
	}
	return result
}