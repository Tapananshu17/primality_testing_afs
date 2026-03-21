package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "primality_afs/proto"
)

const (
	port      = ":50051"
	inputDir  = "./afs_data/server1/input"
	outputDir = "./afs_data/server1/output"
	metaFile  = "server_metadata.json"
	chunkSize = 64 * 1024
)

type afsServer struct {
	pb.UnimplementedAFSServer
	mu       sync.RWMutex
	metadata map[string]int32
}

func newServer() *afsServer {
	s := &afsServer{metadata: make(map[string]int32)}
	s.initMetadata()
	return s
}

func (s *afsServer) initMetadata() {
	s.mu.Lock()
	defer s.mu.Unlock()

	os.MkdirAll(inputDir, 0755)
	os.MkdirAll(outputDir, 0755)

	metaPath := filepath.Join(outputDir, metaFile)
	if data, err := os.ReadFile(metaPath); err == nil {
		json.Unmarshal(data, &s.metadata)
		return
	}

	entries, _ := os.ReadDir(inputDir)
	for _, entry := range entries {
		if !entry.IsDir() {
			s.metadata[entry.Name()] = 1
		}
	}
	s.saveMetadata()
}

func (s *afsServer) saveMetadata() {
	metaPath := filepath.Join(outputDir, metaFile)
	data, _ := json.MarshalIndent(s.metadata, "", "  ")
	os.WriteFile(metaPath, data, 0644)
}

func (s *afsServer) getFilePath(filename string) string {
	base := filepath.Base(filename) // Strip any path traversal attempts
	if base == "primes.txt" {
		return filepath.Join(outputDir, base)
	}
	return filepath.Join(inputDir, base)
}

func (s *afsServer) TestAuth(ctx context.Context, req *pb.TestAuthRequest) (*pb.TestAuthResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	base := filepath.Base(req.Filename)
	serverVer, exists := s.metadata[base]
	if !exists {
		return &pb.TestAuthResponse{IsValid: false, ServerVersion: 0}, nil
	}
	return &pb.TestAuthResponse{IsValid: req.Version == serverVer, ServerVersion: serverVer}, nil
}

func (s *afsServer) FetchFile(req *pb.FileRequest, stream pb.AFS_FetchFileServer) error {
	base := filepath.Base(req.Filename)
	filePath := s.getFilePath(base)

	file, err := os.Open(filePath)
	if err != nil {
		return status.Errorf(codes.NotFound, "file not found on server")
	}
	defer file.Close()

	s.mu.RLock()
	version := s.metadata[base]
	s.mu.RUnlock()

	buf := make([]byte, chunkSize)
	firstChunk := true

	for {
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		chunk := &pb.FileChunk{Content: buf[:n]}
		if firstChunk {
			chunk.Version = version
			firstChunk = false
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (s *afsServer) StoreFile(stream pb.AFS_StoreFileServer) error {
	var filename, tempPath string
	var tempFile *os.File

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if tempFile != nil {
				tempFile.Close()
				os.Remove(tempPath)
			}
			return err
		}

		if filename == "" {
			filename = filepath.Base(chunk.Filename)
			tempPath = filepath.Join(outputDir, filename+".tmp")
			tempFile, err = os.Create(tempPath)
			if err != nil {
				return err
			}
		}

		if _, err := tempFile.Write(chunk.Content); err != nil {
			return err
		}
	}

	if tempFile != nil {
		tempFile.Close()
		finalPath := s.getFilePath(filename)
		os.Rename(tempPath, finalPath)

		s.mu.Lock()
		if s.metadata[filename] == 0 {
			s.metadata[filename] = 1
		} else {
			s.metadata[filename]++
		}
		newVer := s.metadata[filename]
		s.saveMetadata()
		s.mu.Unlock()

		return stream.SendAndClose(&pb.StoreResponse{Success: true, NewVersion: newVer})
	}
	return status.Errorf(codes.InvalidArgument, "empty stream")
}

func main() {
	lis, _ := net.Listen("tcp", port)
	grpcServer := grpc.NewServer()
	pb.RegisterAFSServer(grpcServer, newServer())
	log.Printf("Server listening on %v", port)
	grpcServer.Serve(lis)
}