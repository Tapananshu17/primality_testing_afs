package client

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "primality_afs/proto"
)

const chunkSize = 64 * 1024

type OpenFile struct {
	osFile     *os.File
	filename   string
	isModified bool
}

type AFSClient struct {
	client    pb.AFSClient
	cacheDir  string
	cacheMeta map[string]int32
	metaFile  string
	openFiles map[int]*OpenFile
	nextFD    int
	mu        sync.Mutex
}

func InitAFS(serverAddr, cacheDir string) (*AFSClient, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	os.MkdirAll(cacheDir, 0755)
	metaPath := filepath.Join(cacheDir, "local_cache.json")

	c := &AFSClient{
		client:    pb.NewAFSClient(conn),
		cacheDir:  cacheDir,
		metaFile:  metaPath,
		cacheMeta: make(map[string]int32),
		openFiles: make(map[int]*OpenFile),
		nextFD:    3,
	}

	if data, err := os.ReadFile(metaPath); err == nil {
		json.Unmarshal(data, &c.cacheMeta)
	}
	return c, nil
}

func (c *AFSClient) saveCacheMeta() {
	data, _ := json.MarshalIndent(c.cacheMeta, "", "  ")
	os.WriteFile(c.metaFile, data, 0644)
}

func (c *AFSClient) AFS_Open(filename string, flag int) (int, error) {
	c.mu.Lock()
	localVer, hasCache := c.cacheMeta[filename]
	c.mu.Unlock()

	needsDownload := true
	if hasCache {
		resp, err := c.client.TestAuth(context.Background(), &pb.TestAuthRequest{
			Filename: filename,
			Version:  localVer,
		})
		if err == nil && resp.IsValid {
			needsDownload = false
		}
	}

	localPath := filepath.Join(c.cacheDir, filename)

	if needsDownload {
		stream, err := c.client.FetchFile(context.Background(), &pb.FileRequest{Filename: filename})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound && (flag&os.O_CREATE != 0) {
				os.WriteFile(localPath, []byte{}, 0644)
				c.mu.Lock()
				c.cacheMeta[filename] = 0
				c.saveCacheMeta()
				c.mu.Unlock()
			} else {
				return -1, err
			}
		} else {
			outFile, err := os.Create(localPath)
			if err != nil {
				return -1, err
			}
			newVersion := int32(0)
			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					st, _ := status.FromError(err)
					if st.Code() == codes.NotFound && (flag&os.O_CREATE != 0) {
						break
					}
					outFile.Close()
					return -1, err
				}
				if chunk.Version != 0 {
					newVersion = chunk.Version
				}
				outFile.Write(chunk.Content)
			}
			outFile.Close()

			c.mu.Lock()
			c.cacheMeta[filename] = newVersion
			c.saveCacheMeta()
			c.mu.Unlock()
		}
	}

	f, err := os.OpenFile(localPath, flag, 0644)
	if err != nil {
		return -1, err
	}

	c.mu.Lock()
	fd := c.nextFD
	c.nextFD++
	c.openFiles[fd] = &OpenFile{
		osFile:     f,
		filename:   filename,
		isModified: false,
	}
	c.mu.Unlock()

	return fd, nil
}

func (c *AFSClient) AFS_Read(fd int, b []byte) (int, error) {
	c.mu.Lock()
	of, exists := c.openFiles[fd]
	c.mu.Unlock()
	if !exists {
		return 0, os.ErrNotExist
	}
	return of.osFile.Read(b)
}

func (c *AFSClient) AFS_Write(fd int, b []byte) (int, error) {
	c.mu.Lock()
	of, exists := c.openFiles[fd]
	c.mu.Unlock()
	if !exists {
		return 0, os.ErrNotExist
	}
	n, err := of.osFile.Write(b)
	if err == nil {
		of.isModified = true
	}
	return n, err
}

func (c *AFSClient) AFS_Close(fd int) error {
	c.mu.Lock()
	of, exists := c.openFiles[fd]
	if !exists {
		c.mu.Unlock()
		return os.ErrNotExist
	}
	delete(c.openFiles, fd)
	c.mu.Unlock()

	of.osFile.Close()
	if !of.isModified {
		return nil
	}

	localPath := filepath.Join(c.cacheDir, of.filename)
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	stream, err := c.client.StoreFile(context.Background())
	if err != nil {
		return err
	}

	buf := make([]byte, chunkSize)
	firstChunk := true

	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		chunk := &pb.StoreChunk{Content: buf[:n]}
		if firstChunk {
			chunk.Filename = of.filename
			firstChunk = false
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.cacheMeta[of.filename] = resp.NewVersion
	c.saveCacheMeta()
	c.mu.Unlock()

	return nil
}