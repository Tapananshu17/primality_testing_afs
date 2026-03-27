// client/afs_client.go

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	maxRedirects = 5
	rpcTimeout   = 10 * time.Second
	// How long to keep retrying discoverPrimary before giving up.
	// This covers the election window (~4s timeout + a bit of slack).
	discoveryTimeout = 10 * time.Second
)

// ─── Types ────────────────────────────────────────────────────────────────────

type OpenFile struct {
	osFile     *os.File
	filename   string
	isModified bool
}

type AFSClient struct {
	mu         sync.Mutex
	cacheDir   string
	knownAddrs []string

	primaryAddr   string
	primaryClient pb.AFSClient

	cacheMeta map[string]int32
	metaFile  string

	openFiles map[int]*OpenFile
	nextFD    int
}

// ─── Init ─────────────────────────────────────────────────────────────────────

// InitAFS accepts a comma-separated list of cluster node addresses.
// It will contact each node and find whoever claims isPrimary=true.
func InitAFS(serverAddrs, cacheDir string) (*AFSClient, error) {
	addrs := strings.Split(serverAddrs, ",")
	for i, a := range addrs {
		addrs[i] = strings.TrimSpace(a)
	}

	os.MkdirAll(cacheDir, 0755)
	metaPath := filepath.Join(cacheDir, "local_cache.json")

	c := &AFSClient{
		cacheDir:   cacheDir,
		metaFile:   metaPath,
		knownAddrs: addrs,
		cacheMeta:  make(map[string]int32),
		openFiles:  make(map[int]*OpenFile),
		nextFD:     3,
	}

	if data, err := os.ReadFile(metaPath); err == nil {
		json.Unmarshal(data, &c.cacheMeta)
	}

	if err := c.discoverPrimary(); err != nil {
		return nil, fmt.Errorf("could not find primary: %w", err)
	}
	return c, nil
}

// discoverPrimary polls every known address asking GetPrimary.
//
// Key fix vs the previous version: a node returning IsPrimary=false with an
// address is just reporting what it *thinks* the primary is — that address may
// itself be dead.  We only trust a node that returns IsPrimary=true for itself,
// i.e. info.Address == that node's own address AND info.IsPrimary==true.
//
// We retry in a loop for up to discoveryTimeout so the client is patient
// during an ongoing election (which takes ~4 s by default).
func (c *AFSClient) discoverPrimary() error {
	deadline := time.Now().Add(discoveryTimeout)

	for time.Now().Before(deadline) {
		for _, addr := range c.knownAddrs {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			info, err := pb.NewAFSClient(conn).GetPrimary(ctx, &pb.Empty{})
			cancel()
			conn.Close()

			if err != nil || !info.IsPrimary {
				// This node is a follower or unreachable — skip.
				continue
			}

			// This node says it IS the primary — dial it directly.
			primaryConn, err := grpc.Dial(info.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			c.primaryAddr = info.Address
			c.primaryClient = pb.NewAFSClient(primaryConn)
			return nil
		}
		// No node claimed primary yet — election may be in progress. Wait and retry.
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("no node claimed primary within %s (tried: %v)",
		discoveryTimeout, c.knownAddrs)
}

// refreshPrimary re-runs discovery; called automatically after redirect/failure.
func (c *AFSClient) refreshPrimary() error {
	return c.discoverPrimary()
}

// dialAddr is a helper for redirect handling — dials a specific address and
// updates the client's primary stub.
func (c *AFSClient) dialAddr(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.primaryAddr = addr
	c.primaryClient = pb.NewAFSClient(conn)
	return nil
}

// extractRedirectAddr pulls the new primary address from a FailedPrecondition
// error of the form "not primary; redirect to <addr>".
func extractRedirectAddr(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return "", false
	}
	const prefix = "not primary; redirect to "
	msg := st.Message()
	if idx := strings.Index(msg, prefix); idx >= 0 {
		return strings.TrimSpace(msg[idx+len(prefix):]), true
	}
	return "", false
}

func (c *AFSClient) saveCacheMeta() {
	data, _ := json.MarshalIndent(c.cacheMeta, "", "  ")
	os.WriteFile(c.metaFile, data, 0644)
}

// ─── Public API ───────────────────────────────────────────────────────────────

func (c *AFSClient) AFS_Open(filename string, flag int) (int, error) {
	c.mu.Lock()
	localVer, hasCache := c.cacheMeta[filename]
	primaryClient := c.primaryClient
	c.mu.Unlock()

	needsDownload := true
	if hasCache {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := primaryClient.TestAuth(ctx, &pb.TestAuthRequest{
			Filename: filename,
			Version:  localVer,
		})
		cancel()
		if err == nil && resp.IsValid {
			needsDownload = false
		}
	}

	localPath := filepath.Join(c.cacheDir, filename)

	if needsDownload {
		if err := c.downloadFile(filename, flag, localPath); err != nil {
			return -1, err
		}
	}

	f, err := os.OpenFile(localPath, flag, 0644)
	if err != nil {
		return -1, err
	}

	c.mu.Lock()
	fd := c.nextFD
	c.nextFD++
	c.openFiles[fd] = &OpenFile{osFile: f, filename: filename, isModified: false}
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

// AFS_Close flushes modified files to the primary.
// On redirect (FailedPrecondition) it dials the new address and retries.
// On Unavailable (election in progress) it runs full re-discovery and retries.
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
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < maxRedirects; attempt++ {
		c.mu.Lock()
		primaryClient := c.primaryClient
		c.mu.Unlock()

		newVersion, err := c.uploadFile(primaryClient, of.filename, data)
		if err == nil {
			c.mu.Lock()
			c.cacheMeta[of.filename] = newVersion
			c.saveCacheMeta()
			c.mu.Unlock()
			return nil
		}

		// Case 1: backup told us exactly where the primary is — dial it directly.
		if redirectAddr, ok := extractRedirectAddr(err); ok {
			c.mu.Lock()
			dialErr := c.dialAddr(redirectAddr)
			c.mu.Unlock()
			if dialErr == nil {
				continue // retry with explicit redirect address
			}
		}

		// Case 2: the node we tried is unavailable (could be mid-election).
		// Re-discover by polling all known nodes until one claims primary.
		if st, ok := status.FromError(err); ok && (st.Code() == codes.Unavailable ||
			st.Code() == codes.FailedPrecondition) {
			c.mu.Lock()
			discErr := c.refreshPrimary()
			c.mu.Unlock()
			if discErr == nil {
				continue
			}
		}

		return err
	}
	return fmt.Errorf("exceeded max redirects (%d) uploading %s", maxRedirects, of.filename)
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

func (c *AFSClient) downloadFile(filename string, flag int, localPath string) error {
	c.mu.Lock()
	primaryClient := c.primaryClient
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	stream, err := primaryClient.FetchFile(ctx, &pb.FileRequest{Filename: filename})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound && (flag&os.O_CREATE != 0) {
			if writeErr := os.WriteFile(localPath, []byte{}, 0644); writeErr != nil {
				return writeErr
			}
			c.mu.Lock()
			c.cacheMeta[filename] = 0
			c.saveCacheMeta()
			c.mu.Unlock()
			return nil
		}
		return err
	}

	outFile, err := os.Create(localPath)
	if err != nil {
		return err
	}
	newVersion := int32(0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			outFile.Close()
			return err
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
	return nil
}

func (c *AFSClient) uploadFile(primaryClient pb.AFSClient, filename string, data []byte) (int32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	stream, err := primaryClient.StoreFile(ctx)
	if err != nil {
		return 0, err
	}

	first := true
	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := &pb.StoreChunk{Content: data[off:end]}
		if first {
			chunk.Filename = filename
			first = false
		}
		if sendErr := stream.Send(chunk); sendErr != nil {
			return 0, sendErr
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf("server rejected store for %s", filename)
	}
	return resp.NewVersion, nil
}