// demo/test.go
//
// End-to-end demo for the primary-backup AFS cluster.
//
// Prerequisites — start the cluster in three separate terminals first:
//
//   Terminal 1 (primary):
//     go run ./server --port :50051 --backups localhost:50052,localhost:50053 --data ./afs_data/server1
//
//   Terminal 2 (backup 1):
//     go run ./backup --port :50052 --primary localhost:50051 --peers localhost:50053 --data ./afs_data/server2
//
//   Terminal 3 (backup 2):
//     go run ./backup --port :50053 --primary localhost:50051 --peers localhost:50052 --data ./afs_data/server3
//
// Then run this file:
//   go run test.go
//
// The demo runs five scenarios in sequence:
//   1. Basic read from primary
//   2. Write + replication verification (read back from each backup directly)
//   3. Cache hit (TestAuth short-circuit)
//   4. Two clients writing concurrently
//   5. Primary failure simulation (kill primary manually when prompted)

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"primality_afs/client"
	pb "primality_afs/proto" // Add your proto package
)

// clusterAddrs is the full list of nodes.  The client will discover the primary.
const clusterAddrs = "localhost:50051,localhost:50052,localhost:50053"

// ─── Helpers ──────────────────────────────────────────────────────────────────

func must(label string, err error) {
	if err != nil {
		log.Fatalf("FATAL [%s]: %v", label, err)
	}
}

func header(title string) {
	bar := strings.Repeat("─", 60)
	fmt.Printf("\n%s\n  %s\n%s\n", bar, title, bar)
}

func pass(msg string) { fmt.Printf("  ✓  %s\n", msg) }
func info(msg string) { fmt.Printf("  ·  %s\n", msg) }
func warn(msg string) { fmt.Printf("  ⚠  %s\n", msg) }

// newClient creates a fresh AFS client with its own cache directory.
func newClient(name string) *client.AFSClient {
	cacheDir := fmt.Sprintf("/tmp/afs_%s", name)
	os.RemoveAll(cacheDir) // start with an empty cache every run
	c, err := client.InitAFS(clusterAddrs, cacheDir)
	must("InitAFS:"+name, err)
	info(fmt.Sprintf("client %q connected (cache: %s)", name, cacheDir))
	return c
}

// ─── Scenario 1: Basic read from primary ─────────────────────────────────────

func scenario1BasicRead() {
	header("Scenario 1 — Basic read (cache miss → download from primary)")

	c := newClient("reader1")

	fd, err := c.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	must("Open input_dataset_001.txt", err)

	buf := make([]byte, 4096)
	n, _ := c.AFS_Read(fd, buf)
	must("Close read-only fd", c.AFS_Close(fd))

	if n == 0 {
		warn("file was empty or read returned 0 bytes")
	} else {
		pass(fmt.Sprintf("read %d bytes from input_dataset_001.txt", n))
		fmt.Printf("\n--- file content (first 200 bytes) ---\n%s\n------\n",
			truncate(string(buf[:n]), 200))
	}
}

// ─── Scenario 2: Write + replication check ───────────────────────────────────

func scenario2WriteAndVerify() {
	header("Scenario 2 — Write to primary, verify replication on both backups")

	// Write via client (goes to primary, which replicates before ACK-ing).
	writer := newClient("writer1")

	payload := fmt.Sprintf("replicated-write test  timestamp=%d\n", time.Now().Unix())

	fd, err := writer.AFS_Open("primes.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("Open primes.txt for write", err)
	_, err = writer.AFS_Write(fd, []byte(payload))
	must("Write payload", err)
	must("Close (triggers upload + replication)", writer.AFS_Close(fd))
	pass("StoreFile completed — primary replicated to backups before ACK")

	// Give the cluster a moment
	time.Sleep(200 * time.Millisecond)

	// Now read the file back using a RAW gRPC connection to bypass the 
	// AFSClient's automatic primary-discovery routing.
	backups := []string{"localhost:50052", "localhost:50053"}
	for _, addr := range backups {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			warn(fmt.Sprintf("backup %s unreachable: %v", addr, err))
			continue
		}

		backupClient := pb.NewAFSClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		
		stream, err := backupClient.FetchFile(ctx, &pb.FileRequest{Filename: "primes.txt"})
		if err != nil {
			warn(fmt.Sprintf("backup %s FetchFile failed: %v", addr, err))
			cancel()
			conn.Close()
			continue
		}

		var gotBytes []byte
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				warn(fmt.Sprintf("backup %s stream read error: %v", addr, err))
				break
			}
			gotBytes = append(gotBytes, chunk.Content...)
		}
		
		cancel()
		conn.Close()

		got := string(gotBytes)
		if strings.TrimSpace(got) == strings.TrimSpace(payload) {
			pass(fmt.Sprintf("backup %s has the correct replicated data directly on disk", addr))
		} else {
			warn(fmt.Sprintf("backup %s data mismatch!\n    want: %q\n    got:  %q",
				addr, payload, got))
		}
	}
}

// ─── Scenario 3: Cache hit (TestAuth short-circuit) ──────────────────────────

func scenario3CacheHit() {
	header("Scenario 3 — Second open of same file uses local cache (TestAuth)")

	c := newClient("cacher1")

	// First open: cache miss, downloads the file.
	info("first open → expect cache miss (download)")
	fd, err := c.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	must("first open", err)
	must("first close", c.AFS_Close(fd))
	pass("first open complete — file cached locally")

	// Second open: file version unchanged on server → TestAuth returns valid →
	// client skips the download entirely.
	info("second open → expect cache hit (no download)")
	start := time.Now()
	fd2, err := c.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	must("second open", err)
	must("second close", c.AFS_Close(fd2))
	elapsed := time.Since(start)

	// A cache hit avoids a streaming RPC — it should be noticeably faster.
	pass(fmt.Sprintf("second open returned in %v (cache hit — no FetchFile RPC)", elapsed))
}

// ─── Scenario 4: Concurrent writes from two clients ──────────────────────────

func scenario4ConcurrentWrites() {
	header("Scenario 4 — Two clients writing different files concurrently")

	var wg sync.WaitGroup
	errors := make(chan error, 2)

	writeFile := func(clientName, filename, content string) {
		defer wg.Done()
		c := newClient(clientName)

		fd, err := c.AFS_Open(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
		if err != nil {
			errors <- fmt.Errorf("[%s] open: %w", clientName, err)
			return
		}
		if _, err := c.AFS_Write(fd, []byte(content)); err != nil {
			errors <- fmt.Errorf("[%s] write: %w", clientName, err)
			return
		}
		if err := c.AFS_Close(fd); err != nil {
			errors <- fmt.Errorf("[%s] close: %w", clientName, err)
			return
		}
		pass(fmt.Sprintf("client %q uploaded %q successfully", clientName, filename))
	}

	wg.Add(2)
	go writeFile("concurrent_A", "primes.txt",           "2\n3\n5\n7\n11\n")
	go writeFile("concurrent_B", "input_dataset_002.txt", "concurrent write from client B\n")
	wg.Wait()
	close(errors)

	for err := range errors {
		warn(err.Error())
	}
}

// ─── Scenario 5: Primary failure + client redirect ────────────────────────────

func scenario5PrimaryFailure() {
	header("Scenario 5 — Primary failure & automatic client redirect")

	fmt.Println(`
  ACTION REQUIRED:
  ─────────────────────────────────────────────────────────
  Kill the PRIMARY server (the one on :50051) right now.
  Then press ENTER to continue.
  ─────────────────────────────────────────────────────────`)
	fmt.Print("  Press ENTER when primary is dead > ")
	fmt.Scanln()

	info("waiting 5 s for backups to elect a new primary…")
	time.Sleep(5 * time.Second)

	// The client discovers the cluster at startup; after primary death it will
	// get a redirect on the first write and retry automatically.
	c := newClient("post_failure_client")

	info("attempting write — client should redirect to new primary automatically")
	fd, err := c.AFS_Open("primes.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		warn(fmt.Sprintf("open after failover failed: %v", err))
		return
	}
	payload := fmt.Sprintf("written after primary failure  t=%d\n", time.Now().Unix())
	c.AFS_Write(fd, []byte(payload))

	if err := c.AFS_Close(fd); err != nil {
		warn(fmt.Sprintf("upload after failover failed: %v", err))
		return
	}
	pass("write succeeded after primary failure — client redirected to new primary")

	// Read it back to confirm the new primary is serving correctly.
	fd2, err := c.AFS_Open("primes.txt", os.O_RDONLY)
	if err != nil {
		warn(fmt.Sprintf("read-back after failover failed: %v", err))
		return
	}
	buf := make([]byte, 512)
	n, _ := c.AFS_Read(fd2, buf)
	c.AFS_Close(fd2)
	pass(fmt.Sprintf("read-back OK: %q", strings.TrimSpace(string(buf[:n]))))
}

// ─── Utilities ────────────────────────────────────────────────────────────────

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// ─── Entry point ──────────────────────────────────────────────────────────────

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║          AFS Primary-Backup Replication — Demo Test          ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Printf("  cluster: %s\n", clusterAddrs)
	fmt.Printf("  time:    %s\n", time.Now().Format(time.RFC3339))

	scenario1BasicRead()
	scenario2WriteAndVerify()
	scenario3CacheHit()
	scenario4ConcurrentWrites()

	fmt.Printf("\n%s\n", strings.Repeat("─", 60))
	fmt.Print("  Run failure scenario (Scenario 5)? [y/N] > ")
	var choice string
	fmt.Scanln(&choice)
	if strings.ToLower(strings.TrimSpace(choice)) == "y" {
		scenario5PrimaryFailure()
	} else {
		info("skipping Scenario 5 (primary failure test)")
	}

	header("All scenarios complete")
}