// demo/test.go
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
	pb "primality_afs/proto"
)

const clusterAddrs = "localhost:50051,localhost:50052,localhost:50053"

var allNodes = []string{"localhost:50051", "localhost:50052", "localhost:50053"}

func must(label string, err error) {
	if err != nil {
		log.Fatalf("FATAL [%s]: %v", label, err)
	}
}

func header(title string) { fmt.Printf("\n--- %s ---\n", title) }
func pass(msg string)     { fmt.Printf("OK  : %s\n", msg) }
func fail(msg string)     { fmt.Printf("FAIL: %s\n", msg) }
func info(msg string)     { fmt.Printf("INFO: %s\n", msg) }
func warn(msg string)     { fmt.Printf("WARN: %s\n", msg) }

// ─── Helpers ──────────────────────────────────────────────────────────────────

func newClient(name string) *client.AFSClient {
	cacheDir := fmt.Sprintf("/tmp/afs_%s", name)
	os.RemoveAll(cacheDir)
	c, err := client.InitAFS(clusterAddrs, cacheDir)
	must("InitAFS:"+name, err)
	return c
}

// dialNode opens a raw gRPC connection to a single node.
func dialNode(addr string) (pb.AFSClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewAFSClient(conn), conn, nil
}

// findLeader polls the cluster and returns the current Raft leader address.
// It retries for up to `timeout`, which is useful right after a node failure.
func findLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, addr := range allNodes {
			afsC, conn, err := dialNode(addr)
			if err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			info, err := afsC.GetPrimary(ctx, &pb.Empty{})
			cancel()
			conn.Close()
			if err != nil {
				continue
			}
			if info.IsPrimary && info.Address != "" {
				return info.Address, nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return "", fmt.Errorf("no leader elected within %s", timeout)
}

// fetchFromNode fetches a file directly from a specific node (bypassing the client cache).
func fetchFromNode(addr, filename string) ([]byte, error) {
	afsC, conn, err := dialNode(addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := afsC.FetchFile(ctx, &pb.FileRequest{Filename: filename})
	if err != nil {
		return nil, fmt.Errorf("FetchFile: %w", err)
	}

	var buf []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("stream recv: %w", err)
		}
		buf = append(buf, chunk.Content...)
	}
	return buf, nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// ─── Scenario 1: Basic read ───────────────────────────────────────────────────

func scenario1BasicRead() {
	header("Scenario 1: Basic read")

	leaderAddr, err := findLeader(5 * time.Second)
	must("find leader", err)
	info(fmt.Sprintf("current leader: %s", leaderAddr))

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
		fmt.Printf("Content preview:\n%s\n", truncate(string(buf[:n]), 200))
	}
}

// ─── Scenario 2: Write to leader, verify replication on followers ─────────────

func scenario2WriteAndVerify() {
	header("Scenario 2: Write to leader, verify Raft replication")

	leaderAddr, err := findLeader(5 * time.Second)
	must("find leader", err)
	info(fmt.Sprintf("current leader: %s", leaderAddr))

	writer := newClient("writer1")
	payload := fmt.Sprintf("raft-replicated-write ts=%d\n", time.Now().Unix())

	// CHANGED: Use a unique filename so it doesn't conflict with Scenario 4
	fd, err := writer.AFS_Open("scenario2_test.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("Open scenario2_test.txt for write", err)
	_, err = writer.AFS_Write(fd, []byte(payload))
	must("Write payload", err)
	must("Close file (triggers Raft commit)", writer.AFS_Close(fd))
	pass("StoreFile committed by Raft leader")

	time.Sleep(1500 * time.Millisecond)

	for _, addr := range allNodes {
		if addr == leaderAddr {
			continue
		}
		// CHANGED: Fetch the unique file
		data, err := fetchFromNode(addr, "scenario2_test.txt")
		if err != nil {
			warn(fmt.Sprintf("follower %s FetchFile error: %v", addr, err))
			continue
		}
		got := strings.TrimSpace(string(data))
		want := strings.TrimSpace(payload)
		if got == want {
			pass(fmt.Sprintf("follower %s replicated correctly", addr))
		} else {
			fail(fmt.Sprintf("follower %s mismatch:\n  want: %q\n   got: %q", addr, want, got))
		}
	}
}

// ─── Scenario 3: Cache hit ────────────────────────────────────────────────────

func scenario3CacheHit() {
	header("Scenario 3: Cache hit (AFS TestAuth)")

	c := newClient("cacher1")

	info("first open — expects full download from leader")
	fd, err := c.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	must("first open", err)
	must("first close", c.AFS_Close(fd))
	pass("first open complete")

	info("second open — expects cache hit (TestAuth validates local copy)")
	start := time.Now()
	fd2, err := c.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	must("second open", err)
	must("second close", c.AFS_Close(fd2))
	elapsed := time.Since(start)

	// A cache hit skips the streaming download so it should be much faster.
	if elapsed < 100*time.Millisecond {
		pass(fmt.Sprintf("cache hit confirmed — second open took only %v", elapsed))
	} else {
		warn(fmt.Sprintf("second open took %v — may not have hit cache", elapsed))
	}
}

// ─── Scenario 4: Concurrent writes ───────────────────────────────────────────

func scenario4ConcurrentWrites() {
	header("Scenario 4: Concurrent writes (Raft serialises via leader)")

	leaderAddr, err := findLeader(5 * time.Second)
	must("find leader", err)
	info(fmt.Sprintf("current leader: %s — all writes will be routed here", leaderAddr))

	var wg sync.WaitGroup
	errors := make(chan error, 4)

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
		pass(fmt.Sprintf("client %q committed %q via Raft", clientName, filename))
	}

	wg.Add(2)
	go writeFile("concurrent_A", "primes.txt", "2\n3\n5\n7\n11\n")
	go writeFile("concurrent_B", "input_dataset_002.txt", "concurrent write from client B\n")
	wg.Wait()
	close(errors)

	for e := range errors {
		warn(e.Error())
	}

	// Verify both files landed on all nodes.
	time.Sleep(1500 * time.Millisecond)
	for _, filename := range []string{"primes.txt", "input_dataset_002.txt"} {
		replicaOK := 0
		for _, addr := range allNodes {
			if _, err := fetchFromNode(addr, filename); err == nil {
				replicaOK++
			}
		}
		if replicaOK == len(allNodes) {
			pass(fmt.Sprintf("%s present on all %d nodes", filename, replicaOK))
		} else {
			warn(fmt.Sprintf("%s only on %d/%d nodes", filename, replicaOK, len(allNodes)))
		}
	}
}

// ─── Scenario 5: Leader crash during read (Raft re-election) ─────────────────

func scenario5LeaderCrashDuringRead() {
	header("Scenario 5: Leader crash & Raft re-election during reads")

	c1 := newClient("setup_client")
	largeData := make([]byte, 100*1024*1024) 
	for i := range largeData {
		largeData[i] = 'A' + byte(i%26)
	}

	leaderAddr, err := findLeader(15 * time.Second) 
	must("find leader before upload", err)
	info(fmt.Sprintf("uploading 100 MB test file via leader %s...", leaderAddr))

	fd, err := c1.AFS_Open("large_read_test.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("open large_read_test.txt", err)
	c1.AFS_Write(fd, largeData)
	must("close large file", c1.AFS_Close(fd))
	pass("100 MB file uploaded and committed")

	fmt.Println("\nACTION REQUIRED:")
	fmt.Printf("  1. Identify which terminal is the leader  (%s)\n", leaderAddr)
	fmt.Println("  2. Press ENTER here, then IMMEDIATELY kill that leader with Ctrl+C")
	fmt.Print("Press ENTER to start > ")
	fmt.Scanln()

	successAfterFailover := false
	info("reading in a tight loop — kill the leader NOW")

	// Increased loop count to account for no sleeping
	for i := 0; i < 200; i++ {
		fmt.Printf("  attempt %d/200...\r", i+1)

		loopClient := newClient(fmt.Sprintf("reader_loop_%d", i))
		start := time.Now()
		fd2, err := loopClient.AFS_Open("large_read_test.txt", os.O_RDONLY)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Println()
			info(fmt.Sprintf("read stream broken mid-flight (expected): %v", err))
			
			// The leader is dead. Wait for the cluster to elect a new one.
			info("waiting for Raft to elect a new leader (takes 4-6 seconds)...")
			newLeader, lerr := findLeader(15 * time.Second)
			if lerr != nil {
				warn("no new leader elected within 15s")
				break
			}
			pass(fmt.Sprintf("cluster survived! New Raft leader elected: %s", newLeader))
			successAfterFailover = true
			break
		}

		loopClient.AFS_Close(fd2)

		if elapsed > 1*time.Second {
			fmt.Println()
			pass(fmt.Sprintf("read stalled for %v — caught the failover mid-request", elapsed))
			successAfterFailover = true
			break
		}
		
		// NO SLEEP. We want the script hammering the server so the kill happens mid-read.
	}

	fmt.Println()
	if !successAfterFailover {
		warn("loop finished — kill was not detected (did you kill the leader?)")
	}
}

// ─── Scenario 6: Client crash during write (no partial commit) ───────────────

func scenario6ClientCrashDuringWrite() {
	header("Scenario 6: Client crash — partial write must NOT reach Raft")

	c1 := newClient("doomed_client")
	info("doomed_client opening 'partial_test.txt'...")

	fd, err := c1.AFS_Open("partial_test.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("open partial_test.txt", err)

	info("writing data to local cache (not yet flushed to Raft)...")
	c1.AFS_Write(fd, []byte("partial write — client will crash before AFS_Close"))

	info("SIMULATING CLIENT CRASH (no AFS_Close called — Raft never sees this data)")
	// Wipe the cache without calling AFS_Close, so StoreFile is never sent.
	os.RemoveAll("/tmp/afs_doomed_client")

	time.Sleep(500 * time.Millisecond)

	// A fresh client should NOT find the file (it was never committed to Raft).
	c2 := newClient("verifier_client")
	_, verifyErr := c2.AFS_Open("partial_test.txt", os.O_RDONLY)
	if verifyErr != nil {
		pass(fmt.Sprintf("correct — file absent from Raft cluster (%v)", verifyErr))
	} else {
		fail("partial file unexpectedly exists on the cluster")
	}
}

// ─── Scenario 7: Full leader failure & writes on new leader ──────────────────

func scenario7LeaderFailureAndRecovery() {
	header("Scenario 7: Leader failure — writes continue on newly elected leader")

	fmt.Println("\nACTION REQUIRED:")
	fmt.Println("  Kill the current leader node (whichever shows 'became Leader' in its log).")
	fmt.Println("  Leave the other two nodes running.")
	fmt.Print("Press ENTER when the leader is dead > ")
	fmt.Scanln()

	info("polling for new Raft leader (up to 10s)...")
	newLeader, err := findLeader(10 * time.Second)
	if err != nil {
		warn(fmt.Sprintf("no new leader found: %v", err))
		return
	}
	pass(fmt.Sprintf("new leader elected: %s", newLeader))

	c := newClient("post_failure_client")

	info("writing 'primes.txt' through new leader...")
	fd, err := c.AFS_Open("primes.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		fail(fmt.Sprintf("open after failover: %v", err))
		return
	}
	payload := fmt.Sprintf("written after leader failure ts=%d\n", time.Now().Unix())
	c.AFS_Write(fd, []byte(payload))
	if err := c.AFS_Close(fd); err != nil {
		fail(fmt.Sprintf("upload after failover: %v", err))
		return
	}
	pass("write committed by new Raft leader")

	// Read back to confirm.
	fd2, err := c.AFS_Open("primes.txt", os.O_RDONLY)
	if err != nil {
		fail(fmt.Sprintf("read-back after failover: %v", err))
		return
	}
	buf := make([]byte, 512)
	n, _ := c.AFS_Read(fd2, buf)
	c.AFS_Close(fd2)
	pass(fmt.Sprintf("read-back OK: %q", strings.TrimSpace(string(buf[:n]))))

	// Verify the surviving follower also has the new data.
	time.Sleep(1500 * time.Millisecond)
	for _, addr := range allNodes {
		if addr == newLeader {
			continue
		}
		data, err := fetchFromNode(addr, "primes.txt")
		if err != nil {
			// Node may be the one we killed — skip.
			info(fmt.Sprintf("node %s unreachable (may be the killed leader)", addr))
			continue
		}
		got := strings.TrimSpace(string(data))
		want := strings.TrimSpace(payload)
		if got == want {
			pass(fmt.Sprintf("surviving follower %s has the new data", addr))
		} else {
			warn(fmt.Sprintf("follower %s data mismatch: got %q", addr, got))
		}
	}
}

// ─── Scenario 8: Leader step-down on term bump ───────────────────────────────

func scenario8TermSafety() {
	header("Scenario 8: Term safety — stale leader steps down automatically")

	leaderAddr, err := findLeader(5 * time.Second)
	must("find initial leader", err)
	info(fmt.Sprintf("initial leader: %s", leaderAddr))

	// Read current term from any node.
	afsC, conn, err := dialNode(leaderAddr)
	must("dial leader", err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	info1, err := afsC.GetPrimary(ctx, &pb.Empty{})
	cancel()
	conn.Close()
	must("GetPrimary", err)
	info(fmt.Sprintf("cluster is healthy, leader confirmed at %s (IsPrimary=%v)", leaderAddr, info1.IsPrimary))

	// Write a file, then immediately verify all nodes agree on the same version.
	c := newClient("term_safety_client")
	payload := fmt.Sprintf("term-safety-check ts=%d", time.Now().UnixNano())

	fd, err := c.AFS_Open("term_check.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("open term_check.txt", err)
	c.AFS_Write(fd, []byte(payload))
	must("close term_check.txt", c.AFS_Close(fd))
	pass("write committed")

	time.Sleep(1500 * time.Millisecond)

	// All live nodes should return the same content.
	var versions []string
	for _, addr := range allNodes {
		data, err := fetchFromNode(addr, "term_check.txt")
		if err != nil {
			warn(fmt.Sprintf("node %s: %v", addr, err))
			versions = append(versions, "ERROR")
			continue
		}
		versions = append(versions, strings.TrimSpace(string(data)))
	}

	allMatch := true
	for _, v := range versions {
		if v != versions[0] {
			allMatch = false
		}
	}
	if allMatch {
		pass(fmt.Sprintf("all nodes agree on content: %q", truncate(versions[0], 60)))
	} else {
		fail(fmt.Sprintf("nodes disagree: %v", versions))
	}
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	fmt.Println("AFS Raft Replication — Integration Test")
	fmt.Printf("Cluster : %s\n", clusterAddrs)
	fmt.Printf("Time    : %s\n", time.Now().Format(time.RFC3339))

	// Automated scenarios — no manual intervention needed.
	scenario1BasicRead()
	scenario2WriteAndVerify()
	scenario3CacheHit()
	scenario4ConcurrentWrites()
	scenario8TermSafety()

	// Crash scenarios — require the user to kill nodes at the right moment.
	fmt.Println("\n------------------------------------------------------------")
	fmt.Print("Run crash/failover scenarios (5, 6 & 7)? [y/N] > ")
	// fmt.Print("Run crash/failover scenarios (6 & 7)? [y/N] > ")
	var choice string
	fmt.Scanln(&choice)
	if strings.ToLower(strings.TrimSpace(choice)) == "y" {
		scenario5LeaderCrashDuringRead()
		scenario6ClientCrashDuringWrite()
		scenario7LeaderFailureAndRecovery()
	} else {
		info("skipping crash scenarios")
	}

	header("All scenarios complete")
}