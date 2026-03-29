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

func must(label string, err error) {
	if err != nil {
		log.Fatalf("FATAL [%s]: %v", label, err)
	}
}

func header(title string) { fmt.Printf("\n--- %s ---\n", title) }
func pass(msg string)     { fmt.Printf("OK: %s\n", msg) }
func info(msg string)     { fmt.Printf("INFO: %s\n", msg) }
func warn(msg string)     { fmt.Printf("WARN: %s\n", msg) }

func newClient(name string) *client.AFSClient {
	cacheDir := fmt.Sprintf("/tmp/afs_%s", name)
	os.RemoveAll(cacheDir)
	c, err := client.InitAFS(clusterAddrs, cacheDir)
	must("InitAFS:"+name, err)
	return c
}

func scenario1BasicRead() {
	header("Scenario 1: Basic read")

	c := newClient("reader1")
	info("client 'reader1' connected")

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

func scenario2WriteAndVerify() {
	header("Scenario 2: Write to primary, verify replication")

	writer := newClient("writer1")
	payload := fmt.Sprintf("replicated-write test timestamp=%d\n", time.Now().Unix())

	fd, err := writer.AFS_Open("primes.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("Open primes.txt for write", err)
	_, err = writer.AFS_Write(fd, []byte(payload))
	must("Write payload", err)
	must("Close file", writer.AFS_Close(fd))
	pass("StoreFile completed")

	time.Sleep(200 * time.Millisecond)

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
			pass(fmt.Sprintf("backup %s verified", addr))
		} else {
			warn(fmt.Sprintf("backup %s mismatch: want %q, got %q", addr, payload, got))
		}
	}
}

func scenario3CacheHit() {
	header("Scenario 3: Cache hit")

	c := newClient("cacher1")

	info("first open (expect download)")
	fd, err := c.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	must("first open", err)
	must("first close", c.AFS_Close(fd))
	pass("first open complete")

	info("second open (expect cache hit)")
	start := time.Now()
	fd2, err := c.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	must("second open", err)
	must("second close", c.AFS_Close(fd2))
	elapsed := time.Since(start)

	pass(fmt.Sprintf("second open took %v", elapsed))
}

func scenario4ConcurrentWrites() {
	header("Scenario 4: Concurrent writes")

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
		pass(fmt.Sprintf("client %q uploaded %q", clientName, filename))
	}

	wg.Add(2)
	go writeFile("concurrent_A", "primes.txt", "2\n3\n5\n7\n11\n")
	go writeFile("concurrent_B", "input_dataset_002.txt", "concurrent write from client B\n")
	wg.Wait()
	close(errors)

	for err := range errors {
		warn(err.Error())
	}
}

func scenario5ServerCrashDuringRead() {
	header("Scenario 5: Server crash during read")

	c1 := newClient("setup_client")

	largeData := make([]byte, 100*1024*1024)
	for i := range largeData {
		largeData[i] = 'A' + byte(i%26)
	}
	info("uploading a 100MB test file...")
	fd, err := c1.AFS_Open("large_read_test.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("open large_read_test.txt", err)
	c1.AFS_Write(fd, largeData)
	must("close large file", c1.AFS_Close(fd))
	pass("100MB file uploaded.")

	fmt.Println("\nACTION REQUIRED:")
	fmt.Println("1. Go to your PRIMARY server terminal (:50051).")
	fmt.Println("2. Get ready to hit Ctrl+C.")
	fmt.Println("3. Come back here, press ENTER.")
	fmt.Println("4. IMMEDIATELY switch back and hit Ctrl+C!")
	fmt.Print("Press ENTER to start the download loop > ")
	fmt.Scanln()

	successAfterFailover := false
	info("starting continuous read loop. KILL THE PRIMARY NOW!")

	for i := 0; i < 100; i++ {
		fmt.Printf("downloading... (attempt %d/100)\r", i+1)

		start := time.Now()

		c2 := newClient(fmt.Sprintf("reader_loop_%d", i))
		fd2, err := c2.AFS_Open("large_read_test.txt", os.O_RDONLY)

		if err != nil {
			warn(fmt.Sprintf("\nread failed: %v", err))
			break
		}

		duration := time.Since(start)
		c2.AFS_Close(fd2)

		if duration > 1*time.Second {
			fmt.Printf("\n")
			pass(fmt.Sprintf("Failover triggered and handled mid-read. Took %v", duration))
			successAfterFailover = true
			break
		}
	}
	fmt.Printf("\n")

	if !successAfterFailover {
		warn("Loop finished before failover was detected.")
	}
}

func scenario6ClientCrashDuringWrite() {
	header("Scenario 6: Client crash during write")

	c1 := newClient("doomed_client")
	info("doomed_client opening 'partial_test.txt'...")

	fd, err := c1.AFS_Open("partial_test.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	must("open partial_test.txt", err)

	info("writing data to local cache...")
	c1.AFS_Write(fd, []byte("This is a partial write. The client will crash before closing."))

	info("SIMULATING CLIENT CRASH... (abandoning without calling AFS_Close)")

	os.RemoveAll("/tmp/afs_doomed_client")

	info("verifying server does NOT have the partial file...")

	time.Sleep(500 * time.Millisecond)

	c2 := newClient("verifier_client")
	_, err = c2.AFS_Open("partial_test.txt", os.O_RDONLY)
	if err != nil {
		pass(fmt.Sprintf("SUCCESS: File does not exist on server (Error: %v)", err))
	} else {
		warn("FAILURE: File exists on the server.")
	}
}

func scenario7PrimaryFailure() {
	header("Scenario 7: Primary failure & client redirect")

	fmt.Println("\nACTION REQUIRED:")
	fmt.Println("If you haven't already, kill the PRIMARY server (:50051).")
	fmt.Println("Ensure only the backups are running.")
	fmt.Print("Press ENTER when primary is dead > ")
	fmt.Scanln()

	info("waiting 5s for backups to elect a new primary...")
	time.Sleep(5 * time.Second)

	c := newClient("post_failure_client")

	info("attempting write...")
	fd, err := c.AFS_Open("primes.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		warn(fmt.Sprintf("open after failover failed: %v", err))
		return
	}
	payload := fmt.Sprintf("written after primary failure t=%d\n", time.Now().Unix())
	c.AFS_Write(fd, []byte(payload))

	if err := c.AFS_Close(fd); err != nil {
		warn(fmt.Sprintf("upload after failover failed: %v", err))
		return
	}
	pass("write succeeded after primary failure")

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

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func main() {
	fmt.Println("AFS Primary-Backup Replication - Demo Test")
	fmt.Printf("Cluster: %s\n", clusterAddrs)
	fmt.Printf("Time: %s\n", time.Now().Format(time.RFC3339))

	scenario1BasicRead()
	scenario2WriteAndVerify()
	scenario3CacheHit()
	scenario4ConcurrentWrites()

	fmt.Println("\n------------------------------------------------------------")
	fmt.Print("Run crash scenarios (Scenarios 5, 6 & 7)? [y/N] > ")
	var choice string
	fmt.Scanln(&choice)
	if strings.ToLower(strings.TrimSpace(choice)) == "y" {
		scenario5ServerCrashDuringRead()
		scenario6ClientCrashDuringWrite()
		scenario7PrimaryFailure()
	} else {
		info("skipping crash scenarios")
	}

	header("All scenarios complete")
}