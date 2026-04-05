package coordinator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"primality_afs/client"
	primepb "primality_afs/prime_proto"
)

const (
	chunkSize    = 10000
	snapshotFile = "snapshot_latest.json"
	rpcTimeout   = 30 * time.Second
	snapInterval = 200 * time.Millisecond
	maxRetries   = 3

	// Worker recovery constants.
	// pingInterval is how long the proxy waits between ping attempts while
	// a worker is down.
	pingInterval = 200 * time.Millisecond
	// workerDeadTimeout is how long a worker is allowed to stay unreachable
	// before the proxy goroutine gives up entirely and removes it from the pool.
	workerDeadTimeout = 2 * time.Minute
)

type Stats struct {
	InputFiles   int
	TotalNumbers int
	PrimesFound  int
	Workers      int
	Duration     time.Duration
}

type Job struct {
	Chunk   *primepb.WorkChunk
	Retries int
}

type GlobalSnapshot struct {
	Timestamp       time.Time           `json:"timestamp"`
	TotalPrimes     int                 `json:"total_primes_found"`
	NextChunkID     int                 `json:"next_chunk_id"`
	CompletedRanges []ChunkRange        `json:"completed_ranges"`
	FileChunkMap    map[string][]int32  `json:"file_chunk_map"`
	Workers         map[int]WorkerState `json:"worker_states"`
}

type ChunkRange struct {
	Lo int32 `json:"lo"`
	Hi int32 `json:"hi"`
}

type WorkerState struct {
	ChunkID int32 `json:"chunk_id"`
	Offset  int32 `json:"offset"`
}

func compactRanges(ids []int32) []ChunkRange {
	if len(ids) == 0 {
		return nil
	}
	sorted := make([]int32, len(ids))
	copy(sorted, ids)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	ranges := []ChunkRange{{Lo: sorted[0], Hi: sorted[0]}}
	for _, id := range sorted[1:] {
		if id == ranges[len(ranges)-1].Hi+1 {
			ranges[len(ranges)-1].Hi = id
		} else {
			ranges = append(ranges, ChunkRange{Lo: id, Hi: id})
		}
	}
	return ranges
}

func isCompleted(id int32, ranges []ChunkRange) bool {
	for _, r := range ranges {
		if id >= r.Lo && id <= r.Hi {
			return true
		}
	}
	return false
}

func takeSnapshot(
	afsClient *client.AFSClient,
	workerClients []primepb.PrimeWorkerClient,
	outputFile string,
	primeSet map[uint64]struct{},
	unflushedPrimes *[]uint64,
	primeMu *sync.Mutex,
	completedMu *sync.Mutex,
	completedIDs *[]int32,
	fileChunkMap map[string][]int32,
	nextChunkID *int,
) {
	log.Println("[SNAPSHOT] Initiating Chandy-Lamport Global Snapshot...")

	primeMu.Lock()
	totalPrimes := len(primeSet)
	batchToWrite := make([]uint64, len(*unflushedPrimes))
	copy(batchToWrite, *unflushedPrimes)
	primeMu.Unlock()

	if len(batchToWrite) > 0 {
		sort.Slice(batchToWrite, func(i, j int) bool { return batchToWrite[i] < batchToWrite[j] })
		var out bytes.Buffer
		for _, p := range batchToWrite {
			out.WriteString(fmt.Sprintf("%d\n", p))
		}

		wfd, werr := afsClient.AFS_Open(outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
		if werr == nil {
			afsClient.AFS_Write(wfd, out.Bytes())
			afsClient.AFS_Close(wfd)

			primeMu.Lock()
			*unflushedPrimes = (*unflushedPrimes)[len(batchToWrite):]
			primeMu.Unlock()

			log.Printf("[SNAPSHOT] Appended %d NEW primes to %s", len(batchToWrite), outputFile)
		} else {
			log.Printf("[SNAPSHOT] WARNING: failed to append to %s: %v (primes retained in buffer)", outputFile, werr)
		}
	}

	snap := GlobalSnapshot{
		Timestamp:    time.Now(),
		TotalPrimes:  totalPrimes,
		NextChunkID:  *nextChunkID,
		Workers:      make(map[int]WorkerState),
		FileChunkMap: fileChunkMap,
	}

	completedMu.Lock()
	snap.CompletedRanges = compactRanges(*completedIDs)
	completedMu.Unlock()

	for i, wc := range workerClients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := wc.CaptureState(ctx, &primepb.SnapshotRequest{Marker: "snap"})
		cancel()
		if err == nil && resp.CurrentChunkId != -1 {
			snap.Workers[i] = WorkerState{
				ChunkID: resp.CurrentChunkId,
				Offset:  resp.Offset,
			}
		}
	}

	jsonBytes, _ := json.MarshalIndent(snap, "", "  ")
	sfd, serr := afsClient.AFS_Open(snapshotFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if serr == nil {
		afsClient.AFS_Write(sfd, jsonBytes)
		afsClient.AFS_Close(sfd)
		log.Printf("[SNAPSHOT] Saved (total_primes=%d, ranges=%d)", snap.TotalPrimes, len(snap.CompletedRanges))
	} else {
		log.Printf("[SNAPSHOT] WARNING: failed to write snapshot JSON: %v", serr)
	}
}

func loadSnapshot(afsClient *client.AFSClient) (*GlobalSnapshot, error) {
	fd, err := afsClient.AFS_Open(snapshotFile, os.O_RDONLY)
	if err != nil {
		return nil, nil
	}
	defer afsClient.AFS_Close(fd)

	var buf bytes.Buffer
	slab := make([]byte, 64*1024)
	for {
		n, readErr := afsClient.AFS_Read(fd, slab)
		if n > 0 {
			buf.Write(slab[:n])
		}
		if readErr != nil {
			break
		}
	}

	var snap GlobalSnapshot
	if err := json.Unmarshal(buf.Bytes(), &snap); err != nil {
		return nil, fmt.Errorf("corrupt snapshot: %w", err)
	}
	return &snap, nil
}

func rebuildPrimeSet(afsClient *client.AFSClient, outputFile string) map[uint64]struct{} {
	set := make(map[uint64]struct{})
	fd, err := afsClient.AFS_Open(outputFile, os.O_RDONLY)
	if err != nil {
		return set
	}
	defer afsClient.AFS_Close(fd)

	var buf bytes.Buffer
	slab := make([]byte, 64*1024)
	for {
		n, readErr := afsClient.AFS_Read(fd, slab)
		if n > 0 {
			buf.Write(slab[:n])
		}
		if readErr != nil {
			break
		}
	}

	sc := bufio.NewScanner(&buf)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			if v, err := strconv.ParseUint(line, 10, 64); err == nil {
				set[v] = struct{}{}
			}
		}
	}

	if err := sc.Err(); err != nil {
		log.Printf("[coordinator] WARNING: error reading existing primes.txt: %v", err)
	}

	return set
}

// waitForWorkerRecovery pings the worker with CaptureState every pingInterval
// until the worker responds successfully or workerDeadTimeout elapses.
//
// Returns true  → worker is alive again, proxy can resume processing jobs.
// Returns false → worker never came back within the timeout, proxy should exit.
//
// We reuse CaptureState as the ping because it already exists in the proto and
// is a lightweight no-op when the worker is idle. No proto changes needed.
func waitForWorkerRecovery(workerClient primepb.PrimeWorkerClient, workerID int) bool {
	log.Printf("[worker %d] entering recovery wait (timeout=%s, ping every %s)",
		workerID, workerDeadTimeout, pingInterval)

	deadline := time.Now().Add(workerDeadTimeout)

	for time.Now().Before(deadline) {
		time.Sleep(pingInterval)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := workerClient.CaptureState(ctx, &primepb.SnapshotRequest{Marker: "ping"})
		cancel()

		if err == nil {
			log.Printf("[worker %d] is back online — resuming job processing", workerID)
			return true
		}

		remaining := time.Until(deadline).Round(time.Second)
		log.Printf("[worker %d] still unreachable (%v) — will retry for %s more",
			workerID, err, remaining)
	}

	log.Printf("[worker %d] did not recover within %s — removing from pool", workerID, workerDeadTimeout)
	return false
}

// runWorkerProxy is the goroutine that represents one worker in the coordinator.
//
// OLD behaviour (one sentence): on any RPC error it requeued the job, called
// break, and the goroutine exited forever — the worker slot was permanently lost.
//
// NEW behaviour: on RPC error the job is requeued and the goroutine calls
// waitForWorkerRecovery instead of breaking. If the worker comes back the
// goroutine resumes pulling jobs normally. If the worker never comes back
// within workerDeadTimeout the goroutine exits cleanly, shrinking the pool
// by one without deadlocking the rest of the system.
//
// The activeWorkers counter is decremented whenever the goroutine exits so
// the coordinator can detect the "all workers permanently dead" case and
// surface it quickly instead of blocking on chunkWg.Wait() forever.
func runWorkerProxy(
	workerClient primepb.PrimeWorkerClient,
	workerID int,
	jobs <-chan Job,
	requeueCh chan<- Job,
	results chan<- *primepb.PrimeResult,
	chunkWg *sync.WaitGroup,
	proxyWg *sync.WaitGroup,
	activeWorkers *atomic.Int32,
) {
	defer proxyWg.Done()
	defer activeWorkers.Add(-1)

	for job := range jobs {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := workerClient.ProcessChunk(ctx, job.Chunk)
		cancel()

		if err == nil {
			// Happy path — forward result and move on.
			results <- resp
			chunkWg.Done()
			continue
		}

		// ── RPC failed ───────────────────────────────────────────────────────
		//
		// 1. Requeue the job immediately so another alive worker can pick it up
		//    while this one is in recovery. Retries counter is intentionally NOT
		//    incremented here — the retry limit is for permanent chunk failures,
		//    not transient worker deaths.
		log.Printf("[worker %d] RPC error on chunk %d: %v — requeuing and entering recovery",
			workerID, job.Chunk.Id, err)
		requeueCh <- job

		// 2. Wait until the worker is reachable again (or give up after timeout).
		recovered := waitForWorkerRecovery(workerClient, workerID)
		if !recovered {
			// Worker is permanently gone. Exit the goroutine cleanly.
			// The jobs channel stays open so remaining alive proxies keep working.
			return
		}

		// 3. Worker is back. Loop continues — next iteration pulls the next job.
		// The requeued job from step 1 will be picked up by whoever is first
		// (could be this goroutine or any other alive proxy).
	}
}

func Run(afsAddrs string, cacheDir string, inputFiles []string, outputFile string, workerAddrs []string, recoverMode bool) (Stats, error) {
	startTime := time.Now()

	afsClient, err := client.InitAFS(afsAddrs, cacheDir)
	if err != nil {
		return Stats{}, fmt.Errorf("AFS connect: %w", err)
	}

	var completedRanges []ChunkRange
	fileChunkMap := make(map[string][]int32)
	startChunkID := 0

	primeSet := make(map[uint64]struct{})
	var unflushedPrimes []uint64

	if recoverMode {
		snap, snapErr := loadSnapshot(afsClient)
		if snapErr == nil && snap != nil {
			log.Printf("[coordinator] RECOVERY: snapshot from %s", snap.Timestamp)
			completedRanges = snap.CompletedRanges
			fileChunkMap = snap.FileChunkMap
			startChunkID = snap.NextChunkID
			primeSet = rebuildPrimeSet(afsClient, outputFile)
		}
	}

	var conns []*grpc.ClientConn
	var grpcClients []primepb.PrimeWorkerClient

	for _, addr := range workerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			conns = append(conns, conn)
			grpcClients = append(grpcClients, primepb.NewPrimeWorkerClient(conn))
		}
	}
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	numWorkers := len(grpcClients)

	if numWorkers == 0 {
		return Stats{}, fmt.Errorf("no workers available")
	}

	jobs := make(chan Job, numWorkers*4)
	requeueCh := make(chan Job, numWorkers*4)
	results := make(chan *primepb.PrimeResult, numWorkers*1000)

	var proxyWg, chunkWg, ingestWg, forwarderWg sync.WaitGroup
	var primeMu, completedMu, snapshotMutex sync.Mutex

	// activeWorkers tracks how many proxy goroutines are still alive.
	// When it hits zero while chunkWg is still > 0, all workers are permanently
	// dead and we will never finish — we detect this below and bail out.
	var activeWorkers atomic.Int32
	activeWorkers.Store(int32(numWorkers))

	completedIDs := make([]int32, 0, 1024)
	for _, r := range completedRanges {
		for id := r.Lo; id <= r.Hi; id++ {
			completedIDs = append(completedIDs, id)
		}
	}

	nextChunkID := startChunkID
	snapCtx, snapCancel := context.WithCancel(context.Background())
	defer snapCancel()

	ingestWg.Add(1)
	go func() {
		defer ingestWg.Done()
		for resp := range results {
			snapshotMutex.Lock()

			primeMu.Lock()
			for _, p := range resp.Primes {
				if _, exists := primeSet[p]; !exists {
					primeSet[p] = struct{}{}
					unflushedPrimes = append(unflushedPrimes, p)
				}
			}
			primeMu.Unlock()

			completedMu.Lock()
			completedIDs = append(completedIDs, resp.ChunkId)
			completedMu.Unlock()

			snapshotMutex.Unlock()
		}
	}()

	forwarderWg.Add(1)
	go func() {
		defer forwarderWg.Done()
		for job := range requeueCh {
			jobs <- job
		}
	}()

	go func() {
		ticker := time.NewTicker(snapInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				snapshotMutex.Lock()
				takeSnapshot(
					afsClient, grpcClients, outputFile,
					primeSet, &unflushedPrimes, &primeMu,
					&completedMu, &completedIDs,
					fileChunkMap, &nextChunkID,
				)
				snapshotMutex.Unlock()
			case <-snapCtx.Done():
				return
			}
		}
	}()

	// Launch one proxy goroutine per worker.
	// Each goroutine now survives individual RPC failures and re-joins the job
	// pool automatically when its worker recovers (see runWorkerProxy).
	for i, c := range grpcClients {
		proxyWg.Add(1)
		go runWorkerProxy(c, i+1, jobs, requeueCh, results, &chunkWg, &proxyWg, &activeWorkers)
	}

	// Watchdog: polls every second. If all proxy goroutines have exited while
	// there is still pending work, every worker is permanently dead and we
	// will deadlock on chunkWg.Wait(). Detect this early and fatal out with
	// a clear message so the operator knows to restart workers and re-run
	// with --recover.
	watchdogDone := make(chan struct{})
	go func() {
		defer close(watchdogDone)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if activeWorkers.Load() == 0 {
					// All proxies are gone. Check if work is still pending by
					// inspecting the jobs channel. If it is non-empty we are
					// stuck — no one can drain it.
					if len(jobs) > 0 {
						log.Fatalf("[coordinator] ALL workers permanently dead with %d chunks still pending — "+
							"restart workers and re-run with --recover", len(jobs))
					}
					return
				}
			case <-snapCtx.Done():
				return
			}
		}
	}()

	totalNumbers := 0
	chunkID := 0

	for _, filename := range inputFiles {
		fd, err := afsClient.AFS_Open(filename, os.O_RDONLY)
		if err != nil {
			log.Printf("[coordinator] WARNING: failed to open %s on AFS: %v", filename, err)
			continue
		}

		var buf bytes.Buffer
		slab := make([]byte, 64*1024)
		for {
			n, readErr := afsClient.AFS_Read(fd, slab)
			if n > 0 {
				buf.Write(slab[:n])
			}
			if readErr != nil {
				break
			}
		}
		afsClient.AFS_Close(fd)

		scanner := bufio.NewScanner(&buf)
		var current []uint64

		flush := func() {
			if len(current) == 0 {
				return
			}
			cid := int32(chunkID)
			chunkID++
			nextChunkID = chunkID

			totalNumbers += len(current)

			if isCompleted(cid, completedRanges) {
				current = nil
				return
			}

			if fileChunkMap[filename] == nil {
				fileChunkMap[filename] = []int32{}
			}
			fileChunkMap[filename] = append(fileChunkMap[filename], cid)

			chunkWg.Add(1)
			jobs <- Job{Chunk: &primepb.WorkChunk{Id: cid, Values: current}, Retries: 0}
			current = nil
		}

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			n, parseErr := strconv.ParseUint(line, 10, 64)
			if parseErr != nil {
				continue
			}
			current = append(current, n)

			if len(current) >= chunkSize {
				flush()
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("[coordinator] FATAL: scanner error reading %s: %v", filename, err)
		}
		flush()
	}

	chunkWg.Wait()
	snapCancel()
	<-watchdogDone   // let the watchdog goroutine exit cleanly
	close(requeueCh)
	forwarderWg.Wait()
	close(jobs)
	proxyWg.Wait()
	close(results)
	ingestWg.Wait()

	snapshotMutex.Lock()
	takeSnapshot(
		afsClient, grpcClients, outputFile,
		primeSet, &unflushedPrimes, &primeMu,
		&completedMu, &completedIDs,
		fileChunkMap, &nextChunkID,
	)
	snapshotMutex.Unlock()

	primeMu.Lock()
	finalCount := len(primeSet)
	primeMu.Unlock()

	return Stats{
		InputFiles:   len(inputFiles),
		TotalNumbers: totalNumbers,
		PrimesFound:  finalCount,
		Workers:      numWorkers,
		Duration:     time.Since(startTime),
	}, nil
}