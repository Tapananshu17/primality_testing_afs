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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"primality_afs/client"
	primepb "primality_afs/prime_proto"
	pb "primality_afs/proto"
)


const (
	chunkSize    = 10000
	snapshotFile = "snapshot_latest.json"
	rpcTimeout   = 30 * time.Second
	snapInterval = 100 * time.Millisecond
	maxRetries   = 3

	pingInterval      = 200 * time.Millisecond
	workerDeadTimeout = 2 * time.Minute

	leaderDiscoveryTimeout = 30 * time.Second
	leaderRetryInterval    = 500 * time.Millisecond
	maxWriteRetries        = 5
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


func discoverLeader(addrs []string) (string, error) {
	deadline := time.Now().Add(leaderDiscoveryTimeout)
	attempt := 0

	for time.Now().Before(deadline) {
		addr := addrs[attempt%len(addrs)]
		attempt++

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[coordinator] dial %s for GetPrimary failed: %v", addr, err)
			time.Sleep(leaderRetryInterval)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		info, err := pb.NewAFSClient(conn).GetPrimary(ctx, &pb.Empty{})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("[coordinator] GetPrimary(%s) error: %v", addr, err)
			time.Sleep(leaderRetryInterval)
			continue
		}

		if info.IsPrimary && info.Address != "" {
			return info.Address, nil
		}

		if info.Address != "" {
			conn2, err := grpc.Dial(info.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
				info2, err2 := pb.NewAFSClient(conn2).GetPrimary(ctx2, &pb.Empty{})
				cancel2()
				conn2.Close()

				if err2 == nil && info2.IsPrimary {
					return info.Address, nil
				}
			}
		}
		time.Sleep(leaderRetryInterval)
	}

	return "", fmt.Errorf("could not discover a Raft leader within %s", leaderDiscoveryTimeout)
}

func newAFSClientForLeader(allAddrs string, leaderAddr string, cacheDir string) (*client.AFSClient, error) {
	addrList := leaderAddr
	for _, a := range strings.Split(allAddrs, ",") {
		a = strings.TrimSpace(a)
		if a != "" && a != leaderAddr {
			addrList += "," + a
		}
	}
	return client.InitAFS(addrList, cacheDir)
}

func isLeaderRedirect(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.FailedPrecondition &&
		strings.HasPrefix(st.Message(), "not primary")
}

func splitAddrs(addrs string) []string {
	var result []string
	for _, a := range strings.Split(addrs, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			result = append(result, a)
		}
	}
	return result
}

func writeToAFSWithRetry(allAddrs, cacheDir, outputFile string, flags int, data []byte) error {
	addrSlice := splitAddrs(allAddrs)

	for attempt := 1; attempt <= maxWriteRetries; attempt++ {
		leaderAddr, err := discoverLeader(addrSlice)
		if err != nil {
			return fmt.Errorf("leader discovery on attempt %d: %w", attempt, err)
		}

		afsClient, err := newAFSClientForLeader(allAddrs, leaderAddr, cacheDir)
		if err != nil {
			time.Sleep(leaderRetryInterval)
			continue
		}

		fd, err := afsClient.AFS_Open(outputFile, flags)
		if err != nil {
			if isLeaderRedirect(err) {
				continue
			}
			return fmt.Errorf("open output (attempt %d): %w", attempt, err)
		}

		_, writeErr := afsClient.AFS_Write(fd, data)
		afsClient.AFS_Close(fd)

		if writeErr != nil {
			if isLeaderRedirect(writeErr) {
				continue
			}
			return fmt.Errorf("write output (attempt %d): %w", attempt, writeErr)
		}

		return nil
	}

	return fmt.Errorf("exceeded %d retries writing %s to AFS leader", maxWriteRetries, outputFile)
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
	afsAddrs string, cacheDir string,
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

		werr := writeToAFSWithRetry(afsAddrs, cacheDir, outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, out.Bytes())
		if werr == nil {
			primeMu.Lock()
			*unflushedPrimes = (*unflushedPrimes)[len(batchToWrite):]
			primeMu.Unlock()
			log.Printf("[SNAPSHOT] Appended %d NEW primes to %s", len(batchToWrite), outputFile)
		} else {
			log.Printf("[SNAPSHOT] WARNING: failed to append to %s: %v (primes retained)", outputFile, werr)
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
	serr := writeToAFSWithRetry(afsAddrs, cacheDir, snapshotFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, jsonBytes)
	if serr == nil {
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
	return set
}


func waitForWorkerRecovery(workerClient primepb.PrimeWorkerClient, workerID int) bool {
	deadline := time.Now().Add(workerDeadTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(pingInterval)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := workerClient.CaptureState(ctx, &primepb.SnapshotRequest{Marker: "ping"})
		cancel()

		if err == nil {
			return true
		}
	}
	return false
}

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
			results <- resp
			chunkWg.Done()
			continue
		}

		log.Printf("[worker %d] RPC error on chunk %d: %v — requeuing", workerID, job.Chunk.Id, err)
		requeueCh <- job

		recovered := waitForWorkerRecovery(workerClient, workerID)
		if !recovered {
			return
		}
	}
}


func Run(
	afsAddrs string,
	cacheDir string,
	inputFiles []string,
	outputFile string,
	workerAddrs []string,
	recoverMode bool,
) (Stats, error) {
	startTime := time.Now()

	addrSlice := splitAddrs(afsAddrs)
	if len(addrSlice) == 0 {
		return Stats{}, fmt.Errorf("no AFS addresses provided")
	}

	leaderAddr, err := discoverLeader(addrSlice)
	if err != nil {
		return Stats{}, fmt.Errorf("leader discovery: %w", err)
	}

	log.Printf("[coordinator] connecting to AFS leader: %s", leaderAddr)
	afsClient, err := newAFSClientForLeader(afsAddrs, leaderAddr, cacheDir)
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
					afsAddrs, cacheDir, grpcClients, outputFile,
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

	for i, c := range grpcClients {
		proxyWg.Add(1)
		go runWorkerProxy(c, i+1, jobs, requeueCh, results, &chunkWg, &proxyWg, &activeWorkers)
	}

	watchdogDone := make(chan struct{})
	go func() {
		defer close(watchdogDone)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if activeWorkers.Load() == 0 {
					if len(jobs) > 0 {
						log.Fatalf("[coordinator] ALL workers permanently dead — restart and re-run with --recover")
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
		flush()
	}

	chunkWg.Wait()
	snapCancel()
	<-watchdogDone
	close(requeueCh)
	forwarderWg.Wait()
	close(jobs)
	proxyWg.Wait()
	close(results)
	ingestWg.Wait()

	snapshotMutex.Lock()
	takeSnapshot(
		afsAddrs, cacheDir, grpcClients, outputFile,
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