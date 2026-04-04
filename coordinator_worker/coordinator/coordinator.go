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
	snapInterval = 1 * time.Second
	maxRetries   = 3 // FIX #4: Limit RPC retries to prevent infinite loops
)

type Stats struct {
	InputFiles   int
	TotalNumbers int
	PrimesFound  int
	Workers      int
	Duration     time.Duration
}

// FIX #4: Wrapper struct to track retries without altering the protobuf
type Job struct {
	Chunk   *primepb.WorkChunk
	Retries int
}

type GlobalSnapshot struct {
	Timestamp       time.Time             `json:"timestamp"`
	TotalPrimes     int                   `json:"total_primes_found"`
	NextChunkID     int                   `json:"next_chunk_id"`
	CompletedRanges []ChunkRange          `json:"completed_ranges"`
	FileChunkMap    map[string][]int32    `json:"file_chunk_map"`
	Workers         map[int]WorkerState   `json:"worker_states"`
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
			
			// FIX #1: Only clear the buffer if the AFS write was actually successful!
			primeMu.Lock()
			*unflushedPrimes = nil 
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
	
	// FIX #7: Scanner error check on rebuild
	if err := sc.Err(); err != nil {
		log.Printf("[coordinator] WARNING: error reading existing primes.txt: %v", err)
	}
	
	return set
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
			// FIX #2 & #8: Removed inFlightOffsets entirely. Idempotent retries are safer.
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
	
	// FIX #3: Silent hang prevention
	if numWorkers == 0 {
		return Stats{}, fmt.Errorf("no workers available")
	}
	
	// FIX #4: Channels now handle Jobs instead of raw chunks
	jobs := make(chan Job, numWorkers*4)
	requeueCh := make(chan Job, numWorkers*4)
	results := make(chan []uint64, numWorkers*1000)

	var proxyWg, chunkWg, ingestWg, forwarderWg sync.WaitGroup
	var primeMu, completedMu sync.Mutex

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
		for primesBatch := range results {
			primeMu.Lock()
			for _, p := range primesBatch {
				if _, exists := primeSet[p]; !exists {
					primeSet[p] = struct{}{}
					unflushedPrimes = append(unflushedPrimes, p)
				}
			}
			primeMu.Unlock()
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
				takeSnapshot(
					afsClient, grpcClients, outputFile,
					primeSet, &unflushedPrimes, &primeMu,
					&completedMu, &completedIDs,
					fileChunkMap, &nextChunkID,
				)
			case <-snapCtx.Done():
				return
			}
		}
	}()

	for i, c := range grpcClients {
		proxyWg.Add(1)
		go func(workerClient primepb.PrimeWorkerClient, id int) {
			defer proxyWg.Done()
			for job := range jobs {
				ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
				resp, err := workerClient.ProcessChunk(ctx, job.Chunk)
				cancel()

				if err != nil {
					// FIX #4: Retry limit enforcement
					if job.Retries >= maxRetries {
						log.Fatalf("[FATAL] chunk %d failed after %d retries: %v", job.Chunk.Id, maxRetries, err)
					}
					log.Printf("[coordinator] RPC error worker %d chunk %d: %v — re-queuing (attempt %d)", id, job.Chunk.Id, err, job.Retries+1)
					job.Retries++
					requeueCh <- job
					continue
				}

				results <- resp.Primes

				completedMu.Lock()
				completedIDs = append(completedIDs, job.Chunk.Id)
				completedMu.Unlock()
				chunkWg.Done()
			}
		}(c, i)
	}

	totalNumbers := 0
	chunkID := startChunkID

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

			// FIX #5 & #6: Do not count numbers or append to map if the chunk is already completed
			if isCompleted(cid, completedRanges) {
				current = nil
				return
			}
			
			// Only update stats for actual work being dispatched
			totalNumbers += len(current)
			
			// Initialize map safely if needed
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
	close(requeueCh)      
	forwarderWg.Wait()    
	close(jobs)           
	proxyWg.Wait()        
	close(results)        
	ingestWg.Wait()       

	takeSnapshot(
		afsClient, grpcClients, outputFile,
		primeSet, &unflushedPrimes, &primeMu,
		&completedMu, &completedIDs,
		fileChunkMap, &nextChunkID,
	)

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