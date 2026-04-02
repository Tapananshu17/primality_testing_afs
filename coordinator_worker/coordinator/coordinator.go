// coordinator_worker/coordinator/coordinator.go
//
// Task 4 — Coordinator-Worker distributed prime finder.
//
// IsPrime is injected as a function argument so this package has zero
// dependency on the primality logic and can be tested independently.

package coordinator

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"primality_afs/client"
)

// NumWorkers is the default number of parallel goroutine workers.
const NumWorkers = 5

// chunkSize is how many numbers we hand to each worker per job.
const chunkSize = 10_000

// WorkChunk is one unit of work passed from coordinator → worker.
type WorkChunk struct {
	ID     int
	Values []uint64
}

// Stats is the summary returned to the caller after the run completes.
type Stats struct {
	InputFiles   int
	TotalNumbers int
	PrimesFound  int
	Workers      int
	Duration     time.Duration
}

// worker reads chunks from jobs, tests each number, sends primes to results.
func worker(id int, jobs <-chan WorkChunk, results chan<- uint64, wg *sync.WaitGroup, found *atomic.Int64, isPrime func(uint64) bool) {
	defer wg.Done()
	for chunk := range jobs {
		local := 0
		for _, n := range chunk.Values {
			if isPrime(n) {
				results <- n
				local++
			}
		}
		found.Add(int64(local))
		log.Printf("[worker %d] chunk %d — %d numbers, %d primes", id, chunk.ID, len(chunk.Values), local)
	}
	log.Printf("[worker %d] done", id)
}

// Run executes the full pipeline and returns a Stats summary.
//
//	afsAddrs   — comma-separated AFS server addresses (primary first)
//	cacheDir   — local directory used as AFS cache
//	inputFiles — filenames on AFS to read numbers from
//	outputFile — filename on AFS to write sorted unique primes
//	numWorkers — parallel goroutine workers (0 → use NumWorkers default)
//	isPrime    — primality function injected by caller
func Run(
	afsAddrs string,
	cacheDir string,
	inputFiles []string,
	outputFile string,
	numWorkers int,
	isPrime func(uint64) bool,
) (Stats, error) {

	if numWorkers <= 0 {
		numWorkers = NumWorkers
	}
	startTime := time.Now()

	log.Printf("[coordinator] connecting to AFS: %s", afsAddrs)
	afsClient, err := client.InitAFS(afsAddrs, cacheDir)
	if err != nil {
		return Stats{}, fmt.Errorf("AFS connect: %w", err)
	}
	log.Printf("[coordinator] AFS ready")

	jobs := make(chan WorkChunk, numWorkers*4)
	results := make(chan uint64, numWorkers*1000)

	var wg sync.WaitGroup
	var totalFound atomic.Int64

	log.Printf("[coordinator] starting %d workers", numWorkers)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg, &totalFound, isPrime)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	totalNumbers := 0
	chunkID := 0

	for _, filename := range inputFiles {
		log.Printf("[coordinator] fetching: %s", filename)

		fd, err := afsClient.AFS_Open(filename, os.O_RDONLY)
		if err != nil {
			log.Printf("[coordinator] WARNING: cannot open %s — skipping (%v)", filename, err)
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

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			n, parseErr := strconv.ParseUint(line, 10, 64)
			if parseErr != nil {
				log.Printf("[coordinator] bad line %q in %s — skipping", line, filename)
				continue
			}
			current = append(current, n)
			totalNumbers++

			if len(current) >= chunkSize {
				jobs <- WorkChunk{ID: chunkID, Values: current}
				chunkID++
				current = nil
			}
		}
		if len(current) > 0 {
			jobs <- WorkChunk{ID: chunkID, Values: current}
			chunkID++
		}
		log.Printf("[coordinator] %s done — running total %d numbers", filename, totalNumbers)
	}

	close(jobs)
	log.Printf("[coordinator] all input dispatched — %d numbers, %d chunks", totalNumbers, chunkID)

	primeSet := make(map[uint64]struct{})
	for p := range results {
		primeSet[p] = struct{}{}
	}

	uniquePrimes := make([]uint64, 0, len(primeSet))
	for p := range primeSet {
		uniquePrimes = append(uniquePrimes, p)
	}
	sort.Slice(uniquePrimes, func(i, j int) bool { return uniquePrimes[i] < uniquePrimes[j] })

	log.Printf("[coordinator] %d unique primes from %d numbers", len(uniquePrimes), totalNumbers)

	var out bytes.Buffer
	for _, p := range uniquePrimes {
		fmt.Fprintf(&out, "%d\n", p)
	}

	fd, err := afsClient.AFS_Open(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return Stats{}, fmt.Errorf("open output %s: %w", outputFile, err)
	}
	if _, err := afsClient.AFS_Write(fd, out.Bytes()); err != nil {
		afsClient.AFS_Close(fd)
		return Stats{}, fmt.Errorf("write %s: %w", outputFile, err)
	}
	if err := afsClient.AFS_Close(fd); err != nil {
		return Stats{}, fmt.Errorf("upload %s: %w", outputFile, err)
	}
	log.Printf("[coordinator] saved '%s' (%d bytes) to AFS", outputFile, out.Len())

	return Stats{
		InputFiles:   len(inputFiles),
		TotalNumbers: totalNumbers,
		PrimesFound:  len(uniquePrimes),
		Workers:      numWorkers,
		Duration:     time.Since(startTime),
	}, nil
}
