package coordinator

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"primality_afs/client"
	"primality_afs/prime_proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const chunkSize = 10000

type Stats struct {
	InputFiles   int
	TotalNumbers int
	PrimesFound  int
	Workers      int
	Duration     time.Duration
}

func Run(
	afsAddrs string,
	cacheDir string,
	inputFiles []string,
	outputFile string,
	workerAddrs []string,
) (Stats, error) {
	startTime := time.Now()

	log.Printf("[coordinator] connecting to AFS: %s", afsAddrs)
	afsClient, err := client.InitAFS(afsAddrs, cacheDir)
	if err != nil {
		return Stats{}, fmt.Errorf("AFS connect: %w", err)
	}
	log.Printf("[coordinator] AFS ready")

	// Connect to gRPC Workers
	var conns []*grpc.ClientConn
	var grpcClients []primepb.PrimeWorkerClient

	for _, addr := range workerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[coordinator] WARNING: failed to connect to worker %s: %v", addr, err)
			continue
		}
		conns = append(conns, conn)
		grpcClients = append(grpcClients, primepb.NewPrimeWorkerClient(conn))
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

	jobs := make(chan *primepb.WorkChunk, numWorkers*4)
	results := make(chan []uint64, numWorkers*1000)
	var wg sync.WaitGroup

	log.Printf("[coordinator] starting %d proxy clients for gRPC workers", numWorkers)
	for i, c := range grpcClients {
		wg.Add(1)
		go func(workerClient primepb.PrimeWorkerClient, id int) {
			defer wg.Done()
			for chunk := range jobs {
				resp, err := workerClient.ProcessChunk(context.Background(), chunk)
				if err != nil {
					log.Printf("[coordinator] RPC error on worker %d: %v", id, err)
					continue
				}
				results <- resp.Primes
			}
		}(c, i)
	}

	// Wait and close results channel when all proxies are done
	go func() {
		wg.Wait()
		close(results)
	}()

	totalNumbers := 0
	chunkID := 0

	// Read from AFS and stream to the jobs channel
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
			if line == "" { continue }
			
			n, parseErr := strconv.ParseUint(line, 10, 64)
			if parseErr != nil {
				continue
			}
			current = append(current, n)
			totalNumbers++

			if len(current) >= chunkSize {
				jobs <- &primepb.WorkChunk{Id: int32(chunkID), Values: current}
				chunkID++
				current = nil
			}
		}
		if len(current) > 0 {
			jobs <- &primepb.WorkChunk{Id: int32(chunkID), Values: current}
			chunkID++
		}
		log.Printf("[coordinator] %s done", filename)
	}

	close(jobs)
	log.Printf("[coordinator] all input dispatched — %d numbers, %d chunks", totalNumbers, chunkID)

	// Collect unique primes
	primeSet := make(map[uint64]struct{})
	for primesBatch := range results {
		for _, p := range primesBatch {
			primeSet[p] = struct{}{}
		}
	}

	uniquePrimes := make([]uint64, 0, len(primeSet))
	for p := range primeSet {
		uniquePrimes = append(uniquePrimes, p)
	}
	sort.Slice(uniquePrimes, func(i, j int) bool { return uniquePrimes[i] < uniquePrimes[j] })

	// Write Output to AFS
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
	afsClient.AFS_Close(fd)
	log.Printf("[coordinator] saved '%s' to AFS", outputFile)

	return Stats{
		InputFiles:   len(inputFiles),
		TotalNumbers: totalNumbers,
		PrimesFound:  len(uniquePrimes),
		Workers:      numWorkers,
		Duration:     time.Since(startTime),
	}, nil
}