package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"primality_afs/coordinator"
	"primality_afs/primality"
	"primality_afs/worker"
)

func main() {
	// Determines which mode the application runs in
	modeFlag := flag.String("mode", "coordinator", "Run mode: 'coordinator' or 'worker'")

	// --- Coordinator Flags ---
	afsFlag := flag.String("afs", "localhost:50051,localhost:50052,localhost:50053", "comma-separated AFS server addresses")
	inputsFlag := flag.String("inputs", "input_dataset_001.txt,input_dataset_002.txt", "comma-separated input filenames on AFS")
	outputFlag := flag.String("output", "primes.txt", "output filename on AFS")
	cacheFlag := flag.String("cache", "/tmp/prime_cache", "local cache directory for AFS client")
	workerAddrsFlag := flag.String("workers", "localhost:6001,localhost:6002", "comma-separated gRPC worker addresses")

	// --- Worker Flags ---
	workerPortFlag := flag.String("port", ":6001", "port for the gRPC worker to listen on (e.g. :6001)")
	workerIdFlag := flag.Int("id", 1, "worker ID for logging")

	flag.Parse()

	if *modeFlag == "worker" {
		fmt.Printf("Starting gRPC Worker %d on port %s...\n", *workerIdFlag, *workerPortFlag)
		if err := worker.StartGRPCServer(*workerPortFlag, *workerIdFlag, primality.IsPrime); err != nil {
			log.Fatalf("Worker failed: %v", err)
		}
		return
	}

	if *modeFlag == "coordinator" {
		inputFiles := parseInputFiles(*inputsFlag)
		workerAddrs := parseInputFiles(*workerAddrsFlag)

		if len(inputFiles) == 0 {
			log.Fatal("ERROR: --inputs must contain at least one valid filename")
		}
		if len(workerAddrs) == 0 {
			log.Fatal("ERROR: --workers must contain at least one worker address")
		}

		if err := os.RemoveAll(*cacheFlag); err != nil {
			log.Fatalf("ERROR: failed to clear cache directory: %v", err)
		}
		if err := os.MkdirAll(*cacheFlag, 0755); err != nil {
			log.Fatalf("ERROR: failed to create cache directory: %v", err)
		}

		fmt.Println("   Distributed Prime Finder (gRPC)  ")
		fmt.Println("   Coordinator Node")
		fmt.Printf("Worker Addrs : %v\n", workerAddrs)

		stats, err := coordinator.Run(
			*afsFlag,
			*cacheFlag,
			inputFiles,
			*outputFlag,
			workerAddrs,
		)
		if err != nil {
			log.Fatalf("ERROR: coordinator failed: %v", err)
		}

		fmt.Println("   Results")
		fmt.Printf("Input files processed  : %d\n", stats.InputFiles)
		fmt.Printf("Total numbers checked  : %d\n", stats.TotalNumbers)
		fmt.Printf("Unique primes found    : %d\n", stats.PrimesFound)
		fmt.Printf("Workers used           : %d\n", stats.Workers)
		fmt.Printf("Time taken             : %s\n", stats.Duration.Round(1_000_000))
	} else {
		log.Fatalf("Unknown mode: %s. Use 'coordinator' or 'worker'", *modeFlag)
	}
}

func parseInputFiles(input string) []string {
	parts := strings.Split(input, ",")
	var result []string
	for _, f := range parts {
		f = strings.TrimSpace(f)
		if f != "" {
			result = append(result, f)
		}
	}
	return result
}