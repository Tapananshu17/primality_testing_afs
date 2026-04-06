package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"primality_afs/coordinator_worker/coordinator"
	"primality_afs/coordinator_worker/primality"
	"primality_afs/coordinator_worker/worker"
	"path/filepath"
)

func main() {
	modeFlag := flag.String("mode", "coordinator", "Run mode: 'coordinator' or 'worker'")

	afsFlag        := flag.String("afs", "localhost:50051,localhost:50052,localhost:50053", "comma-separated AFS server addresses")
	inputsFlag     := flag.String("inputs", "input_dataset_001.txt,input_dataset_002.txt", "comma-separated input filenames on AFS")
	outputFlag     := flag.String("output", "primes.txt", "output filename on AFS")
	cacheFlag      := flag.String("cache", "/tmp/prime_cache", "local cache directory for AFS client")
	workerAddrsFlag := flag.String("workers", "localhost:6001,localhost:6002", "comma-separated gRPC worker addresses")

	recoverFlag := flag.Bool("recover", false, "resume from the latest snapshot on AFS instead of starting fresh")

	workerPortFlag := flag.String("port", ":6001", "port for the gRPC worker to listen on (e.g. :6001)")
	workerIDFlag   := flag.Int("id", 1, "worker ID for logging")

	flag.Parse()

	if *modeFlag == "worker" {
		fmt.Printf("Starting gRPC Worker %d on port %s...\n", *workerIDFlag, *workerPortFlag)
		if err := worker.StartGRPCServer(*workerPortFlag, *workerIDFlag, primality.IsPrime); err != nil {
			log.Fatalf("Worker failed: %v", err)
		}
		return
	}

	if *modeFlag == "coordinator" {
		inputFiles  := parseList(*inputsFlag)
		workerAddrs := parseList(*workerAddrsFlag)

		if len(inputFiles) == 0 {
			log.Fatal("ERROR: --inputs must contain at least one valid filename")
		}
		if len(workerAddrs) == 0 {
			log.Fatal("ERROR: --workers must contain at least one worker address")
		}

		if !*recoverFlag {
			if err := os.RemoveAll(*cacheFlag); err != nil {
				log.Fatalf("ERROR: failed to clear cache directory: %v", err)
			}
		}
		if err := os.MkdirAll(*cacheFlag, 0755); err != nil {
			log.Fatalf("ERROR: failed to create cache directory: %v", err)
		}

		fmt.Println("=== Distributed Prime Finder (gRPC) ===")
		fmt.Println("    Coordinator Node")
		fmt.Printf("Worker Addrs : %v\n", workerAddrs)
		if *recoverFlag {
			fmt.Println("Mode         : RECOVERY (resuming from snapshot)")
		} else {
			fmt.Println("Mode         : FRESH START")
		}

		stats, err := coordinator.Run(
			*afsFlag,
			*cacheFlag,
			inputFiles,
			*outputFlag,
			workerAddrs,
			*recoverFlag, 
		)
		if err != nil {
			log.Fatalf("ERROR: coordinator failed: %v", err)
		}

		fmt.Println("\n=== Results ===")
		fmt.Printf("Input files processed  : %d\n", stats.InputFiles)
		fmt.Printf("Total numbers checked  : %d\n", stats.TotalNumbers)
		fmt.Printf("Unique primes found    : %d\n", stats.PrimesFound)
		fmt.Printf("Workers used           : %d\n", stats.Workers)
		fmt.Printf("Time taken             : %s\n", stats.Duration.Round(1_000_000))
		return
	}

	log.Fatalf("Unknown mode: %s. Use 'coordinator' or 'worker'", *modeFlag)
}

func parseList(input string) []string {
	parts := strings.Split(input, ",")
	var result []string
	
	for _, f := range parts {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		
		matches, err := filepath.Glob(f)
		if err == nil && len(matches) > 0 {
			result = append(result, matches...)
		} else {
			result = append(result, f)
		}
	}
	return result
}