// coordinator_worker/main.go

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	coordinator "primality_afs/coordinator_worker/coordinator"
	"primality_afs/coordinator_worker/primality"
)

func main() {
	// Flags
	afsFlag := flag.String(
		"afs",
		"localhost:50051,localhost:50052,localhost:50053",
		"comma-separated AFS server addresses (primary listed first)",
	)

	inputsFlag := flag.String(
		"inputs",
		"input_dataset_001.txt,input_dataset_002.txt",
		"comma-separated input filenames on AFS",
	)

	outputFlag := flag.String(
		"output",
		"primes.txt",
		"output filename on AFS for the unique sorted primes",
	)

	cacheFlag := flag.String(
		"cache",
		"/tmp/prime_cache",
		"local cache directory for AFS client",
	)

	workersFlag := flag.Int(
		"workers",
		coordinator.NumWorkers,
		"number of parallel primality-testing goroutines",
	)

	flag.Parse()

	//Validate Inputs
	inputFiles := parseInputFiles(*inputsFlag)
	if len(inputFiles) == 0 {
		log.Fatal("ERROR: --inputs must contain at least one valid filename")
	}

	if *workersFlag < 1 {
		log.Fatal("ERROR: --workers must be >= 1")
	}

	if err := os.RemoveAll(*cacheFlag); err != nil {
		log.Fatalf("ERROR: failed to clear cache directory: %v", err)
	}
	if err := os.MkdirAll(*cacheFlag, 0755); err != nil {
		log.Fatalf("ERROR: failed to create cache directory: %v", err)
	}

	fmt.Println("   Distributed Prime Finder  ")
	fmt.Println("   Coordinator-Worker Model")
	fmt.Printf("AFS servers  : %s\n", *afsFlag)
	fmt.Printf("Input files  : %v\n", inputFiles)
	fmt.Printf("Output file  : %s\n", *outputFlag)
	fmt.Printf("Workers      : %d\n", *workersFlag)
	fmt.Println()

	stats, err := coordinator.Run(
		*afsFlag,
		*cacheFlag,
		inputFiles,
		*outputFlag,
		*workersFlag,
		primality.IsPrime,
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
	fmt.Printf("Output saved to AFS    : %s\n", *outputFlag)
	fmt.Println("DONE.")
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
