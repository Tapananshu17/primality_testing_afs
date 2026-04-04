package main

import (
	"bufio"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// isPrime uses Go's built-in Miller-Rabin probabilistic primality test
// which is extremely fast and accurate for large numbers
func isPrime(s string) bool {
	n := new(big.Int)
	_, ok := n.SetString(s, 10)
	if !ok || n.Sign() <= 0 {
		return false
	}
	return n.ProbablyPrime(20) // 20 rounds = near-certain accuracy
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run count_primes.go <filename>")
		os.Exit(1)
	}

	filename := os.Args[1]

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	start := time.Now()

	numWorkers := runtime.NumCPU()
	fmt.Printf("Using %d CPU cores for parallel processing...\n\n", numWorkers)

	// Channel to send unique number strings to workers
	jobs := make(chan string, 10000)

	// Shared set for deduplication (guarded by mutex)
	var mu sync.Mutex
	seen := make(map[string]struct{})

	var totalCount, primeCount, dupCount, invalidCount int

	// Worker pool
	var wg sync.WaitGroup
	var resultMu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localPrimes := 0
			for numStr := range jobs {
				if isPrime(numStr) {
					localPrimes++
				}
			}
			resultMu.Lock()
			primeCount += localPrimes
			resultMu.Unlock()
		}()
	}

	// Read file and feed unique numbers into job channel
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.FieldsFunc(line, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t'
		})

		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}

			// Validate: try int64 first, then big.Int for huge numbers
			_, err := strconv.ParseInt(field, 10, 64)
			if err != nil {
				n := new(big.Int)
				_, ok := n.SetString(field, 10)
				if !ok {
					invalidCount++
					continue
				}
			}

			totalCount++

			mu.Lock()
			_, alreadySeen := seen[field]
			if !alreadySeen {
				seen[field] = struct{}{}
			}
			mu.Unlock()

			if alreadySeen {
				dupCount++
				continue
			}

			jobs <- field
		}
	}

	close(jobs)
	wg.Wait()

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	uniqueCount := totalCount - dupCount

	fmt.Println("========================================")
	fmt.Println("         PRIME COUNTER RESULTS          ")
	fmt.Println("========================================")
	fmt.Printf("File             : %s\n", filename)
	fmt.Printf("Total numbers    : %d\n", totalCount)
	fmt.Printf("Duplicates found : %d\n", dupCount)
	fmt.Printf("Unique numbers   : %d\n", uniqueCount)
	fmt.Printf("Prime numbers    : %d  (unique primes only)\n", primeCount)
	fmt.Printf("Non-primes       : %d\n", uniqueCount-primeCount)
	if invalidCount > 0 {
		fmt.Printf("Invalid/skipped  : %d\n", invalidCount)
	}
	fmt.Printf("Time taken       : %s\n", elapsed)
	fmt.Println("========================================")
}