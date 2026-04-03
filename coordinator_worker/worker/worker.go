// coordinator_worker/worker/worker.go
//
// Worker logic for the distributed prime finder.
// Each worker reads WorkChunks from the jobs channel, tests each number
// for primality using the injected isPrime function, and sends primes to
// the results channel.

package worker

import (
	"log"
	"sync"
	"sync/atomic"
)

// WorkChunk is one unit of work passed from coordinator → worker.
type WorkChunk struct {
	ID     int
	Values []uint64
}

// Run reads chunks from jobs, tests each number, sends primes to results.
// isPrime is injected so this package has zero dependency on primality logic.
func Run(id int, jobs <-chan WorkChunk, results chan<- uint64, wg *sync.WaitGroup, found *atomic.Int64, isPrime func(uint64) bool) {
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
