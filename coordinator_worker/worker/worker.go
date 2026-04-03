package worker

import (
	"context"
	"log"
	"net"

	"primality_afs/prime_proto"
	"google.golang.org/grpc"
)

// Server implements the primepb.PrimeWorkerServer interface.
type Server struct {
	primepb.UnimplementedPrimeWorkerServer
	IsPrime func(uint64) bool
	WorkerID int
}

// ProcessChunk handles an incoming gRPC request containing a chunk of numbers.
func (s *Server) ProcessChunk(ctx context.Context, req *primepb.WorkChunk) (*primepb.PrimeResult, error) {
	var primes []uint64

	for _, n := range req.Values {
		if s.IsPrime(n) {
			primes = append(primes, n)
		}
	}

	log.Printf("[worker %d] processed chunk %d — %d numbers, %d primes", s.WorkerID, req.Id, len(req.Values), len(primes))

	return &primepb.PrimeResult{
		ChunkId: req.Id,
		Primes:  primes,
	}, nil
}

// StartGRPCServer starts listening for requests from the Coordinator.
func StartGRPCServer(addr string, workerID int, isPrime func(uint64) bool) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	srv := &Server{
		IsPrime:  isPrime,
		WorkerID: workerID,
	}
	
	primepb.RegisterPrimeWorkerServer(grpcServer, srv)
	
	log.Printf("[worker %d] listening on %s", workerID, addr)
	return grpcServer.Serve(lis)
}