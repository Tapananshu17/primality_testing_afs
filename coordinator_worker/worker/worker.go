package worker

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	primepb "primality_afs/prime_proto"
)

type Server struct {
	primepb.UnimplementedPrimeWorkerServer
	IsPrime  func(uint64) bool
	WorkerID int

	mu             sync.Mutex
	currentChunkID int32
	currentOffset  int32
	isProcessing   bool
}

func (s *Server) ProcessChunk(ctx context.Context, req *primepb.WorkChunk) (*primepb.PrimeResult, error) {
	s.mu.Lock()
	s.currentChunkID = req.Id
	s.isProcessing = true
	s.mu.Unlock()

	startOffset := req.StartOffset
	atomic.StoreInt32(&s.currentOffset, startOffset)

	var primes []uint64

	for i := startOffset; i < int32(len(req.Values)); i++ {
		n := req.Values[i]
		
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.isProcessing = false
			s.mu.Unlock()
			return &primepb.PrimeResult{ChunkId: req.Id, Primes: primes}, ctx.Err()
		default:
		}

		if s.IsPrime(n) {
			primes = append(primes, n)
		}
		
		atomic.StoreInt32(&s.currentOffset, i+1)
	}

	s.mu.Lock()
	s.isProcessing = false
	s.mu.Unlock()

	log.Printf("[worker %d] chunk %d: processed %d numbers (skipped %d) → %d primes",
		s.WorkerID, req.Id, int32(len(req.Values))-startOffset, startOffset, len(primes))

	return &primepb.PrimeResult{
		ChunkId: req.Id,
		Primes:  primes,
	}, nil
}

func (s *Server) CaptureState(ctx context.Context, req *primepb.SnapshotRequest) (*primepb.SnapshotResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isProcessing {
		return &primepb.SnapshotResponse{CurrentChunkId: -1, Offset: 0}, nil
	}

	return &primepb.SnapshotResponse{
		CurrentChunkId: s.currentChunkID,
		Offset:         atomic.LoadInt32(&s.currentOffset),
	}, nil
}

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