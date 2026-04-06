// server/raft.go
//
// Standard Raft implementation for the AFS server.
//
// Roles    : Follower → Candidate → Leader (and back).
// Log      : In-memory slice of LogEntry; persisted to <dataDir>/raft_log.json
//            on every append so the node can recover after a crash.
// State    : currentTerm + votedFor persisted to <dataDir>/raft_state.json.
// Commits  : Once a log entry is committed (majority ACK), applyFn is called to
//            materialise the write into the file store.
// Timeouts : Election timeout 150–300 ms (randomised), heartbeat 50 ms.

package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "primality_afs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	heartbeatInterval    = 1 * time.Second       // Changed from 150ms
	electionTimeoutMin   = 4 * time.Second       // Changed from 1500ms
	electionTimeoutMax   = 5 * time.Second       // Changed from 3000ms
)

// ─── Role ─────────────────────────────────────────────────────────────────────

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

// ─── Persistent state (written to disk on every change) ───────────────────────

type persistentState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"` // "" means none
}

// ─── RaftNode ─────────────────────────────────────────────────────────────────

// ApplyFn is called (under no lock) when a log entry is committed.
// It must write the file data to the local store and return the new version.
type ApplyFn func(filename string, data []byte, version int32)

type RaftNode struct {
	mu sync.Mutex

	// Identity
	self    string   // "host:port" of this node
	peers   []string // addresses of all OTHER nodes

	// Persistent
	ps      persistentState
	log     []*pb.LogEntry // log[0] is a sentinel (index=0, term=0)
	dataDir string

	// Volatile (all nodes)
	commitIndex int64
	lastApplied int64
	role        Role
	leaderID    string // known leader address (may be "")

	// Volatile (leader only) — reset on election
	nextIndex  map[string]int64 // peer → next log index to send
	matchIndex map[string]int64 // peer → highest replicated index

	// Channels / timers
	resetElectionC chan struct{} // signal to reset election timer
	stepDownC      chan struct{} // signal leader to step down

	applyFn ApplyFn

	// gRPC connections to peers (lazy, cached)
	peerConns   map[string]pb.RaftClient
	peerConnsMu sync.Mutex
}

// ─── Constructor ──────────────────────────────────────────────────────────────

func NewRaftNode(self string, peers []string, dataDir string, applyFn ApplyFn) *RaftNode {
	n := &RaftNode{
		self:           self,
		peers:          peers,
		dataDir:        dataDir,
		applyFn:        applyFn,
		resetElectionC: make(chan struct{}, 1),
		stepDownC:      make(chan struct{}, 1),
		peerConns:      make(map[string]pb.RaftClient),
	}

	// Sentinel entry so that real entries start at index 1.
	n.log = []*pb.LogEntry{{Term: 0, Index: 0}}

	n.loadPersistentState()
	n.loadLog()

	return n
}

// ─── Persistence ──────────────────────────────────────────────────────────────

func (n *RaftNode) statePath() string { return filepath.Join(n.dataDir, "raft_state.json") }
func (n *RaftNode) logPath() string   { return filepath.Join(n.dataDir, "raft_log.json") }

func (n *RaftNode) savePersistentState() {
	data, _ := json.Marshal(n.ps)
	_ = os.WriteFile(n.statePath(), data, 0644)
}

func (n *RaftNode) loadPersistentState() {
	data, err := os.ReadFile(n.statePath())
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &n.ps)
}

func (n *RaftNode) saveLog() {
	data, _ := json.Marshal(n.log)
	_ = os.WriteFile(n.logPath(), data, 0644)
}

func (n *RaftNode) loadLog() {
	data, err := os.ReadFile(n.logPath())
	if err != nil {
		return
	}
	var entries []*pb.LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return
	}
	if len(entries) > 0 {
		n.log = entries
	}
	// Rehydrate commitIndex / lastApplied from the log length; the apply loop
	// will re-apply committed entries after a restart.
	if len(n.log) > 1 {
		n.commitIndex = n.log[len(n.log)-1].Index
		// We'll re-apply from 1 when Run() starts.
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func (n *RaftNode) lastLogIndex() int64 {
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) lastLogTerm() int64 {
	return n.log[len(n.log)-1].Term
}

// entryAt returns the log entry at logical index idx (1-based).
// Returns nil if out of range.
func (n *RaftNode) entryAt(idx int64) *pb.LogEntry {
	if idx < 0 || int(idx) >= len(n.log) {
		return nil
	}
	return n.log[idx]
}

func (n *RaftNode) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// LeaderAddr returns the known leader address (may be empty).
func (n *RaftNode) LeaderAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// becomeFollower — must be called with n.mu held.
func (n *RaftNode) becomeFollower(term int64, leaderID string) {
	prevRole := n.role
	n.role = Follower
	n.leaderID = leaderID
	if term > n.ps.CurrentTerm {
		n.ps.CurrentTerm = term
		n.ps.VotedFor = ""
		n.savePersistentState()
	}
	// Reset the follower election timer.

	if prevRole == Leader {
		log.Printf("[raft %s] stepped down from Leader (term %d)", n.self, n.ps.CurrentTerm)
	}
	select {
	case n.resetElectionC <- struct{}{}:
	default:
	}
	// If we were leader, kick the runLeader loop out immediately.
	if prevRole == Leader {
		select {
		case n.stepDownC <- struct{}{}:
		default:
		}
	}
}

// randomElectionTimeout returns a duration in [min, max).
func randomElectionTimeout() time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(span))
}

// ─── peer gRPC dial (cached) ──────────────────────────────────────────────────

func (n *RaftNode) peerClient(addr string) pb.RaftClient {
	n.peerConnsMu.Lock()
	defer n.peerConnsMu.Unlock()

	if c, ok := n.peerConns[addr]; ok {
		return c
	}
	conn, err := grpc.Dial(addr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(256*1024*1024), // 256 MB
			grpc.MaxCallSendMsgSize(256*1024*1024), // 256 MB
		),
	)
	if err != nil {
		return nil
	}
	c := pb.NewRaftClient(conn)
	n.peerConns[addr] = c
	return c
}

// ─── Run — main Raft goroutine ────────────────────────────────────────────────

func (n *RaftNode) Run() {
	// Re-apply any committed log entries that survived a restart.
	n.mu.Lock()
	toApply := n.commitIndex
	n.mu.Unlock()
	for i := int64(1); i <= toApply; i++ {
		n.mu.Lock()
		e := n.entryAt(i)
		n.mu.Unlock()
		if e != nil {
			n.applyFn(e.Filename, e.Data, e.Version)
		}
	}
	n.mu.Lock()
	n.lastApplied = toApply
	n.mu.Unlock()

	for {
		n.mu.Lock()
		role := n.role
		n.mu.Unlock()

		switch role {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

// ─── Follower loop ────────────────────────────────────────────────────────────

func (n *RaftNode) runFollower() {
	timeout := randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-n.resetElectionC:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomElectionTimeout())

		case <-timer.C:
			n.mu.Lock()
			log.Printf("[raft %s] election timeout — starting election (term %d)",
				n.self, n.ps.CurrentTerm+1)
			n.role = Candidate
			n.mu.Unlock()
			return
		}
	}
}

// ─── Candidate loop ───────────────────────────────────────────────────────────

func (n *RaftNode) runCandidate() {
	n.mu.Lock()
	n.ps.CurrentTerm++
	n.ps.VotedFor = n.self
	n.savePersistentState()
	term := n.ps.CurrentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	peers := n.peers
	n.mu.Unlock()

	log.Printf("[raft %s] became Candidate (term %d)", n.self, term)

	votes := 1 // vote for self
	needed := (len(peers)+1)/2 + 1

	voteCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(addr string) {
			c := n.peerClient(addr)
			if c == nil {
				voteCh <- false
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			resp, err := c.RequestVote(ctx, &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.self,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				voteCh <- false
				return
			}
			n.mu.Lock()
			if resp.Term > n.ps.CurrentTerm {
				n.becomeFollower(resp.Term, "")
				n.mu.Unlock()
				voteCh <- false
				return
			}
			n.mu.Unlock()
			voteCh <- resp.VoteGranted
		}(peer)
	}

	timeout := randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for i := 0; i < len(peers); i++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
				if votes >= needed {
					n.mu.Lock()
					// Only become leader if still a candidate in the same term.
					// becomeFollower() may have concurrently demoted us.
					if n.role == Candidate && n.ps.CurrentTerm == term {
						n.role = Leader
						n.leaderID = n.self
						n.nextIndex = make(map[string]int64)
						n.matchIndex = make(map[string]int64)
						for _, p := range n.peers {
							n.nextIndex[p] = n.lastLogIndex() + 1
							n.matchIndex[p] = 0
						}
						// Include self so maybeAdvanceCommit quorum counts correctly.
						n.matchIndex[n.self] = n.lastLogIndex()
						log.Printf("[raft %s] became Leader (term %d)", n.self, term)
					}
					n.mu.Unlock()
					return
				}
			}
		case <-timer.C:
			// Election timed out — stay Candidate, outer loop restarts.
			return
		case <-n.resetElectionC:
			// Higher term seen — stepped down to Follower.
			return
		}
	}
	// All responses in but no majority yet — wait out the remainder of the timer.
	select {
	case <-timer.C:
	case <-n.resetElectionC:
	}
}

// ─── Leader loop ─────────────────────────────────────────────────────────────

func (n *RaftNode) runLeader() {
	// Send immediate heartbeat on becoming leader.
	n.broadcastAppendEntries()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.broadcastAppendEntries()

		case <-n.stepDownC:
			return
		}
	}
}

// ─── AppendEntries broadcast (leader → followers) ─────────────────────────────

func (n *RaftNode) broadcastAppendEntries() {
	n.mu.Lock()
	peers := n.peers
	term := n.ps.CurrentTerm
	n.mu.Unlock()

	for _, peer := range peers {
		go n.sendAppendEntries(peer, term)
	}
}

func (n *RaftNode) sendAppendEntries(peer string, term int64) {
	n.mu.Lock()
	if n.role != Leader || n.ps.CurrentTerm != term {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	prevIdx := nextIdx - 1
	var prevTerm int64
	if e := n.entryAt(prevIdx); e != nil {
		prevTerm = e.Term
	}

	// Collect entries to send.
	var entries []*pb.LogEntry
	for i := nextIdx; i < int64(len(n.log)); i++ {
		entries = append(entries, n.log[i])
	}

	commitIndex := n.commitIndex
	selfTerm := n.ps.CurrentTerm
	n.mu.Unlock()

	c := n.peerClient(peer)
	if c == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	resp, err := c.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         selfTerm,
		LeaderId:     n.self,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})

	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.ps.CurrentTerm {
		n.becomeFollower(resp.Term, "")
		select {
		case n.stepDownC <- struct{}{}:
		default:
		}
		return
	}

	if n.role != Leader || n.ps.CurrentTerm != term {
		return
	}

	if resp.Success {
		newMatch := prevIdx + int64(len(entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
			n.nextIndex[peer] = newMatch + 1
		}
		n.maybeAdvanceCommit()
	} else {
		// Back-track using conflict hint.
		if resp.ConflictIndex > 0 {
			n.nextIndex[peer] = resp.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

// maybeAdvanceCommit advances commitIndex if a majority has replicated up to a
// new index in the current term.  Must be called with n.mu held.
func (n *RaftNode) maybeAdvanceCommit() {
	clusterSize := len(n.peers) + 1
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		e := n.entryAt(idx)
		if e == nil || e.Term != n.ps.CurrentTerm {
			continue
		}
		count := 0
		for _, mi := range n.matchIndex {
			if mi >= idx {
				count++
			}
		}
		if count > clusterSize/2 {
			n.commitIndex = idx
			go n.applyCommitted()
			break
		}
	}
}

// applyCommitted applies all newly committed entries.
func (n *RaftNode) applyCommitted() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		n.lastApplied++
		e := n.entryAt(n.lastApplied)
		n.mu.Unlock()

		if e != nil {
			n.applyFn(e.Filename, e.Data, e.Version)
		}
	}
}

// ─── Raft gRPC server handlers ────────────────────────────────────────────────

// AppendEntries implements pb.RaftServer.
func (n *RaftNode) AppendEntries(
	_ context.Context, req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	n.mu.Lock()
	defer n.mu.Unlock()

	// §5.1: reply false if term < currentTerm.
	if req.Term < n.ps.CurrentTerm {
		return &pb.AppendEntriesResponse{Term: n.ps.CurrentTerm, Success: false}, nil
	}

	// Valid leader message — become/stay follower.
	n.becomeFollower(req.Term, req.LeaderId)

	// §5.3: reply false if log doesn't contain entry at prevLogIndex with prevLogTerm.
	if req.PrevLogIndex > 0 {
		prev := n.entryAt(req.PrevLogIndex)
		if prev == nil || prev.Term != req.PrevLogTerm {
			// Compute conflict hint.
			conflictIdx := req.PrevLogIndex
			var conflictTerm int64
			if prev != nil {
				conflictTerm = prev.Term
				// Walk back to find first entry of that term.
				for conflictIdx > 1 && n.log[conflictIdx-1].Term == conflictTerm {
					conflictIdx--
				}
			}
			return &pb.AppendEntriesResponse{
				Term:          n.ps.CurrentTerm,
				Success:       false,
				ConflictIndex: conflictIdx,
				ConflictTerm:  conflictTerm,
			}, nil
		}
	}

	// §5.3: append new entries, truncating on conflict.
	logChanged := false
	for _, entry := range req.Entries {
		existing := n.entryAt(entry.Index)
		if existing != nil && existing.Term != entry.Term {
			// Delete this and all following entries.
			n.log = n.log[:entry.Index]
			logChanged = true
		}
		if entry.Index >= int64(len(n.log)) {
			n.log = append(n.log, entry)
			logChanged = true
		}
	}
	
	// Only hammer the disk if we actually added or truncated entries!
	if logChanged {
		n.saveLog()
	}

	// §5.3: update commitIndex.
	if req.LeaderCommit > n.commitIndex {
		last := n.lastLogIndex()
		if req.LeaderCommit < last {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = last
		}
		go n.applyCommitted()
	}

	return &pb.AppendEntriesResponse{Term: n.ps.CurrentTerm, Success: true}, nil
}

// RequestVote implements pb.RaftServer.
func (n *RaftNode) RequestVote(
	_ context.Context, req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {

	n.mu.Lock()
	defer n.mu.Unlock()

	deny := &pb.RequestVoteResponse{Term: n.ps.CurrentTerm, VoteGranted: false}

	if req.Term < n.ps.CurrentTerm {
		return deny, nil
	}

	if req.Term > n.ps.CurrentTerm {
		n.becomeFollower(req.Term, "")
	}

	// §5.2 / §5.4: grant vote if we haven't voted yet (or voted for this candidate)
	// AND candidate's log is at least as up-to-date as ours.
	alreadyVoted := n.ps.VotedFor != "" && n.ps.VotedFor != req.CandidateId
	logOK := req.LastLogTerm > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	if alreadyVoted || !logOK {
		return deny, nil
	}

	n.ps.VotedFor = req.CandidateId
	n.savePersistentState()

	// Reset election timer — we just heard from a valid candidate.
	select {
	case n.resetElectionC <- struct{}{}:
	default:
	}

	return &pb.RequestVoteResponse{Term: n.ps.CurrentTerm, VoteGranted: true}, nil
}

// ─── Client-command entry point (used by the AFS StoreFile handler) ──────────

// Submit appends a new command to the leader's log and blocks until it is
// committed (or returns an error if this node is not the leader).
func (n *RaftNode) Submit(filename string, data []byte) (int32, error) {
	n.mu.Lock()

	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		msg := "not primary; redirect to " + leader
		return 0, status.Error(codes.FailedPrecondition, msg)
	}

	// Assign version = previous version of this file + 1.
	// (Simple approach: version = log length, guaranteed monotonically increasing.)
	version := int32(len(n.log)) // 1-based since log[0] is sentinel
	entry := &pb.LogEntry{
		Term:     n.ps.CurrentTerm,
		Index:    int64(len(n.log)),
		Filename: filename,
		Data:     data,
		Version:  version,
	}
	n.log = append(n.log, entry)
	n.saveLog()
	// Update own matchIndex.
	n.matchIndex[n.self] = entry.Index
	commitTarget := entry.Index

	n.mu.Unlock()

	// Trigger immediate replication.
	n.broadcastAppendEntries()

	// Poll until committed or we lose leadership.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		committed := n.commitIndex >= commitTarget
		stillLeader := n.role == Leader
		n.mu.Unlock()

		if committed {
			return version, nil
		}
		if !stillLeader {
			return 0, status.Error(codes.Unavailable, "lost leadership before commit")
		}
		time.Sleep(5 * time.Millisecond)
	}

	return 0, status.Error(codes.DeadlineExceeded, "commit timed out")
}