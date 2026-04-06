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


const (
	heartbeatInterval    = 1 * time.Second       
	electionTimeoutMin   = 4 * time.Second       
	electionTimeoutMax   = 5 * time.Second       
)


type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}


type persistentState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"` 
}

type ApplyFn func(filename string, data []byte, version int32)

type RaftNode struct {
	mu sync.Mutex

	self    string   
	peers   []string 

	ps      persistentState
	log     []*pb.LogEntry 
	dataDir string

	commitIndex int64
	lastApplied int64
	role        Role
	leaderID    string 

	nextIndex  map[string]int64 
	matchIndex map[string]int64 

	resetElectionC chan struct{} 
	stepDownC      chan struct{} 

	applyFn ApplyFn

	peerConns   map[string]pb.RaftClient
	peerConnsMu sync.Mutex
}


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

	n.log = []*pb.LogEntry{{Term: 0, Index: 0}}

	n.loadPersistentState()
	n.loadLog()

	return n
}


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

	if len(n.log) > 1 {
		n.commitIndex = n.log[len(n.log)-1].Index
	}
}


func (n *RaftNode) lastLogIndex() int64 {
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) lastLogTerm() int64 {
	return n.log[len(n.log)-1].Term
}

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

func (n *RaftNode) LeaderAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *RaftNode) becomeFollower(term int64, leaderID string) {
	prevRole := n.role
	n.role = Follower
	n.leaderID = leaderID
	if term > n.ps.CurrentTerm {
		n.ps.CurrentTerm = term
		n.ps.VotedFor = ""
		n.savePersistentState()
	}

	if prevRole == Leader {
		log.Printf("[raft %s] stepped down from Leader (term %d)", n.self, n.ps.CurrentTerm)
	}
	select {
	case n.resetElectionC <- struct{}{}:
	default:
	}
	if prevRole == Leader {
		select {
		case n.stepDownC <- struct{}{}:
		default:
		}
	}
}

func randomElectionTimeout() time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(span))
}


func (n *RaftNode) peerClient(addr string) pb.RaftClient {
	n.peerConnsMu.Lock()
	defer n.peerConnsMu.Unlock()

	if c, ok := n.peerConns[addr]; ok {
		return c
	}
	conn, err := grpc.Dial(addr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(256*1024*1024), 
			grpc.MaxCallSendMsgSize(256*1024*1024), 
		),
	)
	if err != nil {
		return nil
	}
	c := pb.NewRaftClient(conn)
	n.peerConns[addr] = c
	return c
}


func (n *RaftNode) Run() {
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

	votes := 1 
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
					if n.role == Candidate && n.ps.CurrentTerm == term {
						n.role = Leader
						n.leaderID = n.self
						n.nextIndex = make(map[string]int64)
						n.matchIndex = make(map[string]int64)
						for _, p := range n.peers {
							n.nextIndex[p] = n.lastLogIndex() + 1
							n.matchIndex[p] = 0
						}
						n.matchIndex[n.self] = n.lastLogIndex()
						log.Printf("[raft %s] became Leader (term %d)", n.self, term)
					}
					n.mu.Unlock()
					return
				}
			}
		case <-timer.C:
			return
		case <-n.resetElectionC:
			return
		}
	}
	select {
	case <-timer.C:
	case <-n.resetElectionC:
	}
}


func (n *RaftNode) runLeader() {
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
		if resp.ConflictIndex > 0 {
			n.nextIndex[peer] = resp.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

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

func (n *RaftNode) AppendEntries(
	_ context.Context, req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.ps.CurrentTerm {
		return &pb.AppendEntriesResponse{Term: n.ps.CurrentTerm, Success: false}, nil
	}

	n.becomeFollower(req.Term, req.LeaderId)

	if req.PrevLogIndex > 0 {
		prev := n.entryAt(req.PrevLogIndex)
		if prev == nil || prev.Term != req.PrevLogTerm {
			conflictIdx := req.PrevLogIndex
			var conflictTerm int64
			if prev != nil {
				conflictTerm = prev.Term
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

	logChanged := false
	for _, entry := range req.Entries {
		existing := n.entryAt(entry.Index)
		if existing != nil && existing.Term != entry.Term {
			n.log = n.log[:entry.Index]
			logChanged = true
		}
		if entry.Index >= int64(len(n.log)) {
			n.log = append(n.log, entry)
			logChanged = true
		}
	}
	
	if logChanged {
		n.saveLog()
	}

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

	alreadyVoted := n.ps.VotedFor != "" && n.ps.VotedFor != req.CandidateId
	logOK := req.LastLogTerm > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	if alreadyVoted || !logOK {
		return deny, nil
	}

	n.ps.VotedFor = req.CandidateId
	n.savePersistentState()

	select {
	case n.resetElectionC <- struct{}{}:
	default:
	}

	return &pb.RequestVoteResponse{Term: n.ps.CurrentTerm, VoteGranted: true}, nil
}


func (n *RaftNode) Submit(filename string, data []byte) (int32, error) {
	n.mu.Lock()

	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		msg := "not primary; redirect to " + leader
		return 0, status.Error(codes.FailedPrecondition, msg)
	}

	version := int32(len(n.log)) 
	entry := &pb.LogEntry{
		Term:     n.ps.CurrentTerm,
		Index:    int64(len(n.log)),
		Filename: filename,
		Data:     data,
		Version:  version,
	}
	n.log = append(n.log, entry)
	n.saveLog()
	n.matchIndex[n.self] = entry.Index
	commitTarget := entry.Index

	n.mu.Unlock()

	n.broadcastAppendEntries()

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