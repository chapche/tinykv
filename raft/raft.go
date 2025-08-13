// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sort"

	"math/rand"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
	// IDs of all nodes (including self)
	peers []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	peers := c.peers
	if len(c.peers) == 0 {
		peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress)
	for _, id := range peers {
		prs[id] = &Progress{}
	}
	return &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower, // reduce unneccesary election
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		peers:            peers,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// check progress
	lastIndex := r.RaftLog.LastIndex()
	if lastIndex > 0 {
		lastIndex--
	}
	logTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		logTerm = 0
	}
	m := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Entries: make([]*pb.Entry, 0, 1),
		Index:   lastIndex, // index
		LogTerm: logTerm,
		Commit:  r.RaftLog.committed,
	}

	// commit no-op
	m.Entries = append(m.Entries, &r.RaftLog.entries[len(r.RaftLog.entries)-1])

	// append the message
	r.msgs = append(r.msgs, *m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	m := &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	// append the message
	r.msgs = append(r.msgs, *m)
}

func (r *Raft) sendRequestVote(to uint64) {
	var logTerm uint64
	var index uint64
	logTerm, index = 0, 0
	if len(r.RaftLog.entries) > 0 {
		entry := &r.RaftLog.entries[len(r.RaftLog.entries)-1]
		index = entry.Index
		logTerm = entry.Term
	}
	m := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   index,
		LogTerm: logTerm,
	}
	// append the message
	r.msgs = append(r.msgs, *m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		// add random time in case multiple nodes timeout at the same time
		src := rand.NewSource(time.Now().UnixNano())
		newRand := rand.New(src)
		randomTime := newRand.Intn(2 * r.electionTimeout)
		if r.electionElapsed >= r.electionTimeout+randomTime {
			r.becomeCandidate()
			if len(r.peers) <= 1 {
				r.becomeLeader()
				break
			}
			for _, id := range r.peers {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for _, id := range r.peers {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
	case StateCandidate:
		r.electionElapsed++
		src := rand.NewSource(time.Now().UnixNano())
		newRand := rand.New(src)
		randomTime := newRand.Intn(2 * r.electionTimeout)
		if r.electionElapsed >= r.electionTimeout+randomTime {
			r.Term++ // start another election round
			r.electionElapsed = 0
			for _, id := range r.peers {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// change state
	r.State = StateFollower
	// change term in case stale message
	r.Term = term
	// reset vote status
	r.Lead = lead
	r.Vote = None
	r.votes = make(map[uint64]bool)
	// reset election timer and heartbeat timer
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	lastIndex := r.RaftLog.LastIndex()
	noOpEntry := &pb.Entry{
		Term:  r.Term,
		Data:  nil,
		Index: lastIndex + 1,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, *noOpEntry)
	if len(r.peers) <= 1 { // commit directly if single node
		r.RaftLog.committed = noOpEntry.Index
	}
	// append noop entry to log
	for _, id := range r.peers {
		if id == r.id {
			r.Prs[id] = &Progress{
				Match: lastIndex + 1,
				Next:  lastIndex + 2,
			}
		} else {
			r.Prs[id] = &Progress{
				Match: 0, // last matched index
				// replicate the entries from last to match since we need to keep the entry sequence
				Next: lastIndex,
			}
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		// heartbeat or append entries
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
		case pb.MessageType_MsgRequestVoteResponse:
			return nil
		default:
			log.Debugf("unexpected message type in follower state")
			return errors.New("unexpected message type")
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend: // new leader is elected
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		default:
			log.Debugf("unexpected message type in candidate state")
			return errors.New("unexpected message type")
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend: // new leader is elected
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgPropose: // propose a new entry
			r.handlePropose(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		// node will become a leader when received more than half votes, we need to handle remaining vote
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		default:
			log.Debugf("unexpected message type in leader state")
			return errors.New("unexpected message type")
		}
	}
	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := false
	// stale msg or voted for someone, reject vote request
	if m.Term < r.Term {
		log.Debugf("termis less than current term, reject vote request: %d < %d", m.Term, r.Term)
		reject = true
	} else if m.Term == r.Term && r.Vote != None {
		log.Debugf("already voted for someone, reject vote request: %d", r.Vote)
		reject = r.Vote != m.From // might receive repeat msg
	} else {
		// check logTerm
		var logTerm uint64
		var index uint64
		logTerm, index = 0, 0
		if len(r.RaftLog.entries) > 0 {
			entry := &r.RaftLog.entries[len(r.RaftLog.entries)-1]
			index = entry.Index
			logTerm = entry.Term
		}
		if logTerm < m.LogTerm || (logTerm == m.LogTerm && index <= m.Index) {
			// vote for the candidate
			log.Debugf("vote for candidate: %d", m.From)
			r.Vote = m.From
			if r.Term < m.Term {
				r.Term = m.Term
			}
			r.electionElapsed = 0
			r.Lead = None
			if r.State != StateFollower {
				r.State = StateFollower
			}
		} else {
			if m.Term > r.Term { // I am STALE!
				r.onTermStale(m.Term, None)
			}
			reject = true
		}
	}
	resp := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, *resp)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if r.Term < m.Term {
		r.onTermStale(m.Term, None)
		return
	}
	if r.State == StateLeader {
		return
	}
	r.votes[m.From] = !m.Reject
	count := 0
	for _, vote := range r.votes {
		if vote {
			count++
		}
	}
	if count > len(r.peers)/2 {
		r.becomeLeader()
	} else if len(r.votes) == len(r.peers) {
		r.becomeFollower(r.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		log.Warnf("handleAppendEntries term is stale, current term: %v-%d, received term: %v-%d", r.id, r.Term, m.From, m.Term)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
			Commit: r.RaftLog.committed,
		})
		return	
	} else {
		// reset timer first
		r.becomeFollower(m.Term, m.From)
		if m.Commit < r.RaftLog.committed {
			// in case there are stale msgs
			log.Warnf("id from:%v to:%v stale committed index from:%v cur:%v", m.From, r.id, m.Commit, r.RaftLog.committed)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				Reject:  true,
				Commit: r.RaftLog.committed,
			})
			return
		}
		// check term and index
		term, err := r.RaftLog.Term(m.Index)
		if err != nil || term != m.LogTerm {
			log.Warnf("log entry not match, index:%v current term: %v-%d, received term: %v-%d", m.Index, r.id, term, m.From, m.LogTerm)
			index := m.Index
			if index > 0 {
				index--
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Index:   index,
				Reject:  true,
				Commit: r.RaftLog.committed,
			})
			return
		}
		// find matched index
		l := len(r.RaftLog.entries)
		j, matchedIndex := 0, l+1
		for ; j < l; j++ {
			entry := r.RaftLog.entries[j]
			if entry.Index == m.Index && entry.Term == m.LogTerm {
				matchedIndex = j
				break
			}
		}
		if matchedIndex < l {
			j = matchedIndex + 1
		} else {
			j = 0
		}
		// append entries to log, this might delete diversed log!
		// and that's why stable should be volitile
		if len(m.Entries) > 0 {
			for i := 0; i < len(m.Entries); i++ {
				if j < l {
					if r.RaftLog.entries[j].Index == m.Entries[i].Index &&
						r.RaftLog.entries[j].Term == m.Entries[i].Term {
						j++
						continue
					}
					// update stabled index since we found conflict entries
					if r.RaftLog.entries[j].Index > 0 {
						r.RaftLog.stabled = r.RaftLog.entries[j].Index - 1
					} else {
						r.RaftLog.stabled = 0
					}
					// delete conflict entry and all that follow it
					r.RaftLog.entries = r.RaftLog.entries[:j]
					l = len(r.RaftLog.entries)
				}
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				j++
			}
		}
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry from message)!
	var commitIndex uint64
	commitIndex = r.RaftLog.LastIndex()
	if m.Commit < commitIndex {
		commitIndex = m.Commit
	}
	r.RaftLog.committed = commitIndex
	// send a response back
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Commit: r.RaftLog.committed,
		Reject: false,
	})
}

func (r *Raft) sendAppendEntries(to uint64, index uint64) {
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {  // entry not found, negotiate from lastIndex
		log.Warnf("sendAppendEntries entry not found! id:%v index:%v commit:%v logTerm:%v to:%v", to, index, r.RaftLog.committed, logTerm, to)
		lastIndex := r.RaftLog.LastIndex()
		logTerm, _ = r.RaftLog.Term(lastIndex)
		msg := &pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      to,
			Term:    r.Term,
			Entries: nil,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: logTerm,
			Commit:  r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, *msg)
		return
	}
	idx := 0
	entries := make([]*pb.Entry, 0, len(r.RaftLog.entries))
	if index < r.RaftLog.stabled {
		stabled_entries, _ := r.RaftLog.storage.Entries(index, r.RaftLog.stabled)
		for _, entry := range stabled_entries {
			entries = append(entries, &entry)
		}
	} else if index > 0 {
		for i, entry := range r.RaftLog.entries {
			if entry.Index == index {
				logTerm = entry.Term
				idx = i + 1
				break
			} else if entry.Index > index {
				break
			}
		}
	}
	for ; idx < len(r.RaftLog.entries); idx++ {
		entries = append(entries, &r.RaftLog.entries[idx])
	}
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Entries: entries,
		Index:   index,
		LogTerm: logTerm,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, *msg)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		r.onTermStale(m.Term, None)
		return
	}
	if m.Reject {
		r.sendAppendEntries(m.From, m.Index)
		return
	}
	// update progress
	pr := r.Prs[m.From]
	// follower has replicated the log, update match and next index.
	if m.Index >= pr.Next {
		pr.Match = m.Index
		pr.Next = m.Index + 1
	}
	// calculate the new committed index.
	matchIndexes := make([]uint64, 0, len(r.Prs))
	for id, pr := range r.Prs {
		if id == r.id {
			continue
		}
		matchIndexes = append(matchIndexes, pr.Match)
	}
	// don't forget to include yourself!
	matchIndexes = append(matchIndexes, r.RaftLog.LastIndex())
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] > matchIndexes[j]
	})
	half := 0
	l := len(matchIndexes)
	if l > 0 {
		half = l / 2
	}
	// commit log when more than half of nodes have replicated it.
	if matchIndexes[half] > r.RaftLog.committed {
		term, err := r.RaftLog.Term(matchIndexes[half])
		if err != nil {
			return
		}
		// only log entries from the leader's current term are committed!
		if term < r.Term {
			return
		}
		r.RaftLog.committed = matchIndexes[half]
		// inform matched followers that commit changed!
		for id, pr := range r.Prs {
			if id == r.id {
				continue
			}
			if pr.Match+1 != pr.Next {
				continue
			}
			r.sendAppendEntries(id, pr.Match)
		}
		return
	}
}

func (r *Raft) handleMsgHup(m pb.Message) {
	// check terms from log entries!
	if len(r.RaftLog.entries) > 0 {
		term := r.RaftLog.entries[len(r.RaftLog.entries)-1].Term
		if term > r.Term { // I am stale, learn term from logs
			r.Term = term
			// return
		}
	}
	r.becomeCandidate()
	// if only one node then win election, become leader directly
	if len(r.peers) <= 1 {
		r.becomeLeader()
		return
	}
	for _, id := range r.peers {
		if id != r.id {
			r.sendRequestVote(id)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// check term and leader
	if m.Term < r.Term {
		log.Warnf("handleHeartbeat term is stale, current term: %v-%d, received term: %v-%d", r.id, r.Term, m.From, m.Term)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// reset election timer
	r.electionElapsed = 0
	// check if we have update-to-date log
	var index uint64
	var logTerm uint64
	index = r.RaftLog.LastIndex()
	logTerm, _ = r.RaftLog.Term(index)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Entries: nil,
		Index:   index,
		LogTerm: logTerm,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

// try commit log by heartbeat
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.onTermStale(m.Term, None)
		return
	}
	pr := r.Prs[m.From]
	term, err := r.RaftLog.Term(m.Index)
	// try update commit index and match info, then send entries if log match
	if err == nil && term == m.LogTerm {
		if m.Index < pr.Match {
			// append from index
			log.Warnf("handleHeartbeatResponse id:%v unexpected match index: %v-%v from:%v", r.id, m.Index, pr.Match, m.From)
			r.sendAppendEntries(m.From, m.Index)
			return
		}
		if m.Index >= pr.Next {
			pr.Match = m.Index
			pr.Next = m.Index + 1
		}
		matchIndexes := make([]uint64, 0, len(r.Prs))
		for id, pr := range r.Prs {
			if id == r.id {
				continue
			}
			matchIndexes = append(matchIndexes, pr.Match)
		}
		// don't forget to include yourself!
		matchIndexes = append(matchIndexes, r.RaftLog.LastIndex())
		sort.Slice(matchIndexes, func(i, j int) bool {
			return matchIndexes[i] > matchIndexes[j]
		})
		half := 0
		l := len(matchIndexes)
		if l > 0 {
			half = l / 2
		}
		// commit log when more than half of nodes have replicated it.
		if matchIndexes[half] > r.RaftLog.committed {
			term, err := r.RaftLog.Term(matchIndexes[half])
			if err != nil {
				return
			}
			// only log entries from the leader's current term are committed!
			if term < r.Term {
				return
			}
			r.RaftLog.committed = matchIndexes[half]
			// inform matched followers that commit changed!
			for id, pr := range r.Prs {
				if id == r.id {
					continue
				}
				if pr.Match+1 != pr.Next {
					continue
				}
				// fill from matched in case unnessary negotiation
				r.sendAppendEntries(id, pr.Match)
			}
			return
		} else if (m.Commit < r.RaftLog.committed) {
			// update commitIndex
			log.Infof("handleHeartbeatResponse id:%v update committedIndex from %v to %v", r.id, m.Commit, r.RaftLog.committed)
			r.sendAppendEntries(m.From, m.Index)
			return
		}
	} else { // only send entries if not match
		log.Warnf("handleHeartbeatResponse index:%v log entry missmatch id:%v-%v from:%v-%v", m.Index, r.id, term, m.From, m.LogTerm)	
		r.sendAppendEntries(m.From, m.Index)
	}
}

// local message
func (r *Raft) handleBeat(m pb.Message) {
	if r.id != m.To || m.From != m.To || r.State != StateLeader {
		return
	}
	r.heartbeatElapsed = 0
	for _, id := range r.peers {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	index := r.RaftLog.LastIndex()
	oldIndex := index
	for _, entry := range m.Entries {
		index++
		entry.Index = index
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	// we can commit directly if there are no peers
	if len(r.Prs) <= 1 {
		r.RaftLog.committed = index
		return
	}
	r.Prs[r.id].Match = index
	r.Prs[r.id].Next = index + 1
	// send msg
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		msg := &pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Entries: m.Entries,
			Index:   oldIndex, // index
			LogTerm: r.Term,
			Commit:  r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, *msg)
	}
}

func (r *Raft) onTermStale(term uint64, lead uint64) {
	if r.State != StateFollower || r.Term < term {
		r.becomeFollower(term, lead)
	} else {
		r.resetElectionTimer()
	}
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
