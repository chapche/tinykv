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
	return &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower, // reduce unneccesary election
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		peers:            c.peers,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	m := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	// check progress
	pr := r.Prs[to]
	// TODO(chapche) Match or Next?
	for i := pr.Match + 1; i < uint64(len(r.RaftLog.entries)); i++ {
		m.Entries = append(m.Entries, &r.RaftLog.entries[i])
	}
	// nothing to append
	if len(m.Entries) == 0 {
		return false
	}

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
	}
	// append the message
	r.msgs = append(r.msgs, *m)
}

func (r *Raft) sendRequestVote(to uint64) {
	m := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
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
	// append noop entry to log
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		m := &pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      id,
			Term:    r.Term,
		}
		// append the message
		r.msgs = append(r.msgs, *m)
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
		reject = r.Vote != m.From
	} else {
		// vote for the candidate
		log.Debugf("vote for candidate: %d", m.From)
		r.Vote = m.From
		if r.Term < m.Term {
			r.Term = m.Term
		}
		r.electionElapsed = 0
		if r.State != StateFollower {
			r.State = StateFollower
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
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Reject {
		r.votes[m.From] = false
		return
	}
	r.votes[m.From] = true
	count := 0
	for _, vote := range r.votes {
		if vote {
			count++
		}
	}
	if count > len(r.peers)/2 {
		r.becomeLeader()
		return
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		log.Debugf("term is stale, current term: %d, received term: %d", r.Term, m.Term)
	} else {
		r.becomeFollower(m.Term, m.From)
		for _, entry := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}

	// if successfully appended entries, send a response back to leader
	// TODO(chapche) calc right index!
	index := uint64(len(r.RaftLog.entries))
	if index > 0 {
		index -= 1
	}
	resp := &pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   index,
	}
	// append the message
	r.msgs = append(r.msgs, *resp)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// TODO(chapche)
}

func (r *Raft) handleMsgHup(m pb.Message) {
	// local msg
	if r.id != m.To {
		return
	}
	r.becomeCandidate()
	// if only one node then win election, become leader directly
	if len(r.peers) == 1 {
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
		log.Debugf("term is stale, current term: %d, received term: %d", r.Term, m.Term)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// reset election timer
	r.electionElapsed = 0
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
