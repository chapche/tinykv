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

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// ErrInvalidConfig is returned when newRaft returns nil
var ErrInvalidConfig = errors.New("raft: newRaft failed")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft      *Raft
	lastReady *Ready
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	node := &RawNode{
		Raft: newRaft(config),
	}
	if node.Raft == nil {
		return nil, ErrInvalidConfig
	}
	snapshot, _ := node.Raft.RaftLog.storage.Snapshot()
	node.lastReady = &Ready{
		SoftState: &SoftState{Lead: node.Raft.Lead, RaftState: node.Raft.State},
		HardState: pb.HardState{
			Term:   node.Raft.Term,
			Vote:   node.Raft.Vote,
			Commit: node.Raft.RaftLog.committed,
		},
		Entries:          node.Raft.RaftLog.entries,
		Messages:         node.Raft.msgs,
		CommittedEntries: node.Raft.RaftLog.nextEnts(),
		Snapshot:         snapshot,
	}
	return node, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	r := Ready{
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
	}
	if rn.lastReady.Lead != rn.Raft.Lead || rn.lastReady.RaftState != rn.Raft.State {
		r.SoftState = &SoftState{Lead: rn.Raft.Lead, RaftState: rn.Raft.State}
		rn.lastReady.SoftState = &SoftState{Lead: rn.Raft.Lead, RaftState: rn.Raft.State}
	}
	if rn.lastReady.Term != rn.Raft.Term || rn.lastReady.Vote != rn.Raft.Vote ||
		rn.lastReady.Commit != rn.Raft.RaftLog.committed {
		r.HardState = pb.HardState{
			Term:   rn.Raft.Term,
			Vote:   rn.Raft.Vote,
			Commit: rn.Raft.RaftLog.committed,
		}
		rn.lastReady.HardState = pb.HardState{
			Term:   rn.Raft.Term,
			Vote:   rn.Raft.Vote,
			Commit: rn.Raft.RaftLog.committed,
		}
	}
	if len(rn.Raft.msgs) > 0 {
		r.Messages = rn.Raft.msgs
		// clear msgs
		rn.Raft.msgs = []pb.Message{}
	}
	return r
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	if rn.lastReady.Lead != rn.Raft.Lead || rn.lastReady.RaftState != rn.Raft.State {
		return true
	}
	if rn.lastReady.Term != rn.Raft.Term || rn.lastReady.Vote != rn.Raft.Vote ||
		rn.lastReady.Commit != rn.Raft.RaftLog.committed {
		return true
	}
	// check msg to be sent
	if len(rn.Raft.msgs) > 0 {
		return true
	}
	// check entries to be stabled or applied
	if len(rn.Raft.RaftLog.entries) > 0 {
		index := rn.Raft.RaftLog.entries[len(rn.Raft.RaftLog.entries)-1].Index
		if index > rn.Raft.RaftLog.stabled {
			return true
		}
	}
	// check entries to be applied
	return rn.Raft.RaftLog.applied < rn.Raft.RaftLog.committed
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// TODO(chapche) snapshot
	if len(rd.Entries) > 0 {
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	if rn.Raft.RaftLog.committed < rd.Commit {
		rn.Raft.RaftLog.committed = rd.Commit	
	}
	if len(rd.CommittedEntries) > 0 {
		lastAppliedIndex := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		rn.Raft.RaftLog.applied = lastAppliedIndex 
	}
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
