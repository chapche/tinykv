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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	log := &RaftLog{
		storage:   storage,
		committed: 0,
		applied:   0, // should be volitile, rebuild from log entries
		stabled:   0,
		entries:   make([]pb.Entry, 0),
	}
	hardState, _, err := storage.InitialState()
	if err != nil {
		return nil
	}
	log.committed = hardState.Commit
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil
	}
	log.stabled = lastIndex
	firstIndex, err := storage.FirstIndex()

	if err == nil && firstIndex <= lastIndex {
		entries, err2 := storage.Entries(firstIndex, lastIndex+1)
		if err2 == nil {
			for _, entry := range entries {
				if entry.Term > 0 && entry.Index > 0 {
					log.entries = append(log.entries, entry)
				}
			}
		}
	}
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Get the first index from storage (entries before this have been compacted)
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return
	}
	// Remove entries that have been compacted in storage
	if len(l.entries) > 0 && l.entries[0].Index < firstIndex {
		// Find the position where we should start keeping entries
		for i := range l.entries {
			if l.entries[i].Index >= firstIndex {
				l.entries = l.entries[i:]
				return
			}
		}
		// All entries have been compacted
		l.entries = nil
	}
}

// matchTerm checks if a log entry at the given index has the given term
func (l *RaftLog) matchTerm(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == term
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	res := make([]pb.Entry, 0, len(l.entries))
	res = append(res, l.entries...)
	return res
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// we don't evict stabled entry from memory since stabled >= committed >= applied
	// it could be used by application even if it's stabled.
	res := make([]pb.Entry, 0, len(l.entries))
	for _, entry := range l.entries {
		if entry.Index > l.stabled {
			res = append(res, entry)
		}
	}
	return res
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// stabled >= committed >= applied
	// get from storage
	res := make([]pb.Entry, 0, l.committed-l.applied+1)
	for _, entry := range l.entries {
		if entry.Index > l.applied && entry.Index <= l.committed {
			res = append(res, entry)
		}
	}
	return res
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	unstable := uint64(len(l.entries))
	if unstable > 0 {
		return l.entries[unstable-1].Index
	}
	// Check pendingSnapshot first
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	stable, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	return stable
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Check pendingSnapshot first
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		// If index is before snapshot, it's compacted
		if i < l.pendingSnapshot.Metadata.Index {
			return 0, ErrCompacted
		}
	}
	// Check unstable entries
	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, nil
		}
	}
	// Check storage
	stableTerm, err := l.storage.Term(i)
	if err == nil {
		return stableTerm, nil
	}
	return 0, err
}
