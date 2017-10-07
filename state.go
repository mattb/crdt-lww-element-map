package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/weaveworks/mesh"
	"sync"
	"time"
)

var _ mesh.GossipData = &state{}

type stateentry struct {
	key        string
	value      string
	addTime    time.Time
	deleteTime time.Time
}

func (e stateentry) hasBeenDeleted() bool {
	return e.deleteTime.After(e.addTime)
}

func (e stateentry) merge(mergeEntry stateentry) (stateentry, bool) {
	if e.key != mergeEntry.key {
		panic(fmt.Errorf("Keys %s and %s don't match on merge", e.key, mergeEntry.key))
	}

	changed := false
	newEntry := stateentry{key: e.key}
	if mergeEntry.addTime.After(e.addTime) {
		newEntry.value = mergeEntry.value
		newEntry.addTime = mergeEntry.addTime
		changed = true
	} else {
		newEntry.value = e.value
		newEntry.addTime = e.addTime
	}
	if mergeEntry.deleteTime.After(e.deleteTime) {
		newEntry.deleteTime = mergeEntry.deleteTime
		changed = true
	} else {
		newEntry.deleteTime = e.deleteTime
	}
	return newEntry, changed
}

type state struct {
	mu      sync.RWMutex
	entries map[string]stateentry
}

func newState() *state {
	return &state{
		entries: map[string]stateentry{},
	}
}

func (st *state) copy() *state {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return &state{
		entries: st.entries,
	}
}

func (st *state) get(key string) (string, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	if entry, found := st.entries[key]; found {
		if !entry.hasBeenDeleted() {
			return entry.value, true
		}
	}
	return "", false
}

func (st *state) delete(key string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if existing, found := st.entries[key]; found {
		st.entries[key] = stateentry{
			key:        existing.key,
			value:      existing.value,
			addTime:    existing.addTime,
			deleteTime: time.Now(),
		}
	} else {
		st.entries[key] = stateentry{
			key:        key,
			deleteTime: time.Now(),
		}
	}
}

func (st *state) set(key string, value string) (complete *state) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if existing, found := st.entries[key]; found {
		st.entries[key] = stateentry{
			key:        key,
			value:      value,
			addTime:    time.Now(),
			deleteTime: existing.deleteTime,
		}
	} else {
		st.entries[key] = stateentry{
			key:     key,
			value:   value,
			addTime: time.Now(),
		}
	}

	return &state{
		entries: st.entries,
	}
}

func (st *state) Encode() [][]byte {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(st.entries); err != nil {
		panic(err)
	}
	fmt.Println(buf.String())
	return [][]byte{buf.Bytes()}
}

func (st *state) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	return st.mergeComplete(other.(*state).copy().entries)
}

func (st *state) mergeComplete(toMerge map[string]stateentry) (complete mesh.GossipData) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for mergeKey, mergeEntry := range toMerge {
		var newEntry stateentry
		if existingEntry, found := st.entries[mergeKey]; found {
			newEntry, _ = existingEntry.merge(mergeEntry)
		} else {
			newEntry = mergeEntry
		}
		st.entries[newEntry.key] = newEntry
	}

	return &state{entries: st.entries}
}

// Merge the set into our state, abiding increment-only semantics.
// Return any key/values that have been mutated, or nil if nothing changed.
func (st *state) mergeDelta(entries map[string]stateentry) (delta mesh.GossipData) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for key, entry := range entries {
		if existingEntry, found := st.entries[key]; found {
			if mergedEntry, changed := existingEntry.merge(entry); changed {
				st.entries[entry.key] = mergedEntry
			} else {
				delete(entries, key)
			}
		} else {
			st.entries[entry.key] = entry
		}
	}

	if len(entries) <= 0 {
		return nil // per OnGossip requirements
	}

	return &state{entries: st.entries}
}

// Merge the set into our state, abiding semantics.
// Return a non-nil mesh.GossipData representation of the received entries.
func (st *state) mergeReceived(entries map[string]stateentry) (received mesh.GossipData) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for key, entry := range entries {
		if existingEntry, found := st.entries[key]; found {
			if mergedEntry, changed := existingEntry.merge(entry); changed {
				st.entries[entry.key] = mergedEntry
			} else {
				delete(entries, key)
			}
		} else {
			st.entries[entry.key] = entry
		}
	}

	return &state{entries: st.entries}
}
