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
	Key        string    `json:"key"`
	Value      string    `json:"value"`
	AddTime    time.Time `json:"add_time"`
	DeleteTime time.Time `json:"delete_time"`
}

func (e stateentry) hasBeenDeleted() bool {
	return e.DeleteTime.After(e.AddTime)
}

func (e stateentry) merge(mergeEntry stateentry) (stateentry, bool) {
	if e.Key != mergeEntry.Key {
		panic(fmt.Errorf("Keys %s and %s don't match on merge", e.Key, mergeEntry.Key))
	}

	changed := false
	newEntry := stateentry{Key: e.Key}
	if mergeEntry.AddTime.After(e.AddTime) {
		newEntry.Value = mergeEntry.Value
		newEntry.AddTime = mergeEntry.AddTime
		changed = true
	} else {
		newEntry.Value = e.Value
		newEntry.AddTime = e.AddTime
	}
	if mergeEntry.DeleteTime.After(e.DeleteTime) {
		newEntry.DeleteTime = mergeEntry.DeleteTime
		changed = true
	} else {
		newEntry.DeleteTime = e.DeleteTime
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
			return entry.Value, true
		}
	}
	return "", false
}

func (st *state) del(key string) (complete *state) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if existing, found := st.entries[key]; found {
		st.entries[key] = stateentry{
			Key:        existing.Key,
			Value:      existing.Value,
			AddTime:    existing.AddTime,
			DeleteTime: time.Now(),
		}
	} else {
		st.entries[key] = stateentry{
			Key:        key,
			DeleteTime: time.Now(),
		}
	}

	return &state{
		entries: st.entries,
	}
}

func (st *state) set(key string, value string) (complete *state) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if existing, found := st.entries[key]; found {
		st.entries[key] = stateentry{
			Key:        key,
			Value:      value,
			AddTime:    time.Now(),
			DeleteTime: existing.DeleteTime,
		}
	} else {
		st.entries[key] = stateentry{
			Key:     key,
			Value:   value,
			AddTime: time.Now(),
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
		st.entries[newEntry.Key] = newEntry
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
				st.entries[entry.Key] = mergedEntry
			} else {
				delete(entries, key)
			}
		} else {
			st.entries[entry.Key] = entry
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
				st.entries[entry.Key] = mergedEntry
			} else {
				delete(entries, key)
			}
		} else {
			st.entries[entry.Key] = entry
		}
	}

	return &state{entries: st.entries}
}
