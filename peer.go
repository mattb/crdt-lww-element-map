package main

import (
	"log"

	"bytes"
	"encoding/json"

	"github.com/weaveworks/mesh"
)

var _ mesh.Gossiper = &peer{}

type peer struct {
	st      *state
	send    mesh.Gossip
	actions chan<- func()
	quit    chan struct{}
	logger  *log.Logger
}

func newPeer(self mesh.PeerName, logger *log.Logger) *peer {
	actions := make(chan func())
	p := &peer{
		st:      newState(),
		send:    nil, // must .register() later
		actions: actions,
		quit:    make(chan struct{}),
		logger:  logger,
	}
	go p.loop(actions)
	return p
}

func (p *peer) del(key string) {
	c := make(chan struct{})
	p.actions <- func() {
		defer close(c)
		st := p.st.del(key)
		if p.send != nil {
			p.send.GossipBroadcast(st)
		} else {
			p.logger.Printf("no sender configured; not broadcasting update right now")
		}
	}
	<-c
}

func (p *peer) set(key string, value string) (result string) {
	c := make(chan struct{})
	p.actions <- func() {
		defer close(c)
		st := p.st.set(key, value)
		if p.send != nil {
			p.send.GossipBroadcast(st)
		} else {
			p.logger.Printf("no sender configured; not broadcasting update right now")
		}
		result, _ = st.get(key)
	}
	<-c
	return result
}

func (p *peer) loop(actions <-chan func()) {
	for {
		select {
		case f := <-actions:
			f()
		case <-p.quit:
			return
		}
	}
}

func (p *peer) register(send mesh.Gossip) {
	p.actions <- func() { p.send = send }
}

func (p *peer) getAll() map[string]string {
	return p.st.getAll()
}

func (p *peer) get(key string) (string, bool) {
	return p.st.get(key)
}

func (p *peer) stop() {
	close(p.quit)
}

func (p *peer) Gossip() (complete mesh.GossipData) {
	complete = p.st.copy()
	p.logger.Printf("Gossip was called in peer => returned complete %v", complete.(*state).entries)
	return complete
}

func (p *peer) OnGossip(buf []byte) (delta mesh.GossipData, err error) {
	var entries map[string]stateentry
	if err := json.NewDecoder(bytes.NewReader(buf)).Decode(&entries); err != nil {
		return nil, err
	}
	p.logger.Printf("OnGossip received => %v", entries)

	delta = p.st.mergeDelta(entries)
	if delta == nil {
		p.logger.Printf("OnGossip nil delta %v => delta %v", entries, delta)
	} else {
		p.logger.Printf("OnGossip some delta %v => delta %v", entries, delta.(*state).entries)
	}
	return delta, nil
}

func (p *peer) OnGossipBroadcast(src mesh.PeerName, buf []byte) (received mesh.GossipData, err error) {
	var entries map[string]stateentry
	if err := json.NewDecoder(bytes.NewReader(buf)).Decode(&entries); err != nil {
		return nil, err
	}

	p.logger.Printf("OnGossipBroadcast received => %v", entries)
	received = p.st.mergeReceived(entries)
	if received == nil {
		p.logger.Printf("OnGossipBroadcast %s %v => delta %v", src, entries, received)
	} else {
		p.logger.Printf("OnGossipBroadcast %s %v => delta %v", src, entries, received.(*state).entries)
	}
	return received, nil
}

func (p *peer) OnGossipUnicast(src mesh.PeerName, buf []byte) error {
	var entries map[string]stateentry
	if err := json.NewDecoder(bytes.NewReader(buf)).Decode(&entries); err != nil {
		return err
	}

	p.logger.Printf("OnGossipUnicast received => %v", entries)
	complete := p.st.mergeComplete(entries)
	p.logger.Printf("OnGossipUnicast called mergeComplete %s with %v => complete %v", src, entries, complete)
	return nil
}
