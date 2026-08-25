package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"quorumkv/internal/raft"
)

// nodeConfig is the validated result of parsing "quorumkv node" flags.
type nodeConfig struct {
	id     raft.NodeID
	listen string
	data   string
	peers  map[raft.NodeID]string
}

// peerFlag implements flag.Value for a repeated "--peer ID=ADDR" flag.
type peerFlag struct {
	peers map[raft.NodeID]string
}

func (p *peerFlag) String() string {
	if p.peers == nil {
		return ""
	}
	parts := make([]string, 0, len(p.peers))
	for id, addr := range p.peers {
		parts = append(parts, fmt.Sprintf("%d=%s", id, addr))
	}
	return strings.Join(parts, ",")
}

func (p *peerFlag) Set(s string) error {
	idStr, addr, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("malformed --peer %q: want ID=ADDRESS", s)
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || id == 0 {
		return fmt.Errorf("malformed --peer %q: node ID must be a positive integer", s)
	}
	if addr == "" {
		return fmt.Errorf("malformed --peer %q: address is empty", s)
	}
	if p.peers == nil {
		p.peers = make(map[raft.NodeID]string)
	}
	if existing, ok := p.peers[raft.NodeID(id)]; ok {
		return fmt.Errorf("duplicate --peer for node %d (already %q, got %q)", id, existing, addr)
	}
	for peerID, peerAddr := range p.peers {
		if peerAddr == addr {
			return fmt.Errorf("duplicate --peer address %q (already used for node %d)", addr, peerID)
		}
	}
	p.peers[raft.NodeID(id)] = addr
	return nil
}

// parseNodeConfig parses and validates "quorumkv node" flags. Rejects a
// malformed --peer, a duplicate peer NodeID or address, a zero/missing
// --id, this node's own ID given as a --peer, and any missing required
// flag — nothing is silently ignored or defaulted around.
func parseNodeConfig(args []string) (nodeConfig, error) {
	fs := flag.NewFlagSet("quorumkv node", flag.ContinueOnError)
	var idRaw uint64
	var listen, data string
	var peers peerFlag
	fs.Uint64Var(&idRaw, "id", 0, "this node's ID (positive integer, required)")
	fs.StringVar(&listen, "listen", "", "address to listen on, e.g. 127.0.0.1:7001 (required)")
	fs.StringVar(&data, "data", "", "directory for this node's persistent state (required)")
	fs.Var(&peers, "peer", "peer as ID=ADDRESS; repeat for each peer")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `Usage: quorumkv node --id ID --listen ADDR --data DIR [--peer ID=ADDR ...]

  --id      this node's ID (positive integer, required)
  --listen  address to listen on, e.g. 127.0.0.1:7001 (required)
  --data    directory for this node's persistent state (required)
  --peer    a peer as ID=ADDRESS; repeat once per peer

Example:

  quorumkv node --id 1 --listen 127.0.0.1:7001 --data ./data/node1 \
      --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003
`)
	}
	if err := fs.Parse(args); err != nil {
		return nodeConfig{}, err
	}

	if idRaw == 0 {
		return nodeConfig{}, fmt.Errorf("--id is required and must be a positive integer")
	}
	id := raft.NodeID(idRaw)
	if listen == "" {
		return nodeConfig{}, fmt.Errorf("--listen is required")
	}
	if data == "" {
		return nodeConfig{}, fmt.Errorf("--data is required")
	}
	if _, ok := peers.peers[id]; ok {
		return nodeConfig{}, fmt.Errorf("--peer includes this node's own --id (%d); a node is never its own peer", id)
	}

	return nodeConfig{id: id, listen: listen, data: data, peers: peers.peers}, nil
}
