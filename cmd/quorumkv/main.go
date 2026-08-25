// Command quorumkv runs one QuorumKV Raft node as a real OS process.
//
// Usage:
//
//	quorumkv node --id 1 --listen 127.0.0.1:7001 --data ./data/node1 \
//	    --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003
//
// See docs/operations.md for the full operational reference.
package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "node":
		if err := runNode(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "quorumkv:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "quorumkv: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `quorumkv runs one QuorumKV Raft node.

Usage:

  quorumkv node --id ID --listen ADDR --data DIR [--peer ID=ADDR ...]

Run "quorumkv node --help" for flag details.
`)
}

// runNode is separated from main so the whole node lifecycle — flags,
// validation, storage recovery, startup, serving, graceful shutdown —
// reports errors through a single return path instead of scattered
// os.Exit calls.
func runNode(args []string) error {
	cfg, err := parseNodeConfig(args)
	if err != nil {
		return err
	}
	return serve(context.Background(), cfg)
}
