// Command qkv is QuorumKV's client and administration CLI.
//
// Usage:
//
//	qkv --addr 127.0.0.1:7001 put x 1
//	qkv --addr 127.0.0.1:7001 get x
//	qkv --addr 127.0.0.1:7001 delete x
//	qkv --addr 127.0.0.1:7001 status
//
// See docs/operations.md for the full command reference.
package main

import (
	"fmt"
	"os"
)

// Exit codes. Kept small and documented rather than a large taxonomy —
// see docs/operations.md.
const (
	exitOK       = 0
	exitFailure  = 1
	exitUsage    = 2
	exitNotFound = 3
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return exitUsage
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		usage()
		return exitOK
	}

	// Global flags (--addr, --timeout) come before the command name;
	// each command's own flags (e.g. transfer-leadership's --target)
	// come after it and are parsed separately by that command.
	gf, rest, err := parseGlobalFlags(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "qkv:", err)
		return exitUsage
	}
	if len(rest) == 0 {
		usage()
		return exitUsage
	}
	cmd := rest[0]
	rest = rest[1:]

	switch cmd {
	case "put":
		return cmdPut(gf, rest)
	case "get":
		return cmdGet(gf, rest)
	case "delete":
		return cmdDelete(gf, rest)
	case "status":
		return cmdStatus(gf, rest)
	case "snapshot":
		return cmdSnapshot(gf, rest)
	case "transfer-leadership":
		return cmdTransferLeadership(gf, rest)
	case "add-voter":
		return cmdAddVoter(gf, rest)
	case "remove-voter":
		return cmdRemoveVoter(gf, rest)
	case "version":
		fmt.Println(version)
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "qkv: unknown command %q\n\n", cmd)
		usage()
		return exitUsage
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `qkv is QuorumKV's client and administration CLI.

Usage:

  qkv --addr ADDR [--addr ADDR ...] [--timeout DURATION] COMMAND [ARGS]

Commands:

  put KEY VALUE          write a key
  get KEY                read a key
  delete KEY              delete a key
  status                  show this node's operational status
  snapshot                trigger snapshot creation (leader only)
  transfer-leadership --target ID     hand off leadership (leader only)
  add-voter --id ID --peer-address ADDR   add a voter (leader only)
  remove-voter --id ID    remove a voter (leader only)
  version                 print the CLI version

Flags:

  --addr      node address to contact; repeat for multiple seed addresses
  --timeout   operation timeout (default 5s)

Examples:

  qkv --addr 127.0.0.1:7001 put x 1
  qkv --addr 127.0.0.1:7001 get x
  qkv --addr 127.0.0.1:7001 status
`)
}
