package main

import (
	"flag"
	"fmt"
	"time"
)

const defaultTimeout = 5 * time.Second

// globalFlags are the flags common to every qkv command: which node(s)
// to contact, and how long to wait.
type globalFlags struct {
	addrs   []string
	timeout time.Duration
}

// addrList implements flag.Value for a repeated "--addr" flag.
type addrList struct {
	values *[]string
}

func (a addrList) String() string {
	if a.values == nil {
		return ""
	}
	return fmt.Sprint(*a.values)
}

func (a addrList) Set(s string) error {
	if s == "" {
		return fmt.Errorf("--addr must not be empty")
	}
	*a.values = append(*a.values, s)
	return nil
}

// parseGlobalFlags parses --addr (repeatable, at least one required) and
// --timeout from the front of args, returning the remaining
// (command-name-and-its-own-args) arguments unconsumed.
func parseGlobalFlags(args []string) (globalFlags, []string, error) {
	fs := flag.NewFlagSet("qkv", flag.ContinueOnError)
	var addrs []string
	timeout := defaultTimeout
	fs.Var(addrList{&addrs}, "addr", "node address to contact; repeat for multiple seed addresses")
	fs.DurationVar(&timeout, "timeout", defaultTimeout, "operation timeout")
	fs.Usage = usage
	if err := fs.Parse(args); err != nil {
		return globalFlags{}, nil, err
	}
	if len(addrs) == 0 {
		return globalFlags{}, nil, fmt.Errorf("--addr is required (at least one)")
	}
	if timeout <= 0 {
		return globalFlags{}, nil, fmt.Errorf("--timeout must be positive, got %s", timeout)
	}
	return globalFlags{addrs: addrs, timeout: timeout}, fs.Args(), nil
}
