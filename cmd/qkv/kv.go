package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"quorumkv/internal/client"
)

// newClient builds a client.Client seeded with gf's addresses. Every
// invocation of qkv is a fresh process, so this always generates a new
// random ClientID (see internal/client.New) — there is no cross-process
// client-session persistence in this CLI. A PUT/DELETE within a single
// qkv invocation is still safely retried by internal/client itself
// (transport failure, TIMEOUT, BUSY, NOT_LEADER redirect); what does NOT
// survive is running qkv a second time to "retry" a write whose result
// you never saw — that is a new ClientID/Sequence and, if the original
// write actually landed, a second logical write. See docs/operations.md.
func newClient(gf globalFlags) *client.Client {
	return client.New(gf.addrs...)
}

func cmdPut(gf globalFlags, args []string) int {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: qkv put KEY VALUE")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()
	if err := newClient(gf).Put(ctx, []byte(args[0]), []byte(args[1])); err != nil {
		fmt.Fprintln(os.Stderr, humanClientError(err))
		return exitFailure
	}
	fmt.Println("OK")
	return exitOK
}

func cmdDelete(gf globalFlags, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: qkv delete KEY")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()
	if err := newClient(gf).Delete(ctx, []byte(args[0])); err != nil {
		fmt.Fprintln(os.Stderr, humanClientError(err))
		return exitFailure
	}
	fmt.Println("OK")
	return exitOK
}

func cmdGet(gf globalFlags, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: qkv get KEY")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()
	value, ok, err := newClient(gf).Get(ctx, []byte(args[0]))
	if err != nil {
		fmt.Fprintln(os.Stderr, humanClientError(err))
		return exitFailure
	}
	if !ok {
		fmt.Println("not found")
		return exitNotFound
	}
	fmt.Println(string(value))
	return exitOK
}

// humanClientError turns an internal/client error into the kind of
// message an operator can act on without knowing this codebase's
// internal status codes — see docs/operations.md.
func humanClientError(err error) string {
	switch {
	case errors.Is(err, client.ErrNoLeaderKnown):
		return "qkv: no leader currently known; is the cluster up and has it elected a leader?"
	case errors.Is(err, client.ErrTimeout):
		return "qkv: timed out waiting for the server; outcome is uncertain (safe to retry this same qkv command)"
	case errors.Is(err, client.ErrBusy):
		return "qkv: server is busy (overloaded); try again shortly"
	case errors.Is(err, client.ErrTooManyRedirects):
		return "qkv: too many leader redirects; the cluster may be electing a new leader, try again shortly"
	case errors.Is(err, client.ErrBadRequest):
		return "qkv: request rejected as invalid (key/value too large?)"
	case errors.Is(err, client.ErrRequestConflict):
		return "qkv: internal error: request identity conflict (this should not happen within one qkv invocation)"
	case errors.Is(err, client.ErrStaleRequest):
		return "qkv: internal error: stale request sequence (this should not happen within one qkv invocation)"
	case errors.Is(err, context.DeadlineExceeded):
		return "qkv: operation timed out (see --timeout); outcome is uncertain"
	case errors.Is(err, context.Canceled):
		return "qkv: operation canceled"
	default:
		return "qkv: " + err.Error()
	}
}
