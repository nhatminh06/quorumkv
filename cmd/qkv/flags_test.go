package main

import "testing"

func TestParseGlobalFlagsValid(t *testing.T) {
	gf, rest, err := parseGlobalFlags([]string{"--addr", "127.0.0.1:7001", "--timeout", "2s", "put", "x", "1"})
	if err != nil {
		t.Fatalf("parseGlobalFlags: %v", err)
	}
	if len(gf.addrs) != 1 || gf.addrs[0] != "127.0.0.1:7001" {
		t.Fatalf("addrs = %v, want [127.0.0.1:7001]", gf.addrs)
	}
	if gf.timeout.Seconds() != 2 {
		t.Fatalf("timeout = %v, want 2s", gf.timeout)
	}
	if len(rest) != 3 || rest[0] != "put" || rest[1] != "x" || rest[2] != "1" {
		t.Fatalf("rest = %v, want [put x 1]", rest)
	}
}

func TestParseGlobalFlagsMultipleAddrs(t *testing.T) {
	gf, _, err := parseGlobalFlags([]string{"--addr", "127.0.0.1:7001", "--addr", "127.0.0.1:7002", "status"})
	if err != nil {
		t.Fatalf("parseGlobalFlags: %v", err)
	}
	if len(gf.addrs) != 2 {
		t.Fatalf("addrs = %v, want 2 entries", gf.addrs)
	}
}

func TestParseGlobalFlagsDefaultTimeout(t *testing.T) {
	gf, _, err := parseGlobalFlags([]string{"--addr", "127.0.0.1:7001", "status"})
	if err != nil {
		t.Fatalf("parseGlobalFlags: %v", err)
	}
	if gf.timeout != defaultTimeout {
		t.Fatalf("timeout = %v, want default %v", gf.timeout, defaultTimeout)
	}
}

func TestParseGlobalFlagsMissingAddr(t *testing.T) {
	_, _, err := parseGlobalFlags([]string{"status"})
	if err == nil {
		t.Fatal("parseGlobalFlags without --addr succeeded, want error")
	}
}

func TestParseGlobalFlagsInvalidTimeout(t *testing.T) {
	_, _, err := parseGlobalFlags([]string{"--addr", "127.0.0.1:7001", "--timeout", "not-a-duration", "status"})
	if err == nil {
		t.Fatal("parseGlobalFlags with invalid --timeout succeeded, want error")
	}
}

func TestParseGlobalFlagsZeroTimeout(t *testing.T) {
	_, _, err := parseGlobalFlags([]string{"--addr", "127.0.0.1:7001", "--timeout", "0s", "status"})
	if err == nil {
		t.Fatal("parseGlobalFlags with --timeout 0s succeeded, want error")
	}
}

func TestParseGlobalFlagsNegativeTimeout(t *testing.T) {
	_, _, err := parseGlobalFlags([]string{"--addr", "127.0.0.1:7001", "--timeout", "-1s", "status"})
	if err == nil {
		t.Fatal("parseGlobalFlags with negative --timeout succeeded, want error")
	}
}

func TestRunUnknownCommand(t *testing.T) {
	code := run([]string{"--addr", "127.0.0.1:7001", "bogus-command"})
	if code != exitUsage {
		t.Fatalf("run(unknown command) = %d, want %d", code, exitUsage)
	}
}

func TestRunNoArgs(t *testing.T) {
	if code := run(nil); code != exitUsage {
		t.Fatalf("run(nil) = %d, want %d", code, exitUsage)
	}
}

func TestRunHelp(t *testing.T) {
	if code := run([]string{"--help"}); code != exitOK {
		t.Fatalf("run(--help) = %d, want %d", code, exitOK)
	}
}

func TestRunPutWrongArgCount(t *testing.T) {
	code := run([]string{"--addr", "127.0.0.1:7001", "put", "onlykey"})
	if code != exitUsage {
		t.Fatalf("run(put with 1 arg) = %d, want %d", code, exitUsage)
	}
}

func TestRunGetWrongArgCount(t *testing.T) {
	code := run([]string{"--addr", "127.0.0.1:7001", "get"})
	if code != exitUsage {
		t.Fatalf("run(get with 0 args) = %d, want %d", code, exitUsage)
	}
}

func TestRunVersion(t *testing.T) {
	if code := run([]string{"--addr", "127.0.0.1:7001", "version"}); code != exitOK {
		t.Fatalf("run(version) = %d, want %d", code, exitOK)
	}
}
