package main

import "testing"

func TestParseNodeConfigValid(t *testing.T) {
	cfg, err := parseNodeConfig([]string{
		"--id", "1",
		"--listen", "127.0.0.1:7001",
		"--data", "/tmp/data1",
		"--peer", "2=127.0.0.1:7002",
		"--peer", "3=127.0.0.1:7003",
	})
	if err != nil {
		t.Fatalf("parseNodeConfig: %v", err)
	}
	if cfg.id != 1 || cfg.listen != "127.0.0.1:7001" || cfg.data != "/tmp/data1" {
		t.Fatalf("cfg = %+v, want id=1 listen=127.0.0.1:7001 data=/tmp/data1", cfg)
	}
	if len(cfg.peers) != 2 || cfg.peers[2] != "127.0.0.1:7002" || cfg.peers[3] != "127.0.0.1:7003" {
		t.Fatalf("cfg.peers = %v, want {2:127.0.0.1:7002, 3:127.0.0.1:7003}", cfg.peers)
	}
}

func TestParseNodeConfigMissingID(t *testing.T) {
	_, err := parseNodeConfig([]string{"--listen", "127.0.0.1:7001", "--data", "/tmp/data1"})
	if err == nil {
		t.Fatal("parseNodeConfig without --id succeeded, want error")
	}
}

func TestParseNodeConfigZeroID(t *testing.T) {
	_, err := parseNodeConfig([]string{"--id", "0", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1"})
	if err == nil {
		t.Fatal("parseNodeConfig with --id 0 succeeded, want error")
	}
}

func TestParseNodeConfigMissingListen(t *testing.T) {
	_, err := parseNodeConfig([]string{"--id", "1", "--data", "/tmp/data1"})
	if err == nil {
		t.Fatal("parseNodeConfig without --listen succeeded, want error")
	}
}

func TestParseNodeConfigMissingData(t *testing.T) {
	_, err := parseNodeConfig([]string{"--id", "1", "--listen", "127.0.0.1:7001"})
	if err == nil {
		t.Fatal("parseNodeConfig without --data succeeded, want error")
	}
}

func TestParseNodeConfigMalformedPeerNoEquals(t *testing.T) {
	_, err := parseNodeConfig([]string{
		"--id", "1", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1",
		"--peer", "2-127.0.0.1:7002",
	})
	if err == nil {
		t.Fatal("parseNodeConfig with malformed --peer succeeded, want error")
	}
}

func TestParseNodeConfigPeerNonNumericID(t *testing.T) {
	_, err := parseNodeConfig([]string{
		"--id", "1", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1",
		"--peer", "x=127.0.0.1:7002",
	})
	if err == nil {
		t.Fatal("parseNodeConfig with non-numeric peer ID succeeded, want error")
	}
}

func TestParseNodeConfigPeerZeroID(t *testing.T) {
	_, err := parseNodeConfig([]string{
		"--id", "1", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1",
		"--peer", "0=127.0.0.1:7002",
	})
	if err == nil {
		t.Fatal("parseNodeConfig with --peer 0=... succeeded, want error")
	}
}

func TestParseNodeConfigPeerEmptyAddress(t *testing.T) {
	_, err := parseNodeConfig([]string{
		"--id", "1", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1",
		"--peer", "2=",
	})
	if err == nil {
		t.Fatal("parseNodeConfig with empty peer address succeeded, want error")
	}
}

func TestParseNodeConfigDuplicatePeerID(t *testing.T) {
	_, err := parseNodeConfig([]string{
		"--id", "1", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1",
		"--peer", "2=127.0.0.1:7002",
		"--peer", "2=127.0.0.1:9999",
	})
	if err == nil {
		t.Fatal("parseNodeConfig with duplicate peer ID succeeded, want error")
	}
}

func TestParseNodeConfigDuplicatePeerAddress(t *testing.T) {
	_, err := parseNodeConfig([]string{
		"--id", "1", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1",
		"--peer", "2=127.0.0.1:7002",
		"--peer", "3=127.0.0.1:7002",
	})
	if err == nil {
		t.Fatal("parseNodeConfig with duplicate peer address succeeded, want error")
	}
}

func TestParseNodeConfigOwnIDAsPeer(t *testing.T) {
	_, err := parseNodeConfig([]string{
		"--id", "1", "--listen", "127.0.0.1:7001", "--data", "/tmp/data1",
		"--peer", "1=127.0.0.1:7099",
	})
	if err == nil {
		t.Fatal("parseNodeConfig with own --id as a --peer succeeded, want error")
	}
}
