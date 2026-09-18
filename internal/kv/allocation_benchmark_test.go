package kv

import (
	"fmt"
	"testing"

	"quorumkv/internal/reqid"
)

func benchmarkCommand(size int) Command {
	var id reqid.ClientID
	id[0] = 1
	return Command{Type: CommandPut, ClientID: id, Sequence: 1, Key: []byte("key"), Value: make([]byte, size)}
}

func BenchmarkAllocationFingerprint(b *testing.B) {
	for _, size := range []int{16, 1024, 16 * 1024} {
		b.Run(fmt.Sprintf("value=%dB", size), func(b *testing.B) {
			cmd := benchmarkCommand(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = Fingerprint(cmd)
			}
		})
	}
}

func BenchmarkAllocationCommandCodec(b *testing.B) {
	for _, size := range []int{16, 1024, 16 * 1024} {
		cmd := benchmarkCommand(size)
		encoded, err := EncodeCommand(cmd)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("encode/value=%dB", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := EncodeCommand(cmd); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("decode/value=%dB", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := DecodeCommand(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAllocationStateMachinePut(b *testing.B) {
	for _, size := range []int{16, 1024, 16 * 1024} {
		b.Run(fmt.Sprintf("value=%dB", size), func(b *testing.B) {
			cmd := benchmarkCommand(size)
			cmd.ClientID = reqid.ClientID{}
			cmd.Sequence = 0
			m := NewStateMachine()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.Apply(cmd)
			}
		})
	}
}
