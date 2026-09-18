package raft

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func segmentFiles(t *testing.T, l *Log) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(generationDir(l.path, l.generation), "*.seg"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func largeSegmentedLog(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log")
	l, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := scalingEntries(5000, 1024)
	if err := l.Append(entries); err != nil {
		t.Fatal(err)
	}
	if got := len(segmentFiles(t, l)); got < 2 {
		t.Fatalf("segments = %d, want multiple", got)
	}
	return l, path
}

func TestSegmentedLogRotationAndReopenPreservesKinds(t *testing.T) {
	l, path := largeSegmentedLog(t)
	config := LogEntry{Term: 9, Kind: EntryConfiguration, Command: []byte("config")}
	noop := LogEntry{Term: 9, Kind: EntryNoop}
	if err := l.Append([]LogEntry{config, noop}); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.LastIndex() != 5002 {
		t.Fatalf("last index = %d", reopened.LastIndex())
	}
	for index, kind := range map[LogIndex]EntryKind{5001: EntryConfiguration, 5002: EntryNoop} {
		entry, ok := reopened.Entry(index)
		if !ok || entry.Kind != kind {
			t.Fatalf("entry %d = %+v, %v", index, entry, ok)
		}
	}
}

func TestSegmentedLogRecoversOnlyTornActiveTail(t *testing.T) {
	l, path := largeSegmentedLog(t)
	before := l.LastIndex()
	f, err := os.OpenFile(l.activeSegment, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	reopened, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.LastIndex() != before {
		t.Fatalf("last index = %d, want %d", reopened.LastIndex(), before)
	}
	info, _ := os.Stat(reopened.activeSegment)
	if info.Size() != l.activeSize {
		t.Fatalf("recovered size = %d, want %d", info.Size(), l.activeSize)
	}
}

func TestSegmentedLogDiscardsWholeUncommittedTailBatch(t *testing.T) {
	l, path := largeSegmentedLog(t)
	before := l.LastIndex()
	f, err := os.OpenFile(l.activeSegment, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range []LogEntry{{Term: 8, Command: []byte("first")}, {Term: 8, Command: []byte("second")}} {
		record, err := encodeEntryRecord(entry)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	commit, err := encodeBatchCommit(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(commit[:len(commit)-2]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.LastIndex() != before {
		t.Fatalf("last index = %d, want pre-batch %d", reopened.LastIndex(), before)
	}
}

func TestSegmentedLogRejectsMidLogCorruption(t *testing.T) {
	l, path := largeSegmentedLog(t)
	files := segmentFiles(t, l)
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	data[segmentHeaderSize+logLengthPrefixSize+2] ^= 0xff
	if err := os.WriteFile(files[0], data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLog(path); !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("OpenLog = %v, want ErrCorruptLog", err)
	}
}

func TestSegmentedLogRejectsGapAndCorruptManifest(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		l, path := largeSegmentedLog(t)
		files := segmentFiles(t, l)
		if err := os.Remove(files[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenLog(path); !errors.Is(err, ErrCorruptLog) {
			t.Fatalf("OpenLog = %v, want ErrCorruptLog", err)
		}
	})
	t.Run("manifest", func(t *testing.T) {
		_, path := largeSegmentedLog(t)
		data, _ := os.ReadFile(manifestPath(path))
		data[len(data)-1] ^= 0xff
		os.WriteFile(manifestPath(path), data, 0o600)
		if _, err := OpenLog(path); !errors.Is(err, ErrCorruptLog) {
			t.Fatalf("OpenLog = %v, want ErrCorruptLog", err)
		}
	})
}

func TestSegmentedLogRejectsMalformedSegments(t *testing.T) {
	tests := map[string]func(*testing.T, []string){
		"truncated header": func(t *testing.T, files []string) {
			if err := os.Truncate(files[0], segmentHeaderSize-1); err != nil {
				t.Fatal(err)
			}
		},
		"corrupt length": func(t *testing.T, files []string) {
			data, err := os.ReadFile(files[0])
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint32(data[segmentHeaderSize:], maxLogRecordSize+1)
			if err := os.WriteFile(files[0], data, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"truncated non-active body": func(t *testing.T, files []string) {
			info, err := os.Stat(files[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(files[0], info.Size()-2); err != nil {
				t.Fatal(err)
			}
		},
		"overlapping header": func(t *testing.T, files []string) {
			data, err := os.ReadFile(files[1])
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint64(data[5:13], 1)
			if err := os.WriteFile(files[1], data, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"name header mismatch": func(t *testing.T, files []string) {
			wrong := filepath.Join(filepath.Dir(files[0]), "00000000000000000002.seg")
			if err := os.Rename(files[0], wrong); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			l, path := largeSegmentedLog(t)
			files := segmentFiles(t, l)
			corrupt(t, files)
			if _, err := OpenLog(path); !errors.Is(err, ErrCorruptLog) {
				t.Fatalf("OpenLog = %v, want ErrCorruptLog", err)
			}
		})
	}
}

func TestSegmentedLogTruncateAndCompactAcrossSegments(t *testing.T) {
	l, path := largeSegmentedLog(t)
	replacement := []LogEntry{{Term: 99, Command: []byte("replacement")}}
	if err := l.TruncateAndAppend(2000, replacement); err != nil {
		t.Fatal(err)
	}
	if l.LastIndex() != 2000 {
		t.Fatalf("last after truncate = %d", l.LastIndex())
	}
	if err := l.Append(scalingEntries(3000, 1024)); err != nil {
		t.Fatal(err)
	}
	term, _ := l.Term(3000)
	if err := l.Compact(3000, term); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.BaseIndex() != 3000 || reopened.LastIndex() != 5000 {
		t.Fatalf("base/last = %d/%d", reopened.BaseIndex(), reopened.LastIndex())
	}
}

func TestSegmentedLogReusesUnaffectedSegments(t *testing.T) {
	t.Run("truncate keeps prefix", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "log")
		l, err := OpenLog(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Append(scalingEntries(10000, 1024)); err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(segmentFiles(t, l)[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := l.TruncateAndAppend(9000, []LogEntry{{Term: 99, Command: []byte("replacement")}}); err != nil {
			t.Fatal(err)
		}
		after, err := os.Stat(segmentFiles(t, l)[0])
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(before, after) {
			t.Fatal("unaffected prefix segment was rewritten")
		}
	})

	t.Run("compaction keeps suffix", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "log")
		l, err := OpenLog(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Append(scalingEntries(10000, 1024)); err != nil {
			t.Fatal(err)
		}
		files := segmentFiles(t, l)
		before, err := os.Stat(files[len(files)-1])
		if err != nil {
			t.Fatal(err)
		}
		term, _ := l.Term(1000)
		if err := l.Compact(1000, term); err != nil {
			t.Fatal(err)
		}
		files = segmentFiles(t, l)
		after, err := os.Stat(files[len(files)-1])
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(before, after) {
			t.Fatal("unaffected suffix segment was rewritten")
		}
	})
}
