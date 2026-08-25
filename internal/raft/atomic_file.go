package raft

import (
	"errors"
	"os"
	"path/filepath"
)

// atomicWriteFile replaces the file at path with data: it writes to a
// temp file in the same directory, fsyncs it, closes it, renames it into
// place (an atomic replace on POSIX filesystems), then fsyncs the
// containing directory so the rename itself survives a crash. A reader
// always observes either the previous complete file or the new complete
// file, never a partial write. Shared by every durable file this package
// owns — PersistentState's Store, the Raft Log, CommitStore, and
// SnapshotStore all rewrite a whole file on every mutation rather than
// appending, so this one function is the entire durable-publication
// contract; see docs/crash-consistency.md.
//
// domain identifies the caller for failpoint naming only (e.g. "stable",
// "log", "commit", "snapshot") — it has no effect on behavior. It is
// combined with a stage name ("before-temp-write", "after-temp-write",
// "after-temp-fsync", "after-rename", "after-dir-fsync") to form
// failpoint names like "stable.after-rename", checked via
// atomicFileFailpoint — nil in production, so production behavior is
// always exactly the real write/fsync/rename/dir-fsync sequence
// described above. Only test code (see crashpoint_test.go and the
// subprocess helper in cmd_crashhelper_test.go) ever sets it.
func atomicWriteFile(domain, path string, data []byte) error {
	if err := checkFailpoint(domain, "before-temp-write"); err != nil {
		return err
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename below succeeds

	if err := writeFull(tmp, data); err != nil {
		tmp.Close()
		return err
	}
	if err := checkFailpoint(domain, "after-temp-write"); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := checkFailpoint(domain, "after-temp-fsync"); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err := checkFailpoint(domain, "after-rename"); err != nil {
		return err
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return err
	}
	return checkFailpoint(domain, "after-dir-fsync")
}

// errNoWriteProgress is returned by writeFull if the underlying Writer
// reports writing zero bytes without an error — looping on that would
// hang forever, so this is treated as a hard failure instead. A
// conforming io.Writer (which os.File is) is documented to never
// actually do this — Write "must return a non-nil error if it returns
// n < len(p)" — but writeFull does not trust that by convention alone;
// see TestWriteFullHandlesShortWrites.
var errNoWriteProgress = errors.New("raft: write made no progress")

// writeFull writes all of data to w, looping over successive Write calls
// if an individual call writes fewer bytes than requested without an
// error. A conforming io.Writer (os.File included) is documented to
// never actually do this — Write "must return a non-nil error if it
// returns n < len(p)" — but writeFull does not rely on that convention
// alone: a silently short-written temp file would eventually surface as
// a checksum/length mismatch on the next read (ErrCorrupt*), which is a
// far worse outcome than simply finishing the write correctly the first
// time. A write that makes zero progress with no error is treated as a
// hard failure (errNoWriteProgress) rather than looped on forever.
func writeFull(w interface{ Write([]byte) (int, error) }, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return errors.New("raft: Write returned an out-of-range byte count")
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return errNoWriteProgress
		}
	}
	return nil
}
