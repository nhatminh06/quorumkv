package raft

import (
	"os"
	"path/filepath"
)

// atomicWriteFile replaces the file at path with data: it writes to a
// temp file in the same directory, fsyncs it, renames it into place (an
// atomic replace on POSIX filesystems), then fsyncs the containing
// directory so the rename itself survives a crash. A reader always
// observes either the previous complete file or the new complete file,
// never a partial write. Shared by PersistentState's Store and the Raft
// Log, both of which rewrite a small file wholesale on every mutation.
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename below succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	return dirFile.Sync()
}
