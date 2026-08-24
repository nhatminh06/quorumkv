# Write-ahead log format

The WAL (`internal/wal`) persists the ordered sequence of `kv.Command`
values applied to the state machine. It is a flat, append-only sequence of
length-prefixed records. All multi-byte integers are big-endian.

## Record layout

```
+----------------------+
| record length (4B)   |  length of everything below (type..checksum)
+----------------------+
| type            (1B) |  1 = PUT, 2 = DELETE
+----------------------+
| key length      (4B) |
+----------------------+
| value length    (4B) |
+----------------------+
| key             (var)|
+----------------------+
| value           (var)|
+----------------------+
| checksum        (4B) |  CRC32C (Castagnoli) over type..value
+----------------------+
```

`record length` = `1 + 4 + 4 + len(key) + len(value) + 4`.

## Size limits

To prevent a corrupt or hostile length field from causing an unbounded
allocation, before reading a record's body the declared record length is
checked against a fixed maximum:

- max key size: 64 KiB
- max value size: 1 MiB
- max declared record length: `9 + 65536 + 1048576 + 4` bytes (~1.06 MiB)

A record whose declared length exceeds this bound is rejected as corrupt
without allocating a buffer for it.

## Append semantics

`Append` encodes a record and writes it with the operating system's
`write(2)`, looping over any short write until all bytes are written or an
error occurs. A successful `Append` means the bytes were handed to the OS —
it is **not** a durability guarantee; the write may still be lost on power
loss or crash if not yet flushed to disk.

`Sync` calls `fsync` on the underlying file. Only a record that has been
both `Append`ed and covered by a subsequent `Sync` is guaranteed to survive
a crash.

## Replay and recovery

`Open` reads every record from the start of the file in order and returns
the decoded commands, so replaying a WAL always reconstructs the state
machine in the exact order commands were originally applied.

Two categories of malformed data are handled differently:

- **Torn tail**: the file ends mid-record (a partial length prefix,
  a partial body, or a partial checksum) because a crash interrupted the
  final `Append`. This is expected and is not an error: replay keeps every
  preceding complete, valid record, and `Open` truncates the file to drop
  the incomplete tail so future appends start from a clean boundary.

- **Mid-log corruption**: a record's length prefix declares more than the
  maximum allowed size, its type byte is neither PUT nor DELETE, its
  encoded lengths are internally inconsistent, or its checksum does not
  match its bytes — while the file continues past it. This can only mean
  the file was corrupted, not that a write was interrupted mid-flight, so
  it is not safe to guess what data if any follows. `Open` returns
  `ErrCorrupt` immediately and does not replay anything past that point.
  The file is left untouched.

## Concurrency

`WAL` is not safe for concurrent use; callers must synchronize access
externally if needed.
