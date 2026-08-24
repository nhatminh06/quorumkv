# Transport

`internal/transport` frames and delivers bounded messages between nodes
over TCP. It answers "how do I safely send a message to another node?" —
nothing here knows about terms, elections, leaders, or replication.

## Wire format

All multi-byte integers are big-endian.

```
+------------------------+
| magic           (4B)   |  "QKV1"
+------------------------+
| version         (1B)   |  1
+------------------------+
| message type    (1B)   |
+------------------------+
| payload length  (4B)   |
+------------------------+
| payload         (var)  |
+------------------------+
| checksum        (4B)   |  CRC32C (Castagnoli) over version..payload
+------------------------+
```

The checksum covers `version | type | payload length | payload` — not the
magic and not itself.

## Message types

`MessageType` is a 1-byte wire value. This milestone defines only
placeholder types used to test framing and transport: `MessagePing`,
`MessagePong`, `MessageTest`. Future Raft RPC types (RequestVote,
AppendEntries, ...) will be added as new `MessageType` values when that
work begins; transport treats every payload as opaque bytes regardless of
type.

## Payload bounds

`MaxPayloadSize` is 1 MiB. The decoder validates a frame's declared
payload length against this bound *before* allocating a payload buffer, so
a peer cannot force a large allocation by declaring a huge length and
never sending that much data.

## Message ownership

`Message.Payload` is copied on construction (`NewMessage`) and on decode
(`ReadFrame` always allocates a fresh buffer) — a caller mutating a slice
it passed in, or mutating a decoded payload, cannot affect anything else.

## TCP stream handling

TCP is a byte stream, not a sequence of message-sized reads. `ReadFrame`
uses `io.ReadFull` for the header, payload, and checksum, so it is
correct regardless of how a `Read` call happens to fragment the data —
one byte at a time, in irregular chunks, or with several frames' worth of
bytes available at once. Calling `ReadFrame` repeatedly on the same stream
decodes successive frames in order with no leakage between them.

`io.EOF` from `ReadFrame` means the stream ended cleanly at a frame
boundary (no partial frame in flight). Any other incomplete read — a
partial header, payload, or checksum — is `ErrTruncatedFrame` and is never
treated as a valid message.

## Connection model

Each TCP connection carries exactly one request and one response, then is
closed. This keeps connection lifecycle deterministic and easy to test.
There is no connection pooling or persistent connection reuse in this
milestone.

## Request/response and timeouts

`Send(ctx, addr, msg)` dials, writes one request frame, reads one response
frame, and closes the connection. `ctx` bounds the entire exchange: if it
is canceled or its deadline passes while blocked on I/O, the connection is
closed to unblock the operation and `ctx.Err()` is returned.

On the server side, `Transport.Close` cancels the context passed to any
handler still running, so a handler that checks `ctx.Done()` can return
promptly during shutdown. A handler that ignores cancellation and isn't
blocked on connection I/O will make `Close` wait for it to actually
return — `Close` does not abandon in-flight handlers.

## Shutdown

`Transport.Close` stops accepting new connections, cancels in-flight
handler contexts, closes every connection currently being served (which
unblocks any handler blocked on reading or writing that connection), and
waits for the accept loop and every connection-handling goroutine to
finish before returning. No transport goroutines remain running once
`Close` returns.

## Malformed peers

A malformed frame (bad magic, unsupported version, unknown type, oversized
declared length, bad checksum, or a truncated frame) closes only the
offending connection. The listener and every other connection are
unaffected.

## Error handling

If a handler returns an error, the connection is closed without writing a
response. There is no transport-level error response protocol yet.

## Delivery and security limitations

- No automatic retries: `Send` returns the error it hit; it does not know
  whether the remote side processed the request before failing, so
  deciding whether to retry is left to the caller.
- No exactly-once, at-least-once, ordering-across-reconnects, or
  deduplication guarantees. Within one decoded stream, frames are read in
  byte order — that is the only ordering guarantee.
- No TLS and no authentication. This is a local educational transport;
  anyone who can reach the listening address can send it frames.
