# MiniKV Network Layer

## Scope

This layer is defined by:

- `src/network/network_server.h`
- `src/network/network_server.cc`
- `src/network/resp_parser.h`
- `src/network/resp_parser.cc`

It owns TCP networking, RESP framing, response encoding, and connection-local
ordering.

## Responsibilities

`NetworkServer` is responsible for:

- opening the listening socket
- accepting connections
- assigning each connection to one I/O thread
- reading and buffering request bytes
- parsing RESP request arrays
- turning parsed parts into `Cmd` objects
- submitting commands to the shared `Scheduler`
- buffering and reordering async completions per connection
- encoding replies back to RESP
- enforcing connection count, request size, and idle timeout limits
- tracking connection and parser metrics

## Current Thread Model

The network layer owns:

- one accept thread
- `io_threads` I/O threads
- one wakeup pipe per I/O thread

Each I/O thread owns:

- accepted socket file descriptors assigned to that thread
- one `Connection` record per active socket
- per-connection read and write buffers
- per-connection request and response sequence numbers
- a queue of completed worker responses waiting to be flushed in order

## Request Flow

The request path inside the network layer is:

1. accept a socket on the accept thread
2. hand the socket to one I/O thread
3. read bytes into the connection-local buffer
4. parse one or more RESP arrays from the read buffer
5. call `CreateCmd(minikv->command_registry(), parts, &cmd)`
6. assign a per-connection request sequence number
7. submit the `Cmd` into the shared `Scheduler`
8. buffer the `CommandResponse` on completion
9. preserve per-connection response order by request sequence
10. encode the final reply with `EncodeResponse()`
11. flush the connection write buffer when the socket becomes writable

The network layer does not implement command semantics itself.

## RESP Surface

Current request parser behavior:

- only RESP arrays are accepted as top-level requests
- each request element must be a RESP bulk string
- malformed frames produce `ERR ...` replies and increment `parse_errors`

Current reply encoder behavior:

- `ReplyNode` supports simple string, error, integer, bulk string, array, map,
  and null
- current commands only emit simple string, integer, bulk string, array, and
  error replies

## Limits And Shutdown

Current connection-level enforcement:

- `max_connections`: reject accepted sockets beyond the configured limit
- `max_request_bytes`: reject oversized requests with `ERR request too large`
  and then close the connection
- `idle_connection_timeout_ms`: close idle connections that have no inflight
  requests and no buffered writes

Current shutdown behavior:

- `Stop()` stops accepting new sockets
- I/O threads keep draining inflight requests and buffered writes
- connections close after their pending work is flushed

The current server implementation is POSIX-only. On non-POSIX targets,
`NetworkServer::Start()` and `Run()` return `NotSupported`.

## Metrics

`NetworkServer::GetMetricsSnapshot()` returns an in-memory snapshot containing:

- worker queue depth, rejections, and inflight count from `Scheduler`
- active, accepted, and closed connection counts
- idle-timeout and error close counts
- RESP parse error count

There is no exported metrics endpoint yet. The snapshot is process-internal.
