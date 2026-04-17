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
- parsing RESP arrays
- turning parsed parts into `Cmd` objects
- submitting commands to the shared `Scheduler`
- reordering async completions per connection
- encoding replies back to RESP
- tracking connection and parser metrics

## Request Flow

The request path inside the network layer is:

1. accept a socket on the accept thread
2. hand the socket to one I/O thread
3. parse one or more RESP arrays from the read buffer
4. call `CreateCmd(minikv->command_registry(), parts, &cmd)`
5. submit the `Cmd` into the shared `Scheduler`
6. buffer the `CommandResponse` on completion
7. preserve per-connection response order by request sequence
8. encode the final reply with `EncodeResponse()`

The network layer does not implement command semantics itself.

## Metrics

`NetworkServer::GetMetricsSnapshot()` returns an in-memory snapshot containing:

- worker queue depth, rejections, and inflight count from `Scheduler`
- active, accepted, and closed connection counts
- idle-timeout and error close counts
- RESP parse error count

There is no exported metrics endpoint yet. The snapshot is process-internal.
