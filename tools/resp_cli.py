#!/usr/bin/env python3
"""Minimal RESP socket client for MiniKV smoke tests."""

from __future__ import annotations

import argparse
import socket
import sys
from dataclasses import dataclass, field
from typing import List, Optional


class RespError(Exception):
    pass


@dataclass
class RespValue:
    kind: str
    text: str = ""
    integer: int = 0
    items: List["RespValue"] = field(default_factory=list)


def encode_command(parts: List[str]) -> bytes:
    out = [f"*{len(parts)}\r\n".encode("utf-8")]
    for part in parts:
        data = part.encode("utf-8")
        out.append(f"${len(data)}\r\n".encode("utf-8"))
        out.append(data)
        out.append(b"\r\n")
    return b"".join(out)


def _read_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise RespError("connection closed while reading response")
        chunks.extend(chunk)
    return bytes(chunks)


def _read_line(sock: socket.socket) -> bytes:
    chunks = bytearray()
    while True:
        byte = sock.recv(1)
        if not byte:
            raise RespError("connection closed while reading line")
        chunks.extend(byte)
        if len(chunks) >= 2 and chunks[-2:] == b"\r\n":
            return bytes(chunks[:-2])


def read_value(sock: socket.socket) -> RespValue:
    prefix = _read_exact(sock, 1)
    if prefix == b"+":
        return RespValue(kind="simple_string", text=_read_line(sock).decode("utf-8"))
    if prefix == b"-":
        return RespValue(kind="error", text=_read_line(sock).decode("utf-8"))
    if prefix == b":":
        return RespValue(kind="integer", integer=int(_read_line(sock)))
    if prefix == b"$":
        length = int(_read_line(sock))
        if length < 0:
            return RespValue(kind="null_bulk_string")
        text = _read_exact(sock, length).decode("utf-8")
        if _read_exact(sock, 2) != b"\r\n":
            return _raise_protocol()
        return RespValue(kind="bulk_string", text=text)
    if prefix == b"*":
        count = int(_read_line(sock))
        if count < 0:
            return RespValue(kind="null_array")
        items = [read_value(sock) for _ in range(count)]
        return RespValue(kind="array", items=items)
    raise RespError(f"unsupported RESP prefix: {prefix!r}")


def _raise_protocol() -> RespValue:
    raise RespError("bulk string missing CRLF terminator")


def format_value(value: RespValue, indent: int = 0) -> str:
    prefix = " " * indent
    if value.kind == "simple_string":
        return f"{prefix}simple_string: {value.text}"
    if value.kind == "bulk_string":
        return f"{prefix}bulk_string: {value.text}"
    if value.kind == "null_bulk_string":
        return f"{prefix}null_bulk_string"
    if value.kind == "integer":
        return f"{prefix}integer: {value.integer}"
    if value.kind == "error":
        return f"{prefix}error: {value.text}"
    if value.kind == "null_array":
        return f"{prefix}null_array"
    if value.kind == "array":
        lines = [f"{prefix}array[{len(value.items)}]"]
        for index, item in enumerate(value.items):
            lines.append(f"{prefix}[{index}]")
            lines.append(format_value(item, indent + 2))
        return "\n".join(lines)
    raise RespError(f"unknown RESP kind: {value.kind}")


class RespClient:
    def __init__(self, host: str, port: int, timeout: float):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock: Optional[socket.socket] = None

    def __enter__(self) -> "RespClient":
        self._sock = socket.create_connection(
            (self._host, self._port), timeout=self._timeout
        )
        self._sock.settimeout(self._timeout)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def command(self, parts: List[str]) -> RespValue:
        if self._sock is None:
            raise RespError("client is not connected")
        self._sock.sendall(encode_command(parts))
        return read_value(self._sock)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send one RESP command to MiniKV.")
    parser.add_argument("--host", default="127.0.0.1", help="server host")
    parser.add_argument("--port", type=int, default=6389, help="server port")
    parser.add_argument(
        "--timeout", type=float, default=2.0, help="socket timeout in seconds"
    )
    parser.add_argument(
        "parts",
        nargs="+",
        help="command and arguments, for example: PING or HSET user:1 name alice",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        with RespClient(args.host, args.port, args.timeout) as client:
            value = client.command(args.parts)
    except (OSError, RespError, ValueError) as exc:
        print(f"client_error: {exc}", file=sys.stderr)
        return 2

    print(format_value(value))
    if value.kind == "error":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
