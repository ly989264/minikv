#!/usr/bin/env python3
"""MiniKV smoke runner and basic latency sampler."""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from typing import Dict, List

from resp_cli import RespClient, RespError, RespValue


def expect_simple_string(value: RespValue, expected: str) -> None:
    if value.kind != "simple_string" or value.text != expected:
        raise RespError(f"expected simple string {expected!r}, got {value!r}")


def expect_integer(value: RespValue, expected: int) -> None:
    if value.kind != "integer" or value.integer != expected:
        raise RespError(f"expected integer {expected!r}, got {value!r}")


def expect_array(value: RespValue, expected: List[str]) -> None:
    if value.kind != "array":
        raise RespError(f"expected array, got {value!r}")
    actual = []
    for item in value.items:
        if item.kind not in {"bulk_string", "simple_string"}:
            raise RespError(f"expected bulk/simple string array item, got {item!r}")
        actual.append(item.text)
    if actual != expected:
        raise RespError(f"expected array {expected!r}, got {actual!r}")


def percentile(sorted_values: List[float], ratio: float) -> float:
    if not sorted_values:
        return 0.0
    index = max(0, min(len(sorted_values) - 1, int(round((len(sorted_values) - 1) * ratio))))
    return sorted_values[index]


def summarize(samples_ms: List[float]) -> Dict[str, float]:
    ordered = sorted(samples_ms)
    return {
        "min": ordered[0],
        "p50": statistics.median(ordered),
        "p95": percentile(ordered, 0.95),
        "avg": statistics.fmean(ordered),
        "max": ordered[-1],
    }


def run_once(client: RespClient, key: str, value_suffix: str) -> Dict[str, float]:
    samples: Dict[str, float] = {}

    started = time.perf_counter_ns()
    response = client.command(["PING"])
    samples["PING"] = (time.perf_counter_ns() - started) / 1_000_000.0
    expect_simple_string(response, "PONG")

    stored_value = f"value:{value_suffix}"
    started = time.perf_counter_ns()
    response = client.command(["HSET", key, "field", stored_value])
    samples["HSET"] = (time.perf_counter_ns() - started) / 1_000_000.0
    expect_integer(response, 1)

    started = time.perf_counter_ns()
    response = client.command(["HGETALL", key])
    samples["HGETALL"] = (time.perf_counter_ns() - started) / 1_000_000.0
    expect_array(response, ["field", stored_value])

    started = time.perf_counter_ns()
    response = client.command(["HDEL", key, "field"])
    samples["HDEL"] = (time.perf_counter_ns() - started) / 1_000_000.0
    expect_integer(response, 1)

    return samples


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MiniKV baseline smoke checks.")
    parser.add_argument("--host", default="127.0.0.1", help="server host")
    parser.add_argument("--port", type=int, default=6389, help="server port")
    parser.add_argument(
        "--timeout", type=float, default=2.0, help="socket timeout in seconds"
    )
    parser.add_argument(
        "--iterations", type=int, default=20, help="number of iterations per command"
    )
    parser.add_argument(
        "--key-prefix", default="baseline:key", help="prefix for generated hash keys"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    aggregated: Dict[str, List[float]] = {"PING": [], "HSET": [], "HGETALL": [], "HDEL": []}
    run_prefix = f"{args.key_prefix}:{time.time_ns()}"

    try:
        with RespClient(args.host, args.port, args.timeout) as client:
            for i in range(args.iterations):
                key = f"{run_prefix}:{i}"
                sample = run_once(client, key, str(i))
                for command, latency_ms in sample.items():
                    aggregated[command].append(latency_ms)
    except (OSError, RespError, ValueError) as exc:
        print(f"smoke_failed: {exc}", file=sys.stderr)
        return 2

    print("smoke_status: ok")
    print(f"iterations: {args.iterations}")
    for command in ("PING", "HSET", "HGETALL", "HDEL"):
        summary = summarize(aggregated[command])
        print(
            f"{command} min={summary['min']:.3f}ms "
            f"p50={summary['p50']:.3f}ms "
            f"p95={summary['p95']:.3f}ms "
            f"avg={summary['avg']:.3f}ms "
            f"max={summary['max']:.3f}ms"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
