#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import sys


def parse_cmake_cache(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith(("//", "#")):
            continue
        key_and_type, separator, value = line.partition("=")
        if not separator:
            continue
        key, _, _type_name = key_and_type.partition(":")
        values[key] = value
    return values


def rewrite_strings(value, replacements: list[tuple[str, str]]):
    if isinstance(value, str):
        rewritten = value
        for old, new in replacements:
            rewritten = rewritten.replace(old, new)
        return rewritten
    if isinstance(value, list):
        return [rewrite_strings(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: rewrite_strings(item, replacements)
                for key, item in value.items()}
    return value


def resolve_existing_path(base: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else (base / candidate).resolve()


def resolve_output_path(base: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else base / candidate


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent

    parser = argparse.ArgumentParser(
        description="Rewrite a container-generated compile_commands.json for host clangd use."
    )
    parser.add_argument(
        "--build-dir",
        default="build",
        help="Build directory containing compile_commands.json and CMakeCache.txt (default: build)",
    )
    parser.add_argument(
        "--output",
        default="compile_commands.json",
        help="Output path for the rewritten compilation database (default: compile_commands.json)",
    )
    parser.add_argument(
        "--configured-source-dir",
        help="Override the source directory recorded during CMake configure",
    )
    parser.add_argument(
        "--configured-build-dir",
        help="Override the build directory recorded during CMake configure",
    )
    args = parser.parse_args()

    build_dir = resolve_existing_path(repo_root, args.build_dir)
    input_path = build_dir / "compile_commands.json"
    cache_path = build_dir / "CMakeCache.txt"
    output_path = resolve_output_path(repo_root, args.output)

    if not input_path.is_file():
        print(f"missing compile commands input: {input_path}", file=sys.stderr)
        return 1
    if not cache_path.is_file():
        print(f"missing CMake cache: {cache_path}", file=sys.stderr)
        return 1

    cache = parse_cmake_cache(cache_path)
    configured_source_dir = Path(
        args.configured_source_dir or cache.get("CMAKE_HOME_DIRECTORY", "")
    )
    configured_build_dir = Path(
        args.configured_build_dir or cache.get("CMAKE_CACHEFILE_DIR", "")
    )

    if not configured_source_dir:
        print("CMAKE_HOME_DIRECTORY not found in CMakeCache.txt", file=sys.stderr)
        return 1
    if not configured_build_dir:
        print("CMAKE_CACHEFILE_DIR not found in CMakeCache.txt", file=sys.stderr)
        return 1

    replacements: list[tuple[str, str]] = []
    if configured_build_dir != build_dir:
        replacements.append((str(configured_build_dir), str(build_dir)))
    if configured_source_dir != repo_root:
        replacements.append((str(configured_source_dir), str(repo_root)))

    replacements.sort(key=lambda item: len(item[0]), reverse=True)

    database = json.loads(input_path.read_text(encoding="utf-8"))
    rewritten_database = rewrite_strings(database, replacements)

    if output_path.is_symlink() or output_path.exists():
        output_path.unlink()
    output_path.write_text(
        json.dumps(rewritten_database, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"wrote {output_path}")
    if replacements:
        for old, new in replacements:
            print(f"rewrote {old} -> {new}")
    else:
        print("no path rewrite was needed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
