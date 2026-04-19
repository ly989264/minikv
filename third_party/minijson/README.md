minijson
========

This is a small header-only JSON DOM/parser/serializer bundled directly in the
repository so `minikv` can build and run JSON-related commands without any
network fetch step.

It intentionally supports only the functionality needed by the standalone
project:

- parsing JSON objects, arrays, strings, numbers, booleans, and null
- preserving object member order
- minified and configurable pretty serialization
- simple numeric formatting for JSON.NUMINCRBY and JSON.CLEAR

The implementation lives in `third_party/minijson/minijson.h`.
