# MiniKV Codec Layer

## Scope

This layer is defined by:

- `src/storage/encoding/key_codec.h`
- `src/storage/encoding/key_codec.cc`

It defines the storage-key encodings used by the current hash storage model.

## Responsibilities

`KeyCodec` provides:

- meta-key encoding
- hash-data-prefix encoding
- hash-data-key encoding
- prefix checks and field extraction helpers

Important boundary:

- `KeyCodec` owns key encodings
- `DefaultCoreKeyService` in `src/core/key_service.cc` owns metadata value
  encoding and decoding
- `ModuleKeyspace` encoding for the shared `module` column family lives in
  `src/runtime/module/module_services.cc`

## Current Layout

Key encodings today:

- meta key: `m| + uint32(key_length) + user_key`
- hash data prefix:
  `h| + uint32(key_length) + user_key + uint64(version)`
- hash data key: `hash_prefix + field`

Metadata value layout today:

- `type`: 1 byte
- `encoding`: 1 byte
- `version`: 8 bytes
- `size`: 8 bytes
- `expire_at_ms`: 8 bytes

That is a fixed-width 26-byte payload encoded by `DefaultCoreKeyService`.

Current lifecycle semantics tied to metadata:

- `expire_at_ms = 0` means no TTL
- `expire_at_ms = 1` means logical tombstone
- expired and tombstoned keys are hidden from user-visible lookups
- recreating an expired or tombstoned hash bumps its version

Module keyspace layout in the shared `module` column family is:

- `uint32(module_name_length) + module_name +
   uint32(local_name_length) + local_name + local_key`

## Current Design Conclusion

This directory is intentionally narrow. It is a key-encoding layer, not a
storage-engine compatibility layer, and it only contains the encoding rules
still used by the current runtime.
