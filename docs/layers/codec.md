# MiniKV Codec Layer

## Scope

This layer is defined by:

- `src/storage/encoding/key_codec.h`
- `src/storage/encoding/key_codec.cc`

It defines the on-disk key and metadata encoding used by the hash storage
model.

## Responsibilities

`KeyCodec` provides:

- meta-key encoding
- hash-data-prefix encoding
- hash-data-key encoding
- metadata value encoding and decoding
- prefix checks and field extraction helpers

## Current Layout

Logical encodings today:

- meta key: `m| + key_length + user_key`
- hash data prefix: `h| + key_length + user_key + version`
- hash data key: `hash_prefix + field`

Current metadata fields:

- `type`
- `encoding`
- `version`
- `size`
- `expire_at_ms`

## Current Design Conclusion

This directory is intentionally narrow now. It is a codec layer, not a storage
engine compatibility layer, and it only contains the encoding rules still used
by the current runtime.
