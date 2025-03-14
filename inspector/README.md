# alto-inspector

[![Crates.io](https://img.shields.io/crates/v/alto-inspector.svg)](https://crates.io/crates/alto-inspector)
[![Docs.rs](https://docs.rs/alto-inspector/badge.svg)](https://docs.rs/alto-inspector)

Inspect `alto` activity.

## Status

`alto-inspector` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.

## Installation

### Local

```bash
cargo install --path . --force
```

### Crates.io

```bash
cargo install alto-inspector
```

## Usage

_Use `-v` or `--verbose` to enable verbose logging (like request latency). Use `--prepare` to initialize the connection before making the request (for accurate latency measurement)._

### Get the latest seed

```bash
inspector get seed latest
```

### Get the notarization for view 100

```bash
inspector get notarization 100
```

### Get the notarizations between views 100 to 110

```bash
inspector get notarization 100..110
```

### Get the finalization for view 50

```bash
inspector get finalization 50
```

### Get the latest finalized block

```bash
inspector get block latest
```

### Get the block at height 10

```bash
inspector get block 10
```

### Get the blocks between heights 10 and 20

```bash
inspector get block 10..20
```

### Get the block with a specific digest

```bash
inspector -- get block 0x65016ff40e824e21fffe903953c07b6d604dbcf39f681c62e7b3ed57ab1d1994
```

### Listen for consensus events

```bash
inspector listen
```