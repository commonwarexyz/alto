# alto-validator

[![Crates.io](https://img.shields.io/crates/v/alto-validator.svg)](https://crates.io/crates/alto-validator)
[![Docs.rs](https://docs.rs/alto-validator/badge.svg)](https://docs.rs/alto-validator)

Run a validator node for `alto`.

## Status

`alto-validator` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.

## Installation

### Local

```bash
cargo install --path . --force
```

### Crates.io

```bash
cargo install alto-validator
```

## Usage

```bash
validator --config config.yaml --peers peers.yaml
```
