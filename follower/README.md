# alto-follower

[![Crates.io](https://img.shields.io/crates/v/alto-follower.svg)](https://crates.io/crates/alto-follower)
[![Docs.rs](https://docs.rs/alto-follower/badge.svg)](https://docs.rs/alto-follower)

Run a follower node for `alto`.

## Status

`alto-follower` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.

## Installation

### Local

```bash
cargo install --path . --force
```

### Crates.io

```bash
cargo install alto-follower
```

## Usage

```bash
follower --config config.yaml
```

See [examples/](./examples/) for sample configuration files.

### Configuration

| Field | Description |
|-------|-------------|
| `source` | URL of the indexer endpoint to fetch blocks and stream certificates from |
| `identity` | Hex-encoded BLS12-381 threshold public key used to verify finalization signatures |
| `directory` | Directory for storing finalized blocks and state |
| `worker_threads` | Number of runtime worker threads |
| `log_level` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |
| `metrics_port` | Port for Prometheus metrics endpoint |
| `mailbox_size` | Size of internal mailboxes |
| `max_repair` | Maximum number of blocks to fetch concurrently during backfill |
| `tip` | When `true`, starts from the latest finalized block instead of backfilling from genesis |
