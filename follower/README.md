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

### Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `source` | URL of the indexer to fetch blocks and stream certificates from | |
| `identity` | Hex-encoded BLS12-381 threshold public key used to verify consensus signatures | |
| `directory` | Path to store finalized blocks and state | |
| `worker_threads` | Number of runtime worker threads | `4` |
| `log_level` | Log verbosity (`trace`, `debug`, `info`, `warn`, `error`) | `info` |
| `metrics_port` | Port for the Prometheus metrics endpoint | `9091` |
| `mailbox_size` | Capacity of internal actor mailboxes | `1024` |
| `max_repair` | Maximum concurrent block fetches during backfill | `256` |
| `tip` | Start from the tip of the finalized chain instead of backfilling from genesis | `false` |

_See [examples/](./examples/) for sample configuration files._
