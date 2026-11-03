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

_To deploy your own instance of `alto`, read the guide in [deploy](../deploy/README.md)._

### Configuration

| Field | Description |
|-------|-------------|
| `source` | URL of the indexer to fetch blocks and stream certificates from |
| `identity` | Hex-encoded BLS12-381 threshold public key used to verify consensus signatures |
| `leader_mode` | Network leader policy: `stable`, `rotating-vrf`, or `rotating-round-robin` |
| `block_size` | Network block payload size in bytes, used to bound the live feed |
| `directory` | Path to store finalized blocks and state |
| `worker_threads` | Number of runtime worker threads |
| `signature_threads` | Number of threads for signature verification |
| `log_level` | Log verbosity (`trace`, `debug`, `info`, `warn`, `error`) |
| `metrics_port` | Port for the Prometheus metrics endpoint |
| `mailbox_size` | Capacity of internal actor mailboxes |
| `max_repair` | Maximum concurrent block fetches during backfill |
| `fetch_retry_timeout_ms` | Milliseconds between follower resolver retries for failed backfill requests |
| `tip` | On a fresh follower, start from the latest finalized block instead of backfilling from genesis |
| `pruning_depth` | Number of finalized blocks to retain before pruning (null to keep all) |

All fields except `pruning_depth` are required. See [examples/](./examples/) for sample configuration files.

With `tip` set, a follower that has processed no blocks beyond genesis starts from the indexer's
latest finalization, falling back to genesis if that request fails. A follower with stored progress
resumes from it.

Set `block_size` to the same value as the validators and indexer. The live feed includes the
encoding allowance described in the [Rust client](../client/README.md).

## Certificate Validation Policy

`alto-follower` considers invalid consensus certificates a fatal error and may panic if the `source` provides one. If you observe this behavior, switch to a new `source` and/or ensure your configuration is correct for the network you are following.
