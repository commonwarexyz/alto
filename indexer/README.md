# alto-indexer

[![Crates.io](https://img.shields.io/crates/v/alto-indexer.svg)](https://crates.io/crates/alto-indexer)
[![Docs.rs](https://docs.rs/alto-indexer/badge.svg)](https://docs.rs/alto-indexer)

Serve `alto` activity.

_This indexer keeps all data in memory and starts empty after every restart. It is intended for local use. For production, adapt it to store data in a database._

## Status

`alto-indexer` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.

## Installation

### Local

```bash
cargo install --path . --force
```

### Crates.io

```bash
cargo install alto-indexer
```

## Usage

### Start the indexer

```bash
indexer --port 8080 --identity <hex-encoded BLS12-381 public key> --leader-mode <stable|rotating-vrf|rotating-round-robin> [--block-size <bytes>]
```

The identity is the threshold public key of the consensus network. The indexer uses it to verify
uploaded seeds and certificates. `--leader-mode` selects the network's leader policy, and the indexer
derives the certificate format from this selection.

`--block-size` is the payload size of the network's blocks. When set, uploads carrying a larger payload
are rejected and the request body limit is that size plus 1 MiB. Without it, upload request bodies
are limited to 5 MiB.

Configure Rust clients with the same `block_size`. Their WebSocket receive limit includes the
indexer's upload allowance plus one message-kind byte. See the [Rust client](../client/README.md).

A deployed indexer reads its port, identity, leader mode, block size, and explorer settings from
`--config indexer.yaml` instead of these flags. When `explorer/build` exists at compile time, the
indexer also serves the explorer and its `/runtime-config.js`. See
[Build the Embedded Explorer](../deploy/README.md#build-the-embedded-explorer).

## API Endpoints

### Health Check

```txt
GET /health
```

### Seeds

```txt
POST /seed          # Upload a seed
GET /seed/latest    # Get the latest seed
GET /seed/<view>    # Get the seed for a specific view (hex-encoded)
```

### Notarizations

```txt
POST /notarization          # Upload a notarization
GET /notarization/latest    # Get the latest notarization
GET /notarization/<view>    # Get the notarization for a specific view (hex-encoded)
```

### Finalizations

```txt
POST /finalization          # Upload a finalization
GET /finalization/latest    # Get the latest finalization
GET /finalization/<view>    # Get the finalization for a specific view (hex-encoded)
```

### Blocks

```txt
POST /block             # Upload a block without a certificate
GET /block/latest       # Get the latest finalized block
GET /block/<height>     # Get the block at a specific height (hex-encoded)
GET /block/<digest>     # Get the block with a specific digest (hex-encoded)
```

Height and `latest` queries return finalized blocks with their certificates. A digest query returns
any stored block with that digest, including uploads without a certificate, so it does not show
that the block was notarized or finalized.

### WebSocket

```txt
WS /consensus/ws    # Stream consensus events (seeds, notarizations, finalizations)
```
