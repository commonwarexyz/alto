# alto-indexer

[![Crates.io](https://img.shields.io/crates/v/alto-indexer.svg)](https://crates.io/crates/alto-indexer)
[![Docs.rs](https://docs.rs/alto-indexer/badge.svg)](https://docs.rs/alto-indexer)

Serve `alto` activity.

_This is a stateless indexer intended for local use. If you want to utilize in production, adapt the code to utilize a database for storage (rather than memory)._

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
indexer --port 8080 --identity <hex-encoded participant set> [--block-size <bytes>]
```

The identity is the participant set of the consensus network: the ordered FN-DSA-512 public keys of
its validators. Every signature in an incoming certificate is verified against its signer's key.

`--block-size` is the payload size of the network's blocks. When set, uploads carrying a larger payload
are rejected and the request body limit is that size plus 1 MiB. Without it, upload request bodies
are limited to 5 MiB. The indexer retains all accepted artifacts in memory until restart.

Configure Rust clients with the same `block_size`. Their WebSocket receive limit includes the
indexer's upload allowance plus one message-kind byte. See the [Rust client](../client/README.md).

## API Endpoints

### Health Check

```txt
GET /health
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
GET /block/latest       # Get the latest finalized block
GET /block/<height>     # Get the block at a specific height (hex-encoded)
GET /block/<digest>     # Get the block with a specific digest (hex-encoded)
```

### WebSocket

```txt
WS /consensus/ws    # Stream consensus events (notarizations, finalizations)
```
