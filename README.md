# alto

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE-MIT)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE-APACHE)
[![Codecov](https://codecov.io/gh/commonwarexyz/alto/graph/badge.svg?token=Y2A6Q5G25W)](https://codecov.io/gh/commonwarexyz/alto)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/commonwarexyz/alto)

## Components

_Components are designed for deployment in adversarial environments. If you find an exploit, please refer to our [security policy](./SECURITY.md) before disclosing it publicly (an exploit may equip a malicious party to attack users of a primitive)._

* [chain](./chain/README.md): A minimal (and wicked fast) blockchain built with the [Commonware Library](https://github.com/commonwarexyz/monorepo).
* [client](./client/README.md): Interact with an `alto` indexer.
* [deploy](./deploy/README.md): Deploy an instance of `alto`.
* [explorer](./explorer/README.md): Visualize `alto` activity.
* [follower](./follower/README.md): Run a follower node for `alto`.
* [indexer](./indexer/README.md): Serve `alto` activity.
* [inspector](./inspector/README.md): Inspect `alto` activity.
* [types](./types/README.md): Common types used throughout `alto`.
* [validator](./validator/README.md): Run a validator node for `alto`.

## Post-Quantum Proof of Concept

This branch is a proof of concept that runs `alto` on post-quantum cryptography:

* Validator identities, peer handshake signatures, and consensus certificates use FN-DSA-512
  (Falcon-512). Each validator signs consensus messages with its identity key, and a certificate
  carries one signature from each member of a quorum.
* Peer handshakes use ML-KEM-768 (FIPS 203) for the ephemeral key exchange, and records are
  encrypted with ChaCha20-Poly1305.
* Leaders are stable: one round-robin leader per term.
* The network identity is the ordered participant set. The indexer, follower, inspector, and
  explorer verify every certificate signature against it.
* An encoded FN-DSA-512 public key is 897 bytes, so deployment host names are derived from the
  SHA-256 digest of the key.

FN-DSA is implemented against the pre-draft standard (FIPS 206 is unpublished), so its key and
signature encodings may change. This branch is for experiments only.

See [deploy](./deploy/README.md) for local and remote deployments.

## Licensing

This repository is dual-licensed under both the [Apache 2.0](./LICENSE-APACHE) and [MIT](./LICENSE-MIT) licenses. You may choose either license when employing this code.

## Support

If you have any questions about `alto`, we encourage you to post in [GitHub Discussions](https://github.com/commonwarexyz/monorepo/discussions). We're happy to help!
