# Ellipsoidal Falcon experiment

This branch replaces the previous FN-DSA-512/Falcon experiment with a distinct,
experimental gamma-6 ellipsoidal Falcon profile. It reduces certificate bytes
while keeping Simplex's votes, quorum, leader selection, timeouts, and recovery
flow. Any valid quorum subset can contribute; validators do not need to know
the eventual subset before signing. The browser verifies **every signature**.

The profile adds no per-signature signing state or trusted setup. Ordinary
Simplex persistent vote state remains necessary. This is an experiment, not a
production deployment or a standard FN-DSA implementation.

## Encoded sizes

These are binary byte counts for the same 50-validator, 34-signer fixture:

| Object | Previous FN-DSA-512 | Ellipsoidal Falcon |
|---|---:|---:|
| Public key | 897 | 897 |
| Network identity | 44,851 | 44,851 |
| Individual signature | 666 | 512 |
| Certificate | 22,660 | 17,424 |
| Notarization or finalization proof | 22,695 | 17,459 |
| Proof plus fixture block | 23,683 | 18,447 |
| Raw verifier WASM module | 147,868 | 149,721 |

The fixed 512-byte signature retains a 40-byte salt and uses canonical zero padding.
The identity is a one-byte count followed by 50 public keys. The certificate is
34 signatures plus 16 bytes of signer bitmap/count encoding. A proof adds the
35-byte proposal; the complete artifact adds this fixture's 988-byte block.
Notarization and finalization have equal lengths here. No transport kind byte,
HTTP/WebSocket framing, compression, or JSON/hex expansion is included.

The certificate saves 5,236 bytes (23.1%) but remains **17,424 bytes, above the
4,000-byte goal**. Total network bandwidth also depends on votes, proposals,
block payloads, retransmission, fanout, and transport overhead; the certificate
reduction alone is not a total-bandwidth measurement.

The checked-in [50-validator fixture](explorer/scripts/ellipsoidal_falcon_50_fixture.json)
uses 34 distinct, noncontiguous participant indices. The
[four-validator fixture](explorer/scripts/ellipsoidal_falcon_fixture.json) covers
three signers. Both encode notarized and finalized artifacts.

## Browser measurements

Measured in headless Chrome 154.0.8037.98 on an Apple M5 Pro with 64 GiB RAM.
Times include artifact decoding in WASM, all 34 signature verifications, certificate hashing,
and serialization of the result to JavaScript:

| Artifact | Previous warm median | Ellipsoidal warm median |
|---|---:|---:|
| Notarized | 0.493 ms | 0.521 ms |
| Finalized | 0.493 ms | 0.522 ms |

Warm medians are over 15 batch means of 100 calls, after 100 warmup calls. Cold
identity-cache-miss medians were approximately 0.6 ms for the previous profile
and 0.7 ms for this profile, with roughly 0.1 ms clock quantization. Cold
measurements exclude the call used to evict the cached identity. Neither set
includes network transfer, WASM startup, worker messaging, or rendering.

Other compiler and desktop processes were active during measurement. These are
measurements of stable artifacts under shared host load, **not an idle-host
performance ranking**. The small timing difference does not establish a
definitive regression.

For both profiles, the browser accepted both fixture sizes and rejected each
individual signature's salt mutation: 68 mutations for the 34-signer proofs
and six for the three-signer proofs. It also rejected the alternate valid
participant identity. These checks ran in Chrome, not only in Node.

Measured WASM SHA-256 values:

```text
previous: 4df011cbb5b903aa47e3ecf03dc3c6e174debc9d7660d50547dc66f625bd0804
current:  ae3773fc8bb3e9464dc307fb39d7825c9b18228599c736030d487854d09c5696
```

The measured current 50-validator fixture has SHA-256
`61fe8dbd042eef3444bc8cc858d452e4fff7dee6f5319fc2adcf490e9b5fef1d`.
Rebuilding may produce a different WASM hash; record hashes with new results.

## Native measurements

Measured sequentially on the same M5 Pro with Rust 1.98.1, LLVM 22.1.8, and the
repository's release overflow checks enabled. Times below are median / p95 in
microseconds. No compiler or test process exceeded 5% CPU in the timing
snapshots; CPU affinity, frequency, and thermal state were not controlled.

| Operation | Samples | Previous FN-DSA-512 | Ellipsoidal Falcon |
|---|---:|---:|---:|
| Key generation | 32 | 2,814.7 / 4,565.9 | 49,707.1 / 70,975.8 |
| Private seed reconstruction | 32 | 2,172.2 / 3,908.6 | 49,505.0 / 70,980.5 |
| Signing | 128 | 131.0 / 140.1 | 1,884.9 / 1,967.4 |
| Public-key decoding | 1,024 | 2.67 / 2.88 | 1.25 / 1.29 |
| Verification with decoded key | 1,024 | 12.29 / 12.67 | 14.17 / 14.58 |
| Public-key decoding plus verification | 1,024 | 15.08 / 15.42 | 15.21 / 15.75 |

Signing is about 14.4 times slower in this implementation. Each call includes
private-basis validation and reconstruction of its sampling context. Seed
decoding regenerates the key pair; it is not a parse of an expanded key.
Verification starts with a decoded signature. The combined row adds public-key
decoding, not signature decoding.

The runs used 32 deterministic keys and 128 distinct 32-byte messages, with
input preparation and output destruction outside each timed interval. Every
signature verified, all reconstructed public keys matched, repeated signing
was deterministic, and a changed message was rejected. These measurements
describe this implementation and sample; they do not imply equal security.

## Reproduce

Use the Commonware revision selected by this experiment's dependency pin and
lockfile, plus a Rust WASM target and `wasm-pack`. On macOS, configure the LLVM
compiler as described in the [explorer build instructions](explorer/README.md).
From the Alto repository:

```sh
cargo test --locked -p alto-types explorer_ellipsoidal_falcon
cd explorer
npm ci
npm run build
python3 -m http.server 8000
```

`npm run build` builds the actual verifier WASM and runs
[check-wasm.mjs](explorer/scripts/check-wasm.mjs) before building React. The
checks cover both quorum sizes, every signature slot, participant identity,
truncation, trailing data, and the certificate/block boundary. Regenerate
fixtures only for an intentional encoding change, using
`ALTO_UPDATE_FIXTURES=1` with the Rust fixture tests; rerun without it afterward.

Open `http://localhost:8000/scripts/benchmark-wasm.html` for the 50-validator
measurement, or add `?fixture=ellipsoidal_falcon_fixture.json` for the smaller
fixture. The [benchmark page](explorer/scripts/benchmark-wasm.html) reports
module initialization, first verification, warm batch statistics, byte sizes,
and WASM memory size. Its first-call measurement is not the separately
cache-evicted cold median above. Record browser/hardware versions and host load
when comparing runs.

For a native signing and verification comparison, run Commonware's Criterion
harness at the pinned revision:

```sh
cargo bench -p commonware-cryptography --bench fn_dsa
```

It includes both profiles. Its repeated single-message cases differ from the
multi-key, multi-message sample used for the native table above.

## Security and deployment scope

The profile has no assigned NIST security category, no claimed 128-bit security,
and no standard FN-DSA interoperability. Current heuristic forgery estimates
are approximately 117 classical bits and 103 quantum bits; these are estimates,
not a proof or certification. The conditioned key distribution, constant-time
implementation, and formal sampler proof remain open review obligations.

The [Commonware profile description](https://github.com/commonwarexyz/monorepo/blob/5471199891843d23658619191ec5f4daf423889f/cryptography/src/fn_dsa/ellipsoidal/README.md)
defines the pinned parameters, provenance, and limitations. This Alto document
describes integration and measurements.

No network was deployed as part of this experiment. The profiles use distinct
wire tags and require matching validator keys, network identity, and verifier
configuration; existing FN-DSA network artifacts cannot be mixed into this
experiment.
