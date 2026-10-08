# alto-deploy

Deploy an instance of [alto](../README.md).

This branch is a post-quantum proof of concept. Validators identify themselves with FN-DSA-512
(Falcon-512) keys, authenticate peer handshakes with them, and sign consensus messages with them, so
every certificate carries one 666-byte FN-DSA-512 signature per signer. Handshakes use ML-KEM-768
for the ephemeral key exchange, and records are encrypted with ChaCha20-Poly1305. Leaders are
either rotating (a new round-robin leader every view, with no VRF) or stable (each serves a term
of views in round-robin order).

FN-DSA is implemented against the pre-draft standard (FIPS 206 is unpublished), so its key and
signature encodings may change. Use these networks for experiments only.

The network identity is the hex-encoded, sorted participant set. Every FN-DSA-512 public key is
897 bytes (1794 hex characters per participant), so the identity of a 50-validator network is
about 90 KB of hex. Indexers, explorers, followers, and the inspector verify every certificate
against this participant set. Deployed indexers and followers read the identity from their
configuration files because Linux limits a single command-line argument to 128 KiB.

Instances, configuration files, and storage directories are named by the first 16 bytes of the
SHA-256 digest of each validator's public key, in hex. `allowed_peers`, `bootstrappers`, and
`peers.yaml` keep full hex-encoded keys.

## Deploy

### Local

_To run a deploy, you must first install [Rust](https://www.rust-lang.org/tools/install)._

#### Create Artifacts

_To configure local indexer upload, add `--indexers '<url>:<count>[;<url>:<count>...]'` to the `generate local` command. For example, `http://localhost:8080:1` assigns one validator to upload to that indexer._

`--leader-mode rotating` assigns a new round-robin leader to every view. Without threshold
signatures there is no VRF seed, so the order is fixed by the participant set. Stable mode assigns
leaders round-robin for terms of `--leader-term-length` views. Stable mode requires a term length
of at least two and `--leader-optimistic-views`, which limits how far proposals and votes may run
ahead of directly notarized ancestry. Values above the term length have no further effect.
Rotating mode rejects both stable-only flags.

`--leader-delay-ms` sets the minimum interval from the parent's timestamp, from 0 through 999 ms.
Zero disables pacing and allows equal timestamps. Use 100 ms for local stable networks: shorter
intervals can cause timeouts and skipped terms.

`--block-size` sets the number of random payload bytes in each proposed block and defaults to `0`.
All validators must use the same value. Encoded messages must fit the authenticated transport's
size limit; validators also reject incoming blocks with payloads larger than their configured size.
Use the same `block_size` for Rust clients so their WebSocket receive limits include the block
payload and the indexer's encoding allowance.

Use an empty validator or follower `directory` for each new network. Reuse an existing directory
only when the network and storage format match. The indexer keeps its state in memory and restarts
empty.

`--traces-sample-rate` sets the fraction of traces exported by each validator and accepts values
from `0` through `1`. It defaults to `0`, which disables trace export.

The stable local example below writes validator configurations containing:

```yaml
leader:
  mode: stable
  delay_ms: 100
  term_length: 1000
  optimistic_views: 48
block_size: 0
traces_sample_rate: 0.0
indexer: http://localhost:8080
```

```bash
cargo run --bin deploy -- generate --peers 4 --bootstrappers 1 --worker-threads 2 --log-level info --traces-sample-rate 0 --mailbox-size 16384 --deque-size 256 --signature-threads 2 --leader-mode stable --leader-delay-ms 100 --leader-term-length 1000 --leader-optimistic-views 48 --output test local --start-port 3000 --indexers 'http://localhost:8080:1'
```

Rotating-leader configurations carry only the delay:

```yaml
leader:
  mode: rotating
  delay_ms: 0
```

```bash
cargo run --bin deploy -- generate --peers 4 --bootstrappers 1 --worker-threads 2 --log-level info --traces-sample-rate 0 --mailbox-size 16384 --deque-size 256 --signature-threads 2 --leader-mode rotating --leader-delay-ms 0 --output test local --start-port 3000 --indexers 'http://localhost:8080:1'
```

The generator prints the network identity's digest, startup commands for indexers and validators,
and commands for reading each validator's metrics. The emitted indexer command
(`cargo run --bin indexer -- --port <port> --identity <participant-set-hex> --block-size <size>`)
passes the identity on the command line, which suits small local networks. A deployed indexer
reads its identity and block size from `indexer.yaml`.

#### Start Validators

Run the emitted start commands in separate terminals:

```bash
cargo run --release --bin validator -- --peers test/peers.yaml --config test/<validator-host-name>.yaml
```

_It is necessary to start at least one bootstrapper for any other peers to connect (used to exchange IPs to dial, not as a relay)._

#### [Optional] Configure Explorer

The indexer embeds `explorer/build` at compile time. To browse a local network at
`http://localhost:8080`, first [build the embedded explorer](#build-the-embedded-explorer), then
compile and start (or restart) the indexer using the emitted `cargo run` command. Its network
identity is injected automatically. To run the explorer from source,
generate its configuration (pass the indexer as `host:port` without a scheme)
and copy it over `explorer/src/local_config.ts`:

```bash
cargo run --bin deploy -- explorer --dir test --backend-url localhost:8080
cp test/config.ts explorer/src/local_config.ts
cd explorer && npm start
```

#### Debugging

##### Too Many Open Files

If you see an error like `unable to append to journal: Runtime(BlobOpenFailed("engine-consensus", "00000000000000ee", Os { code: 24, kind: Uncategorized, message: "Too many open files" }))`, you may need to increase the maximum number of open files. You can do this by running:

```bash
ulimit -n 65536
```

_MacOS defaults to 256 open files, which is too low for the default settings (where 1 journal file is maintained per recent view)._

### Remote

Install [Rust](https://www.rust-lang.org/tools/install), [Node.js with npm](https://nodejs.org/),
[wasm-pack](https://rustwasm.github.io/wasm-pack/installer/),
[Docker with Buildx](https://docs.docker.com/build/buildx/install/), and
[just](https://just.systems/man/en/packages.html). On macOS, also install
Homebrew LLVM with `brew install llvm` for the explorer's WebAssembly build.

#### Install `commonware-deployer`

```bash
cargo install commonware-deployer --features aws
```

#### Create Artifacts

Pass `--indexer` to deploy Alto's indexer alongside the validators. The generator configures one
validator in each region to upload to `http://indexer:8080`. Open the indexer's public URL on
port 8080 to view the explorer. To use external indexers, pass
`--indexers '<url>:<count>[;<url>:<count>...]'`, for example,
`https://idx-a.example.com:2;https://idx-b.example.com:1`. Uploaders are selected round-robin
across regions.

Deployments also accept regions without map coordinates. Their validators remain in the count
and have no map marker.

Each selected validator config will contain:

```yaml
indexer: https://your-indexer.example.com
```

##### Global (scripted)

```bash
./deploy.sh stable
./deploy.sh rotating
```

`deploy.sh` takes the leader mode as its only argument. It generates the configuration, tests and
builds the explorer, builds the validator and indexer binaries with `just graviton-binaries`,
creates the Global cluster, and prints the explorer URL. The remaining deployment steps describe
the manual flow used for the USA cluster.

The script deploys 50 validators and one indexer on `c7gd.4xlarge` instances. Each validator uses
8 worker threads and 16 signature threads. Stable mode uses a 5 ms proposal interval and pipelines
100,000-view leader terms with 48 optimistic views. Rotating mode uses zero proposal delay and a
new round-robin leader every view. Deployment concurrency is 50.

_Each `c7gd.4xlarge` provides a 950GB ephemeral NVMe instance store, which the deployer mounts at
`/home/ubuntu` for validator data. The 25GB storage setting sizes the gp3 root volume. Terminating
or replacing an instance discards its NVMe data._

##### USA

```bash
cargo run --bin deploy -- generate --peers 50 --bootstrappers 5 --worker-threads 2 --log-level info --traces-sample-rate 0 --mailbox-size 16384 --deque-size 256 --signature-threads 2 --leader-mode stable --leader-delay-ms 10 --leader-term-length 1000 --leader-optimistic-views 48 --output assets remote --regions us-east-1,us-east-2,us-west-1,us-west-2 --monitoring-instance-type c8g.4xlarge --monitoring-storage-size 100 --instance-type c8g.large --storage-size 75 --dashboard deploy/dashboard.json
```

_Validators retain finalized blocks. Monitor disk usage as the finalized history grows._

#### [Optional] Configure Explorer

An indexer deployed with `--indexer` serves an explorer configured for its network at
`http://<indexer-ip>:8080/`. Build that explorer from the same revision as the validators.

#### [Optional] Configure Followers and Inspector

Use the generated network identity and block size for every client. Deployments with `--indexer`
store these values in `assets/indexer.yaml`.

Set `source`, `identity`, and `block_size` in the follower configuration. Pass `--indexer`,
`--identity`, and `--block-size` to the inspector. Use fresh follower data directories when
connecting to a newly generated network. The inspector's `--identity` must fit the platform's
argument limit: on Linux, 128 KiB holds the identity of at most 73 participants.

#### Build the Embedded Explorer

Install Node.js/npm and `wasm-pack`, then run from the repository root before compiling an indexer
that will serve the explorer:

```bash
npm --prefix explorer ci
npm --prefix explorer run build
```

On macOS, install Homebrew LLVM (`brew install llvm`) and replace the second command with:

```bash
CC="$(brew --prefix llvm)/bin/clang" AR="$(brew --prefix llvm)/bin/llvm-ar" npm --prefix explorer run build
```

`deploy.sh` performs this step automatically. An indexer compiled without `explorer/build` serves
only the API. Building the frontend afterward requires recompiling and restarting the indexer.

#### Build Deployment Binaries

Before building an indexer that serves the explorer, complete
[Build the Embedded Explorer](#build-the-embedded-explorer).

Run the recipe for the deployment's instance type from the repository root:

| Recipe | Architecture | CPU target | Example instances |
| --- | --- | --- | --- |
| `just graviton-binaries` | ARM64 | `neoverse-512tvb` | Graviton 3/4/5 (`c7g`, `c8g`, `c9g`) |
| `just graviton4-binaries` | ARM64 | `neoverse-v2` | Graviton 4 (`c8g`, `m8g`, `r8g`, `i8g`) |
| `just intel-binaries` | x86-64 | `emeraldrapids` | Intel I7i |

Each recipe writes `assets/validator`, `assets/indexer`, and their debug-symbol variants. Graviton 4
binaries require CPU features unavailable on earlier Graviton generations.

The builder runs on the local Docker architecture and cross-compiles the binaries, so no
`--platform` argument is needed on an ARM64 development machine.

##### Local Compilation

To build against a local checkout of the monorepo, change the `commonware-*` dependencies in
`Cargo.toml` to `path = "/monorepo/<crate>"` entries. Generate the lockfile inside the container,
where the monorepo is mounted, then build each binary with that lockfile:

```bash
just build-intel-image
docker run --rm -v "${PWD}:/alto" -v "${PWD}/../monorepo:/monorepo" alto-validator-builder:intel-local 'cargo generate-lockfile'
docker run --rm -v "${PWD}:/alto" -v "${PWD}/../monorepo:/monorepo" alto-validator-builder:intel-local
docker run --rm -v "${PWD}:/alto" -v "${PWD}/../monorepo:/monorepo" -e BINARY_NAME=indexer alto-validator-builder:intel-local
```

Emitted binaries are placed in `assets/`.

#### Deploy Cluster

```bash
cd assets
deployer aws create --config config.yaml
```

_If your deployer machine has limited bandwidth, use `--concurrency <concurrency>` to lower the maximum number of instances configured at once (must be >= 1, default: 128)._

#### Monitor Performance on Grafana

Visit `http://<monitoring-ip>:3000/d/chain`

_This dashboard is only accessible from the IP used to deploy the infrastructure. When an indexer
is deployed, its Prometheus target shows as down: the indexer exposes no metrics endpoint._

#### [Optional] Update Validator Binary

##### Re-Compile Binary

Choose the recipe for the deployment's instance type:

| Instance type | Recipe |
| --- | --- |
| Intel I7i | `just validator-intel-binary` |
| Graviton 3/4/5 | `just validator-graviton-binary` |
| Graviton 4 | `just validator-graviton4-binary` |

##### Restart Validator Binary on EC2 Instances

```bash
deployer aws update --config config.yaml
```

_`deployer aws update` replaces and restarts the binary on every instance in `config.yaml`,
including an indexer deployed with `--indexer` (its in-memory history is reset). Every binary named
in `config.yaml` must exist under `assets/`, so rebuild `assets/indexer` alongside the validator
(otherwise the existing file is pushed again)._

#### [Optional] Profile Validator

Collect a CPU profile from a running validator using `samply`:

```bash
deployer aws profile --config config.yaml --instance <instance-name> --binary validator-debug
```

The `validator-debug` binary contains debug symbols for symbolication. The profile will be saved locally and can be viewed in the [Firefox Profiler](https://profiler.firefox.com).

#### Destroy Infrastructure

```bash
deployer aws destroy --config config.yaml
```

#### Debugging

##### Missing AWS Credentials

If `commonware-deployer` can't detect your AWS credentials, you'll see a "Request has expired." error:

```
2025-03-05T01:36:47.550105Z  INFO deployer::ec2::create: created EC2 client region="eu-west-1"
2025-03-05T01:36:48.268330Z ERROR deployer: failed to create EC2 deployment error=AwsEc2(Unhandled(Unhandled { source: ErrorMetadata { code: Some("RequestExpired"), message: Some("Request has expired."), extras: Some({"aws_request_id": "006f6b92-4965-470d-8eac-7c9644744bdf"}) }, meta: ErrorMetadata { code: Some("RequestExpired"), message: Some("Request has expired."), extras: Some({"aws_request_id": "006f6b92-4965-470d-8eac-7c9644744bdf"}) } }))
```

##### EC2 Throttling

EC2 instances may throttle network traffic if a workload exceeds the allocation for a particular instance type. To check
if an instance is throttled, SSH into the instance and run:

```bash
ethtool -S ens5 | grep "allowance"
```

If throttled, you'll see a non-zero value for some "allowance" item:

```txt
bw_in_allowance_exceeded: 0
bw_out_allowance_exceeded: 14368
pps_allowance_exceeded: 0
conntrack_allowance_exceeded: 0
linklocal_allowance_exceeded: 0
```
