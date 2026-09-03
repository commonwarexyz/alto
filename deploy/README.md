# alto-deploy

Deploy an instance of [alto](../README.md).

## Deploy

### Local

_To run a deploy, you must first install [Rust](https://www.rust-lang.org/tools/install)._

#### Create Artifacts

_To configure local indexer upload, add `--indexers '<url>:<count>[;<url>:<count>...]'` to the `generate local` command. For example, `http://localhost:8080:1` assigns one validator to upload to that indexer._

Leader election is selected for every generated validator with `--leader-mode`, and
`--leader-delay-ms` sets the minimum proposal interval. Use `--leader-mode rotating` for a new
VRF-derived leader each view. Stable mode keeps one round-robin leader for each term and also
requires `--leader-term-length` to set the number of views in each term and
`--leader-optimistic-views` to cap proposal issuance and vote admission ahead of directly
notarized ancestry. Values above the term length add no window. Stable mode uses native standard
threshold crypto with one signature per vote and certificate. Rotating mode carries an additional
VRF seed signature used to select the next leader.

Changing an existing network from VRF or wrapper-framed standard certificates to native standard
certificates is not a rolling upgrade: the untagged wire and persistent certificate layouts have
different fixed sizes. Start with newly generated network configuration and empty validator and
follower certificate state, then switch every validator, indexer, follower, and browser verifier
together. `deploy.sh` generates a new network identity and infrastructure for this purpose.

`--block-size` is a `u32` setting for the number of random bytes appended to each proposed block
and defaults to `0`. Validators reject only values whose encoded blocks exceed the authenticated
transport capacity. All validators in a network must use the same value.

`--traces-sample-rate` sets the fraction of traces exported by each validator and accepts values
from `0` through `1`. It defaults to `0`, which disables trace export.

Generated validator configs use:

```yaml
leader:
  mode: stable
  delay_ms: 10
  term_length: 1000
  optimistic_views: 48
block_size: 0
traces_sample_rate: 0.0
indexer: http://localhost:8080
```

```bash
cargo run --bin deploy -- generate --peers 5 --bootstrappers 1 --worker-threads 3 --log-level info --traces-sample-rate 0 --mailbox-size 16384 --deque-size 256 --signature-threads 2 --leader-mode stable --leader-delay-ms 10 --leader-term-length 1000 --leader-optimistic-views 48 --output test local --start-port 3000 --indexers 'http://localhost:8080:1'
```

_If the command succeeds, you should see the following output:_

```
2025-12-23T13:41:54.034863Z  INFO setup: generated network key identity=8b2c34e0356beb83874317f8f04fb211e4d3ed34640631a36ff191cb3fcd9768403b8749824b41ff770a92e40885174b15516db966816870ba9619a64b4d5b79ea7b4a73240710169ecc44da0951cdd60e2db65544cba5647f81ab19ca50cf4e
2025-12-23T13:41:54.037106Z  INFO setup: wrote peer configuration file path="04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37.yaml"
2025-12-23T13:41:54.037417Z  INFO setup: wrote peer configuration file path="0b2412d7eb2238b319920504f19b28447c7dbb3c58059c97d22cc0d27ea31e81.yaml"
2025-12-23T13:41:54.037690Z  INFO setup: wrote peer configuration file path="71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f.yaml"
2025-12-23T13:41:54.037966Z  INFO setup: wrote peer configuration file path="c58244243f263ebc975640d5bb4e43e8e78e4b41361e4e7984cd8b027480558a.yaml"
2025-12-23T13:41:54.038228Z  INFO setup: wrote peer configuration file path="f26a6d4f52c4d595b6cb659b643968b0e1fc9931b460c6407be10cebe4eeff2d.yaml"
2025-12-23T13:41:54.038232Z  INFO setup: setup complete bootstrappers=["71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f"]
To start local indexers, run:
http://localhost:8080: cargo run --bin indexer -- --port 8080 --identity 8b2c34e0356beb83874317f8f04fb211e4d3ed34640631a36ff191cb3fcd9768403b8749824b41ff770a92e40885174b15516db966816870ba9619a64b4d5b79ea7b4a73240710169ecc44da0951cdd60e2db65544cba5647f81ab19ca50cf4e --certificate-mode standard
To start validators, run:
04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37.yaml
0b2412d7eb2238b319920504f19b28447c7dbb3c58059c97d22cc0d27ea31e81: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/0b2412d7eb2238b319920504f19b28447c7dbb3c58059c97d22cc0d27ea31e81.yaml
71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f.yaml
c58244243f263ebc975640d5bb4e43e8e78e4b41361e4e7984cd8b027480558a: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/c58244243f263ebc975640d5bb4e43e8e78e4b41361e4e7984cd8b027480558a.yaml
f26a6d4f52c4d595b6cb659b643968b0e1fc9931b460c6407be10cebe4eeff2d: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/f26a6d4f52c4d595b6cb659b643968b0e1fc9931b460c6407be10cebe4eeff2d.yaml
Configured indexers:
04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37: http://localhost:8080
To view metrics, run:
04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37: curl http://localhost:3001/metrics
0b2412d7eb2238b319920504f19b28447c7dbb3c58059c97d22cc0d27ea31e81: curl http://localhost:3003/metrics
71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f: curl http://localhost:3005/metrics
c58244243f263ebc975640d5bb4e43e8e78e4b41361e4e7984cd8b027480558a: curl http://localhost:3007/metrics
f26a6d4f52c4d595b6cb659b643968b0e1fc9931b460c6407be10cebe4eeff2d: curl http://localhost:3009/metrics
```

#### Start Validators

Run the emitted start commands in separate terminals:

```bash
cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/10cf8d03daca2332213981adee2a4bfffe4a1782bb5cce036c1d5689c6090997.yaml
```

_It is necessary to start at least one bootstrapper for any other peers to connect (used to exchange IPs to dial, not as a relay)._

#### [Optional] Configure Explorer

```bash
cargo run --bin deploy -- explorer --dir test --backend-url <backend URL> local
```

#### Debugging

##### Too Many Open Files

If you see an error like `unable to append to journal: Runtime(BlobOpenFailed("engine-consensus", "00000000000000ee", Os { code: 24, kind: Uncategorized, message: "Too many open files" }))`, you may need to increase the maximum number of open files. You can do this by running:

```bash
ulimit -n 65536
```

_MacOS defaults to 256 open files, which is too low for the default settings (where 1 journal file is maintained per recent view)._

### Remote

_To run a deploy, you must first install [Rust](https://www.rust-lang.org/tools/install), [Node.js with npm](https://nodejs.org/), [Docker with Buildx](https://docs.docker.com/build/buildx/install/), and [just](https://just.systems/man/en/packages.html)._

#### Install `commonware-deployer`

```bash
cargo install commonware-deployer --features aws
```

#### Create Artifacts

Pass `--indexer` to deploy Alto's indexer alongside the validators. The generator configures one
validator in each region to upload to `http://indexer:8080`. The indexer also serves the explorer
from port 8080, so its public URL can be opened directly from a laptop. To use indexers managed
outside this deployment instead, pass `--indexers '<url>:<count>[;<url>:<count>...]'`; for example,
`https://idx-a.example.com:2;https://idx-b.example.com:1`. Uploaders are selected round-robin
across regions.

Each selected validator config will contain:

```yaml
indexer: https://your-indexer.example.com
```

##### Global

```bash
./deploy.sh
```

This deploys 50 validators on 16-core `c7gd.4xlarge` Graviton 3 instances with 8 Tokio workers and
16 signature threads, a 5ms proposal target, and 100,000-view stable-leader terms. It builds the
explorer into an indexer on the same instance type, deploys with concurrency 50, and prints the
explorer URL when deployment completes. Validators retain the full finalized history in immutable
archives. Validator and indexer binaries use the cross-generation Graviton recipe targeting
`neoverse-512tvb`, AWS's recommended compiler target for Graviton 3.

The runtime and signature pools deliberately oversubscribe the 16 single-threaded cores. Treat
8/16 as a deployment tuning point and compare CPU saturation and network backlog before reusing it
for another workload.

_C7gd provides a 950GB ephemeral NVMe instance store, which the deployer mounts at `/home/ubuntu`
for validator data. The 25GB storage setting sizes the gp3 root volume; terminating or replacing an
instance discards its NVMe data._

##### USA

```bash
cargo run --bin deploy -- generate --peers 50 --bootstrappers 5 --worker-threads 2 --log-level info --traces-sample-rate 0 --mailbox-size 16384 --deque-size 256 --signature-threads 2 --leader-mode stable --leader-delay-ms 10 --leader-term-length 1000 --leader-optimistic-views 48 --output assets remote --regions us-east-1,us-east-2,us-west-1,us-west-2 --monitoring-instance-type c8g.4xlarge --monitoring-storage-size 100 --instance-type c8g.large --storage-size 75 --dashboard deploy/dashboard.json
```

_This configuration consumes ~30MB of disk space per hour per validator (~13 views per second). With 75GB of storage allocated, validators will exhaust available storage in ~3 months._

#### [Optional] Configure Explorer

```bash
cargo run --bin deploy -- explorer --dir assets --backend-url <backend URL> remote
```

#### [Optional] Update Public Key

After redeploying a cluster, update the identity (BLS12-381 threshold public key) across example configs and the inspector default:

```bash
# Global cluster:
OLD_KEY=$(sed -nE 's/^identity: "(.*)"$/\1/p' follower/examples/global.yml)
NEW_KEY="<new-key-hex>"
sed -i '' "s/$OLD_KEY/$NEW_KEY/g" follower/examples/global.yml
sed -i '' -E "s|^const DEFAULT_IDENTITY: &str = \".*\";|const DEFAULT_IDENTITY: &str = \"$NEW_KEY\";|" inspector/src/main.rs

# USA cluster:
OLD_KEY=$(sed -nE 's/^identity: "(.*)"$/\1/p' follower/examples/usa.yml)
NEW_KEY="<new-key-hex>"
sed -i '' "s/$OLD_KEY/$NEW_KEY/g" follower/examples/usa.yml
```

#### Build Deployment Binaries

The build platform is an explicit recipe:

| Recipe | Output architecture | CPU target | Example instances |
| --- | --- | --- | --- |
| `just validator-graviton-binary` | ARM64 | `neoverse-512tvb` | Graviton 3/4/5 (`c7g`, `c8g`, `c9g`) |
| `just indexer-graviton-binary` | ARM64 | `neoverse-512tvb` | Graviton 3/4/5 (`c7g`, `c8g`, `c9g`) |
| `just validator-graviton4-binary` | ARM64 | `neoverse-v2` | Graviton 4 (`c8g`, `m8g`, `r8g`, `i8g`) |
| `just indexer-graviton4-binary` | ARM64 | `neoverse-v2` | Graviton 4 (`c8g`, `m8g`, `r8g`, `i8g`) |
| `just validator-intel-binary` | x86-64 | `emeraldrapids` | Intel I7i |
| `just indexer-intel-binary` | x86-64 | `emeraldrapids` | Intel I7i |

##### Intel I7i

```bash
just intel-binaries
```

_The Intel binary is compiled with `target-cpu=emeraldrapids`._

The builder runs on the local Docker architecture and cross-compiles the binaries, so no
`--platform` argument is needed on an ARM64 development machine.

##### Graviton 3/4/5 Compatibility

```bash
just graviton-binaries
```

_The cross-generation Graviton binary is compiled with `target-cpu=neoverse-512tvb`._

##### Graviton 4

```bash
just graviton4-binaries
```

_The Graviton 4 binary is compiled with `target-cpu=neoverse-v2`, exposing the Armv9 and SVE2
features available on Graviton 4 to LLVM. Do not run this binary on earlier Graviton generations._

The grouped recipes write `assets/validator`, `assets/indexer`, and their debug-symbol variants.
Run the recipe matching the deployment's instance type last.

###### Local Compilation

_Before running this command, ensure you change any `version` dependencies you'd like to compile locally to `path` dependencies in `Cargo.toml`._

```bash
just build-intel-image
docker run --rm -v "${PWD}:/alto" -v "${PWD}/../monorepo:/monorepo" alto-validator-builder:intel-local
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

_This dashboard is only accessible from the IP used to deploy the infrastructure._

#### [Optional] Update Validator Binary

##### Re-Compile Binary

```bash
# Intel I7i
just validator-intel-binary

# Graviton 3/4/5 compatibility
just validator-graviton-binary

# Graviton 4
just validator-graviton4-binary
```

##### Restart Validator Binary on EC2 Instances

```bash
deployer aws update --config config.yaml
```

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
