# alto-chain

[![Crates.io](https://img.shields.io/crates/v/alto-chain.svg)](https://crates.io/crates/alto-chain)
[![Docs.rs](https://docs.rs/alto-chain/badge.svg)](https://docs.rs/alto-chain)

A minimal (and wicked fast) blockchain built with the [Commonware Library](https://github.com/commonwarexyz/monorepo).

## Status

`alto-chain` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.

## Setup

### Local

_To run this example, you must first install [Rust](https://www.rust-lang.org/tools/install)._

#### Create Artifacts

_To configure indexer upload, add `--indexer-port <port>` to the `generate local` command. The first validator is configured to push data to it._

```bash
cargo run --bin setup -- generate --peers 5 --bootstrappers 1 --worker-threads 3 --log-level info --message-backlog 16384 --mailbox-size 16384 --deque-size 10 --output test local --start-port 3000 --indexer-port 8080
```

_If setup succeeds, you should see the following output:_

```
2025-12-23T13:41:54.034863Z  INFO setup: generated network key identity=8b2c34e0356beb83874317f8f04fb211e4d3ed34640631a36ff191cb3fcd9768403b8749824b41ff770a92e40885174b15516db966816870ba9619a64b4d5b79ea7b4a73240710169ecc44da0951cdd60e2db65544cba5647f81ab19ca50cf4e
2025-12-23T13:41:54.037106Z  INFO setup: wrote peer configuration file path="04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37.yaml"
2025-12-23T13:41:54.037417Z  INFO setup: wrote peer configuration file path="0b2412d7eb2238b319920504f19b28447c7dbb3c58059c97d22cc0d27ea31e81.yaml"
2025-12-23T13:41:54.037690Z  INFO setup: wrote peer configuration file path="71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f.yaml"
2025-12-23T13:41:54.037966Z  INFO setup: wrote peer configuration file path="c58244243f263ebc975640d5bb4e43e8e78e4b41361e4e7984cd8b027480558a.yaml"
2025-12-23T13:41:54.038228Z  INFO setup: wrote peer configuration file path="f26a6d4f52c4d595b6cb659b643968b0e1fc9931b460c6407be10cebe4eeff2d.yaml"
2025-12-23T13:41:54.038232Z  INFO setup: setup complete bootstrappers=["71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f"]
To start local indexer, run:
cargo run --bin indexer -- --port 8080 --identity 8b2c34e0356beb83874317f8f04fb211e4d3ed34640631a36ff191cb3fcd9768403b8749824b41ff770a92e40885174b15516db966816870ba9619a64b4d5b79ea7b4a73240710169ecc44da0951cdd60e2db65544cba5647f81ab19ca50cf4e
To start validators, run:
04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37.yaml
0b2412d7eb2238b319920504f19b28447c7dbb3c58059c97d22cc0d27ea31e81: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/0b2412d7eb2238b319920504f19b28447c7dbb3c58059c97d22cc0d27ea31e81.yaml
71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/71943989f39d485eb8a1f7c8f9909673caaa658d12a586c93f37575dae44438f.yaml
c58244243f263ebc975640d5bb4e43e8e78e4b41361e4e7984cd8b027480558a: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/c58244243f263ebc975640d5bb4e43e8e78e4b41361e4e7984cd8b027480558a.yaml
f26a6d4f52c4d595b6cb659b643968b0e1fc9931b460c6407be10cebe4eeff2d: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/f26a6d4f52c4d595b6cb659b643968b0e1fc9931b460c6407be10cebe4eeff2d.yaml
Indexer URL: http://localhost:8080 (pushed by 04dc128c6fc22cb93a9eb785c48d4251346eb7b387cd2a66599cc59a3ce47a37)
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
cargo run --bin setup -- explorer --dir test --backend-url <backend URL> local
```

#### Debugging

##### Too Many Open Files

If you see an error like `unable to append to journal: Runtime(BlobOpenFailed("engine-consensus", "00000000000000ee", Os { code: 24, kind: Uncategorized, message: "Too many open files" }))`, you may need to increase the maximum number of open files. You can do this by running:

```bash
ulimit -n 65536
```

_MacOS defaults to 256 open files, which is too low for the default settings (where 1 journal file is maintained per recent view)._

### Remote

_To run this example, you must first install [Rust](https://www.rust-lang.org/tools/install) and [Docker](https://www.docker.com/get-started/)._

#### Install `commonware-deployer`

```bash
cargo install commonware-deployer
```

#### Create Artifacts

_To configure indexer upload, add `--indexer-url <URL> --indexer-count <count>` to the `generate remote` command. Indexers are selected in round-robin fashion across regions._

##### Global

```bash
cargo run --bin setup -- generate --peers 50 --bootstrappers 5 --worker-threads 2 --log-level info --message-backlog 16384 --mailbox-size 16384 --deque-size 10 --output assets remote --regions us-west-1,us-east-1,eu-west-1,ap-northeast-1,eu-north-1,ap-south-1,sa-east-1,eu-central-1,ap-northeast-2,ap-southeast-2 --monitoring-instance-type c7g.4xlarge --monitoring-storage-size 100 --instance-type c7g.large --storage-size 25 --dashboard dashboard.json
```

_This configuration consumes ~10MB of disk space per hour per validator (~5 views per second). With 25GB of storage allocated, validators will exhaust available storage in ~3 months._

##### USA

```bash
cargo run --bin setup -- generate --peers 50 --bootstrappers 5 --worker-threads 2 --log-level info --message-backlog 16384 --mailbox-size 16384 --deque-size 10 --output assets remote --regions us-east-1,us-east-2,us-west-1,us-west-2 --monitoring-instance-type c7g.4xlarge --monitoring-storage-size 100 --instance-type c7g.large --storage-size 75 --dashboard dashboard.json
```

_This configuration consumes ~30MB of disk space per hour per validator (~13 views per second). With 75GB of storage allocated, validators will exhaust available storage in ~3 months._

#### [Optional] Configure Explorer

```bash
cargo run --bin setup -- explorer --dir assets --backend-url <backend URL> remote
```

#### Build Validator Binary

##### Build Cross-Platform Compiler

```bash
docker build -t validator-builder .
```

##### Compile Binary for ARM64

```bash
docker run -it -v ${PWD}/..:/alto validator-builder
```

###### Local Compilation

_Before running this command, ensure you change any `version` dependencies you'd like to compile locally to `path` dependencies in `Cargo.toml`._

```bash
docker run -it -v ${PWD}/..:/alto -v ${PWD}/../../monorepo:/monorepo validator-builder
```

_Emitted binary `validator` is placed in `assets`._

#### Deploy Validator Binary

```bash
cd assets
deployer ec2 create --config config.yaml
```

#### Monitor Performance on Grafana

Visit `http://<monitoring-ip>:3000/d/chain`

_This dashboard is only accessible from the IP used to deploy the infrastructure._

#### [Optional] Update Validator Binary

##### Re-Compile Binary for ARM64

```bash
docker run -it -v ${PWD}/..:/alto validator-builder
```

##### Restart Validator Binary on EC2 Instances

```bash
deployer ec2 update --config config.yaml
```

#### Destroy Infrastructure

```bash
deployer ec2 destroy --config config.yaml
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
