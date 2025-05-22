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

```bash
cargo run --bin setup -- generate --peers 5 --bootstrappers 1 --worker-threads 3 --log-level info --message-backlog 16384 --mailbox-size 16384 --deque-size 10 --output test local --start-port 3000
```

_If setup succeeds, you should see the following output:_

```
2025-05-02T14:47:55.906379Z  INFO setup: generated network key identity=ab6284904e71efb665c42f7ab1f713bfc2c87e2bd937c4027514cea74ef588c05803a4592ddd1970def6bd261210b83b
2025-05-02T14:47:55.907805Z  INFO setup: wrote peer configuration file path="4cf00f5c66ed27ba3e753f6a8b989b306eae5ce2d3f3c2db105aae2123a012c8.yaml"
2025-05-02T14:47:55.908022Z  INFO setup: wrote peer configuration file path="95ce6a717dfc7b7dc8dcded623c4bc5ce7a6b4e9c986e923baa3acc5078d7a0f.yaml"
2025-05-02T14:47:55.908239Z  INFO setup: wrote peer configuration file path="f79141801d52e8a1a7f16b639038032f1a402707ad51f6d7aa94098c8f07e068.yaml"
2025-05-02T14:47:55.908458Z  INFO setup: wrote peer configuration file path="f79a65c60ac706e67dd964cf4cde9b804c89c15c330f00c9b0adc2ef51d6616c.yaml"
2025-05-02T14:47:55.908669Z  INFO setup: wrote peer configuration file path="fa6fffb46bb3aceecde1324ac31d8cfddda6c0857a63567796ff8507fef1a965.yaml"
2025-05-02T14:47:55.908677Z  INFO setup: setup complete bootstrappers=["fa6fffb46bb3aceecde1324ac31d8cfddda6c0857a63567796ff8507fef1a965"]
To start validators, run:
4cf00f5c66ed27ba3e753f6a8b989b306eae5ce2d3f3c2db105aae2123a012c8: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/4cf00f5c66ed27ba3e753f6a8b989b306eae5ce2d3f3c2db105aae2123a012c8.yaml
95ce6a717dfc7b7dc8dcded623c4bc5ce7a6b4e9c986e923baa3acc5078d7a0f: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/95ce6a717dfc7b7dc8dcded623c4bc5ce7a6b4e9c986e923baa3acc5078d7a0f.yaml
f79141801d52e8a1a7f16b639038032f1a402707ad51f6d7aa94098c8f07e068: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/f79141801d52e8a1a7f16b639038032f1a402707ad51f6d7aa94098c8f07e068.yaml
f79a65c60ac706e67dd964cf4cde9b804c89c15c330f00c9b0adc2ef51d6616c: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/f79a65c60ac706e67dd964cf4cde9b804c89c15c330f00c9b0adc2ef51d6616c.yaml
fa6fffb46bb3aceecde1324ac31d8cfddda6c0857a63567796ff8507fef1a965: cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/fa6fffb46bb3aceecde1324ac31d8cfddda6c0857a63567796ff8507fef1a965.yaml
To view metrics, run:
4cf00f5c66ed27ba3e753f6a8b989b306eae5ce2d3f3c2db105aae2123a012c8: curl http://localhost:3001/metrics
95ce6a717dfc7b7dc8dcded623c4bc5ce7a6b4e9c986e923baa3acc5078d7a0f: curl http://localhost:3003/metrics
f79141801d52e8a1a7f16b639038032f1a402707ad51f6d7aa94098c8f07e068: curl http://localhost:3005/metrics
f79a65c60ac706e67dd964cf4cde9b804c89c15c330f00c9b0adc2ef51d6616c: curl http://localhost:3007/metrics
fa6fffb46bb3aceecde1324ac31d8cfddda6c0857a63567796ff8507fef1a965: curl http://localhost:3009/metrics
```

#### Start Validators

Run the emitted start commands in separate terminals:

```bash
cargo run --bin validator -- --peers=<your-path>/test/peers.yaml --config=<your-path>/test/10cf8d03daca2332213981adee2a4bfffe4a1782bb5cce036c1d5689c6090997.yaml
```

_It is necessary to start at least one bootstrapper for any other peers to connect (used to exchange IPs to dial, not as a relay)._

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

```bash
cargo run --bin setup -- generate --peers 50 --bootstrappers 5 --worker-threads 4 --log-level info --message-backlog 16384 --mailbox-size 16384 --deque-size 10 --output assets remote --regions us-west-1,us-east-1,eu-west-1,ap-northeast-1,eu-north-1,ap-south-1,sa-east-1,eu-central-1,ap-northeast-2,ap-southeast-2 --monitoring-instance-type c7g.4xlarge --monitoring-storage-size 100 --instance-type c7g.xlarge --storage-size 40 --dashboard dashboard.json
```

_We use 1 less `worker-threads` than the number of `vCPUs` to leave a core for `blocking-threads`._

#### [Optional] Configure Indexer Upload

```bash
cargo run --bin setup -- indexer --count <uploaders> --dir assets --url <indexer URL>
```

_The indexer URL is configured separately because it is typically only known after the threshold key is generated (derived in `setup generate`). The iteration order of this command is deterministic (re-running will update the same configuration files)._

#### [Optional] Configure Explorer

```bash
cargo run --bin setup -- explorer --dir assets --backend-url <backend URL>
```

_The backend URL should be a WebSocket endpoint (with a `ws://` or `wss://` prefix)._

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