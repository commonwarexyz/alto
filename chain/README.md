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
2025-04-30T01:39:42.495691Z  INFO setup: generated network key identity=aaadf87ccd821dec083ada0cfaf494ef33c180458fc69e5804b523200d4ef90b469fda59a50504922942f71feffbd6bf
2025-04-30T01:39:42.497013Z  INFO setup: wrote peer configuration file path="10cf8d03daca2332213981adee2a4bfffe4a1782bb5cce036c1d5689c6090997.yaml"
2025-04-30T01:39:42.497236Z  INFO setup: wrote peer configuration file path="3ed17734da2f5f718f3386cd73d69b93f3d421da6dc216ef8064fc93161cc75a.yaml"
2025-04-30T01:39:42.497455Z  INFO setup: wrote peer configuration file path="82f2bdca758f7ecae48ea21678d121938e1c9d98a3853ef402c4612548ce7141.yaml"
2025-04-30T01:39:42.497684Z  INFO setup: wrote peer configuration file path="888ba06f969372f0471b9593b054bef8ad0dcdd8690257eee1424b67af2157f2.yaml"
2025-04-30T01:39:42.497899Z  INFO setup: wrote peer configuration file path="a77b691b016ae389cf1b4765c2bdc12060548b525cae3986b5953d5da74593e0.yaml"
2025-04-30T01:39:42.497907Z  INFO setup: emitting start commands bootstrappers=["888ba06f969372f0471b9593b054bef8ad0dcdd8690257eee1424b67af2157f2"]
cargo run --bin validator -- --peers=<your-path-to-alto>/alto/chain/test/peers.yaml --config=<your-path-to-alto>/alto/chain/test/10cf8d03daca2332213981adee2a4bfffe4a1782bb5cce036c1d5689c6090997.yaml
cargo run --bin validator -- --peers=<your-path-to-alto>/alto/chain/test/peers.yaml --config=<your-path-to-alto>/alto/chain/test/3ed17734da2f5f718f3386cd73d69b93f3d421da6dc216ef8064fc93161cc75a.yaml
cargo run --bin validator -- --peers=<your-path-to-alto>/alto/chain/test/peers.yaml --config=<your-path-to-alto>/alto/chain/test/82f2bdca758f7ecae48ea21678d121938e1c9d98a3853ef402c4612548ce7141.yaml
cargo run --bin validator -- --peers=<your-path-to-alto>/alto/chain/test/peers.yaml --config=<your-path-to-alto>/alto/chain/test/888ba06f969372f0471b9593b054bef8ad0dcdd8690257eee1424b67af2157f2.yaml
cargo run --bin validator -- --peers=<your-path-to-alto>/alto/chain/test/peers.yaml --config=<your-path-to-alto>/alto/chain/test/a77b691b016ae389cf1b4765c2bdc12060548b525cae3986b5953d5da74593e0.yaml
```

#### Start Validators

Run the emitted start commands in separate terminals:

```bash
cargo run --bin validator -- --peers=<your-path-to-alto>/alto/chain/test/peers.yaml --config=<your-path-to-alto>/alto/chain/test/10cf8d03daca2332213981adee2a4bfffe4a1782bb5cce036c1d5689c6090997.yaml
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
cargo run --bin setup -- generate --peers 50 --bootstrappers 5 --worker-threads 3 --log-level info --message-backlog 16384 --mailbox-size 16384 --deque-size 10 --output assets remote --regions us-west-1,us-east-1,eu-west-1,ap-northeast-1,eu-north-1,ap-south-1,sa-east-1,eu-central-1,ap-northeast-2,ap-southeast-2 --monitoring-instance-type c7g.4xlarge --monitoring-storage-size 100 --instance-type c7g.xlarge --storage-size 40 --dashboard dashboard.json
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