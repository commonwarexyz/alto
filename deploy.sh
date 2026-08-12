#!/usr/bin/env bash
# Generate, build, and deploy the global Alto cluster.
set -euo pipefail
cd "$(dirname "$0")"

for tool in cargo just docker deployer npm; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "missing required command: $tool" >&2
        exit 1
    fi
done
if ! docker info >/dev/null 2>&1; then
    echo "docker daemon is unavailable" >&2
    exit 1
fi
if ! docker buildx version >/dev/null 2>&1; then
    echo "docker buildx is unavailable" >&2
    exit 1
fi
if [ ! -f deploy/dashboard.json ]; then
    echo "missing deploy/dashboard.json" >&2
    exit 1
fi

if [ -d assets ]; then
    read -r -p "./assets exists — remove and regenerate? [y/N] " answer
    [ "$answer" = "y" ] || exit 1
    rm -rf ./assets
fi

# Keep enough of the 16-vCPU machine free for networking, storage, and consensus
# while reserving most of the remaining compute for signature verification.
cargo run --locked --bin deploy -- generate \
    --peers 50 \
    --bootstrappers 5 \
    --worker-threads 4 \
    --log-level info \
    --mailbox-size 16384 \
    --deque-size 256 \
    --signature-threads 12 \
    --leader-mode stable \
    --leader-delay-ms 5 \
    --leader-term-length 100000 \
    --output assets \
    remote \
    --regions us-west-1,us-east-1,eu-west-1,ap-northeast-1,eu-north-1,ap-south-1,sa-east-1,eu-central-1,ap-northeast-2,ap-southeast-2 \
    --monitoring-instance-type c8g.4xlarge \
    --monitoring-storage-size 100 \
    --instance-type i7i.4xlarge \
    --storage-size 25 \
    --dashboard deploy/dashboard.json \
    --indexer

for artifact in config.yaml dashboard.json indexer.yaml; do
    if [ ! -f "assets/$artifact" ]; then
        echo "deployment generator did not create assets/$artifact" >&2
        exit 1
    fi
done

npm --prefix explorer ci
GENERATE_SOURCEMAP=false npm --prefix explorer run build:react
if [ ! -f explorer/build/index.html ]; then
    echo "explorer build did not create explorer/build/index.html" >&2
    exit 1
fi

just intel-binaries
for artifact in validator indexer; do
    if [ ! -x "assets/$artifact" ]; then
        echo "build did not create executable assets/$artifact" >&2
        exit 1
    fi
done

(cd assets && deployer aws create --config config.yaml --concurrency 50)

DEPLOY_TAG="$(sed -n 's/^tag: //p' assets/config.yaml)"
HOSTS_PATH="${HOME}/.commonware_deployer/${DEPLOY_TAG}/hosts.yaml"
if [ -f "$HOSTS_PATH" ]; then
    INDEXER_IP="$(awk '
        $1 == "-" && $2 == "name:" { current = $3; next }
        $1 == "name:" { current = $2; next }
        current == "indexer" && $1 == "ip:" { print $2; exit }
    ' "$HOSTS_PATH")"
    if [ -n "$INDEXER_IP" ]; then
        echo "Explorer: http://${INDEXER_IP}:8080/"
    else
        echo "Explorer is on port 8080; indexer IP is in $HOSTS_PATH"
    fi
fi
