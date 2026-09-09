#!/usr/bin/env bash
# Generate, build, and deploy the global Alto cluster in the given leader mode.
set -euo pipefail
cd "$(dirname "$0")"

usage() {
    echo "usage: $0 <stable|rotating>" >&2
    exit 1
}
[ $# -eq 1 ] || usage
case "$1" in
    stable)
        # One round-robin leader per term with a 48-view optimistic window.
        leader_flags=(--leader-mode stable --leader-delay-ms 5 --leader-term-length 100000 --leader-optimistic-views 48)
        ;;
    rotating)
        # A VRF-seeded leader for every view.
        leader_flags=(--leader-mode rotating --leader-delay-ms 0)
        ;;
    *)
        usage
        ;;
esac

for tool in cargo just docker deployer npm wasm-pack; do
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

explorer_build_env=()
if [ "$(uname -s)" = "Darwin" ]; then
    if ! command -v brew >/dev/null 2>&1; then
        echo "Homebrew LLVM is required to compile the explorer's WebAssembly on macOS" >&2
        exit 1
    fi
    if ! llvm_prefix="$(brew --prefix llvm 2>/dev/null)"; then
        echo "Homebrew LLVM is not installed. Run: brew install llvm" >&2
        exit 1
    fi
    wasm_clang="${llvm_prefix}/bin/clang"
    wasm_ar="${llvm_prefix}/bin/llvm-ar"
    if [ ! -x "$wasm_clang" ] || [ ! -x "$wasm_ar" ]; then
        echo "Homebrew LLVM is missing clang or llvm-ar under ${llvm_prefix}/bin" >&2
        exit 1
    fi
    if ! printf '' | "$wasm_clang" --target=wasm32-unknown-unknown -x c -c -o /dev/null -; then
        echo "Homebrew clang cannot compile wasm32-unknown-unknown" >&2
        exit 1
    fi
    explorer_build_env=("CC=${wasm_clang}" "AR=${wasm_ar}")
fi

if [ -d assets ]; then
    read -r -p "./assets exists — remove and regenerate? [y/N] " answer
    [ "$answer" = "y" ] || exit 1
    rm -rf ./assets
fi

# The c7gd.4xlarge has 16 Graviton3 cores. Use 8 runtime threads and 16 signature
# threads to overlap network and signature work.
cargo run --locked --bin deploy -- generate \
    --peers 50 \
    --bootstrappers 5 \
    --worker-threads 8 \
    --network-buffer-pool-max-per-class 16384 \
    --log-level info \
    --traces-sample-rate 0 \
    --mailbox-size 16384 \
    --deque-size 256 \
    --block-size 0 \
    --signature-threads 16 \
    "${leader_flags[@]}" \
    --output assets \
    remote \
    --regions us-west-1,us-east-1,eu-west-1,ap-northeast-1,eu-north-1,ap-south-1,sa-east-1,eu-central-1,ap-northeast-2,ap-southeast-2 \
    --monitoring-instance-type c8g.4xlarge \
    --monitoring-storage-size 100 \
    --instance-type c7gd.4xlarge \
    --storage-size 25 \
    --dashboard deploy/dashboard.json \
    --indexer

npm --prefix explorer ci
CI=true npm --prefix explorer test -- --watchAll=false --runInBand
env ${explorer_build_env[@]+"${explorer_build_env[@]}"} BUILD_PATH=build PUBLIC_URL=/ GENERATE_SOURCEMAP=false npm --prefix explorer run build
if [ ! -f explorer/build/index.html ]; then
    echo "explorer build did not create explorer/build/index.html" >&2
    exit 1
fi

just graviton-binaries

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
        echo "Explorer is on port 8080. The indexer IP is in $HOSTS_PATH"
    fi
fi
