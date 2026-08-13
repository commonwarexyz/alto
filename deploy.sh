#!/usr/bin/env bash
# Generate, build, and deploy the global Alto cluster.
set -euo pipefail
cd "$(dirname "$0")"

for tool in cargo file just docker deployer npm wasm-pack; do
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
        echo "Homebrew LLVM is not installed; run: brew install llvm" >&2
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

# C7gd exposes 16 single-threaded Graviton 3 cores. The 8/16 split is the
# deployment tuning point for overlapping network and signature work.
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
    --leader-mode stable \
    --leader-delay-ms 5 \
    --leader-term-length 100000 \
    --leader-optimistic-views 48 \
    --output assets \
    remote \
    --regions us-west-1,us-east-1,eu-west-1,ap-northeast-1,eu-north-1,ap-south-1,sa-east-1,eu-central-1,ap-northeast-2,ap-southeast-2 \
    --monitoring-instance-type c8g.4xlarge \
    --monitoring-storage-size 100 \
    --instance-type c7gd.4xlarge \
    --storage-size 25 \
    --dashboard deploy/dashboard.json \
    --indexer

for artifact in config.yaml dashboard.json indexer.yaml; do
    if [ ! -f "assets/$artifact" ]; then
        echo "deployment generator did not create assets/$artifact" >&2
        exit 1
    fi
done

network_key_check_dir="$(mktemp -d)"
cleanup_network_key_check() {
    rm -rf -- "$network_key_check_dir"
}
trap cleanup_network_key_check EXIT
first_validator_config="$(awk '
    $1 == "binary:" { binary = $2; next }
    $1 == "config:" && binary == "validator" { print $2; exit }
' assets/config.yaml)"
if [ -z "$first_validator_config" ] || [ ! -f "assets/${first_validator_config}" ]; then
    echo "deployment does not contain a readable validator config" >&2
    exit 1
fi
cp assets/config.yaml "$network_key_check_dir/config.yaml"
cp "assets/${first_validator_config}" "$network_key_check_dir/${first_validator_config}"
cargo run --quiet --locked --bin deploy -- explorer \
    --dir "$network_key_check_dir" \
    --backend-url unused.invalid \
    remote >/dev/null
derived_network_identity="$(sed -n 's/^export const PUBLIC_KEY_HEX = "\(.*\)";$/\1/p' "$network_key_check_dir/config.ts")"
deployed_network_identity="$(sed -n 's/^identity: //p' assets/indexer.yaml)"
if [ -z "$derived_network_identity" ] || [ "$derived_network_identity" != "$deployed_network_identity" ]; then
    echo "indexer explorer identity does not match the validator network polynomial" >&2
    exit 1
fi
derived_certificate_mode="$(sed -n 's/^export const CERTIFICATE_MODE = "\(.*\)" as const;$/\1/p' "$network_key_check_dir/config.ts")"
deployed_certificate_mode="$(sed -n 's/^certificate_mode: //p' assets/indexer.yaml)"
if [ -z "$derived_certificate_mode" ] || [ "$derived_certificate_mode" != "$deployed_certificate_mode" ]; then
    echo "indexer explorer certificate mode does not match the validator leader mode" >&2
    exit 1
fi
cleanup_network_key_check
trap - EXIT

validator_config_count=0
while IFS= read -r validator_config; do
    validator_config_count=$((validator_config_count + 1))
    for expected_setting in \
        'worker_threads: 8' \
        'network_buffer_pool_max_per_class: 16384' \
        'traces_sample_rate: 0.0' \
        'signature_threads: 16'; do
        if ! grep -qx "$expected_setting" "assets/${validator_config}"; then
            echo "validator config is missing '${expected_setting}': assets/${validator_config}" >&2
            exit 1
        fi
    done
done < <(awk '
    $1 == "binary:" { binary = $2; next }
    $1 == "config:" && binary == "validator" { print $2 }
' assets/config.yaml)
if [ "$validator_config_count" -ne 50 ]; then
    echo "expected 50 validator configs, found ${validator_config_count}" >&2
    exit 1
fi

npm --prefix explorer ci
CI=true npm --prefix explorer test -- --watchAll=false --runInBand
env "${explorer_build_env[@]}" GENERATE_SOURCEMAP=false npm --prefix explorer run build
if [ ! -f explorer/build/index.html ]; then
    echo "explorer build did not create explorer/build/index.html" >&2
    exit 1
fi

just graviton-binaries
for artifact in validator indexer; do
    if [ ! -x "assets/$artifact" ]; then
        echo "build did not create executable assets/$artifact" >&2
        exit 1
    fi
    if ! file "assets/$artifact" | grep -q 'ARM aarch64'; then
        echo "build did not create an AArch64 assets/$artifact binary" >&2
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
