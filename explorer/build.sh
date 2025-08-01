#!/bin/bash
set -e

# Install Rust if not present
if ! command -v rustc &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

# Add wasm32 target
rustup target add wasm32-unknown-unknown

# Install wasm-pack
if ! command -v wasm-pack &> /dev/null; then
    curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
fi

# Create a clang symlink to gcc since blst specifically looks for clang
export PATH="/tmp/bin:$PATH"
mkdir -p /tmp/bin
ln -sf /usr/bin/gcc /tmp/bin/clang
ln -sf /usr/bin/gcc /tmp/bin/clang++

# Also set CC environment variables
export CC=gcc
export CXX=g++
export CC_wasm32_unknown_unknown=gcc
export CXX_wasm32_unknown_unknown=g++

# Run the build
npm run build