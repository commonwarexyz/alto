#!/bin/bash
set -e

# Debug: Check what's available in the environment
echo "=== Build Environment Info ==="
echo "PATH: $PATH"
echo "Available commands:"
which gcc || echo "gcc not found"
which clang || echo "clang not found"
which rustc || echo "rustc not found"
echo "==========================="

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

# Try to use emscripten for wasm builds if available
if command -v emcc &> /dev/null; then
    export CC_wasm32_unknown_unknown=emcc
elif command -v gcc &> /dev/null; then
    export CC_wasm32_unknown_unknown=gcc
fi

# Run the build
npm run build