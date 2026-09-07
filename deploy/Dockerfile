# Use same Ubuntu version as `commonware-deployer`
FROM ubuntu:24.04

ARG TARGET_TRIPLE
ARG TARGET_CARGO_NAME
ARG TARGET_CC_NAME
ARG TARGET_CPU
ARG TARGET_LINKER
ARG TARGET_CXX
ARG TARGET_AR
ARG TARGET_STRIP

RUN apt-get update && apt-get install -y --no-install-recommends \
    binutils-aarch64-linux-gnu \
    binutils-x86-64-linux-gnu \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    g++-aarch64-linux-gnu \
    g++-x86-64-linux-gnu \
    gcc-aarch64-linux-gnu \
    gcc-x86-64-linux-gnu \
    git \
    libc6-dev-amd64-cross \
    libc6-dev-arm64-cross \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    --default-toolchain stable \
    --profile minimal

ENV PATH="/root/.cargo/bin:${PATH}" \
    TARGET_TRIPLE="${TARGET_TRIPLE}" \
    TARGET_CARGO_NAME="${TARGET_CARGO_NAME}" \
    TARGET_CC_NAME="${TARGET_CC_NAME}" \
    TARGET_CPU="${TARGET_CPU}" \
    TARGET_LINKER="${TARGET_LINKER}" \
    TARGET_CXX="${TARGET_CXX}" \
    TARGET_AR="${TARGET_AR}" \
    TARGET_STRIP="${TARGET_STRIP}" \
    PACKAGE_NAME="alto-validator" \
    BINARY_NAME="validator" \
    OUTPUT_NAME="validator" \
    CARGO_PROFILE_RELEASE_DEBUG=true

RUN rustup target add ${TARGET_TRIPLE}

WORKDIR /alto

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["set -euo pipefail && export CARGO_TARGET_${TARGET_CARGO_NAME}_LINKER=${TARGET_LINKER} && export CARGO_TARGET_${TARGET_CARGO_NAME}_RUSTFLAGS=\"-C target-cpu=${TARGET_CPU}\" && export CC_${TARGET_CC_NAME}=${TARGET_LINKER} && export CXX_${TARGET_CC_NAME}=${TARGET_CXX} && export AR_${TARGET_CC_NAME}=${TARGET_AR} && mkdir -p assets && cargo build --locked --release --package ${PACKAGE_NAME} --bin ${BINARY_NAME} --target ${TARGET_TRIPLE} && cp target/${TARGET_TRIPLE}/release/${BINARY_NAME} assets/${OUTPUT_NAME}-debug && ${TARGET_STRIP} -o assets/${OUTPUT_NAME} assets/${OUTPUT_NAME}-debug"]
