# Stage 1: Build the binary
FROM rust:latest AS builder

# Install dependencies needed for linking (like openssl) and building (protoc)
# Using bookworm-slim as a base, adjust if needed for specific dependencies
RUN apt-get update && apt-get install -y --no-install-recommends libssl-dev pkg-config protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/alto

# Copy workspace dependencies first to leverage Docker cache
COPY ../Cargo.toml ../Cargo.lock /usr/src/alto/
# Copy project-specific dependencies' manifests
COPY ./chain/Cargo.toml ./chain/
COPY ../types/Cargo.toml ./types/
COPY ../client/Cargo.toml ./client/
COPY ../inspector/Cargo.toml ./inspector/

# Copy all source code
COPY ./chain/src ./chain/src
COPY ../types/src ./types/src
COPY ../client/src ./client/src
COPY ../inspector/src ./inspector/src

# Build the validator binary (no separate dependency caching step)
RUN cargo build --release --bin validator

# Stage 2: Create the runtime image
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies (like OpenSSL)
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/alto/target/release/validator /usr/local/bin/validator

# Set the entrypoint to run the validator binary
# Add any necessary runtime arguments here if needed
ENTRYPOINT ["/usr/local/bin/validator"]

# Optionally expose ports if the validator listens on any
# EXPOSE <port_number>