# Build stage
FROM rust:1.93-trixie AS builder

WORKDIR /usr/src/app
COPY . .

RUN cargo build --release

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

# Install necessary libraries if any (e.g., openssl if needed, though not for basic OSM parsing)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/overpass-minimal /app/overpass-minimal

# Default entry point
ENTRYPOINT ["/app/overpass-minimal"]
