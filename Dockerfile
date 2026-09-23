FROM lukemathwalker/cargo-chef:latest-rust-1-bookworm AS chef
WORKDIR /app

# Install native C build dependencies required by rdkafka-sys
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Copy source code and build the final executable
COPY . .
RUN cargo build --release --bin l2-aggregator

FROM debian:bookworm-slim AS runtime
WORKDIR /app

# ca-certificates required for tokio-tungstenite to negotiate wss:// TLS connections
RUN apt-get update && apt-get install -y ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/l2-aggregator /usr/local/bin/
ENTRYPOINT ["l2-aggregator"]
