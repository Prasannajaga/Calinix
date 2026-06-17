# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm

WORKDIR /app

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    CARGO_HTTP_TIMEOUT=600 \
    CARGO_NET_RETRY=10

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --locked --release --bin cache-aware-routing && \
    cp /app/target/release/cache-aware-routing /usr/local/bin/cache-aware-routing

ENTRYPOINT ["/usr/local/bin/cache-aware-routing"]
