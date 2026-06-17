FROM rust:1-bookworm

WORKDIR /app
COPY . .
RUN cargo build --release --bin cache-aware-routing

ENTRYPOINT ["/app/target/release/cache-aware-routing"]
