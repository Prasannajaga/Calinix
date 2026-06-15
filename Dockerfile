FROM rust:1.82-bookworm AS build

WORKDIR /app
COPY . .
RUN cargo build --release --bin cache-aware-routing

FROM debian:bookworm-slim

COPY --from=build /app/target/release/cache-aware-routing /usr/local/bin/cache-aware-routing
ENTRYPOINT ["/usr/local/bin/cache-aware-routing"]
