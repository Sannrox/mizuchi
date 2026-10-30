# Flight coordinator/worker image (ADR 0011). Release builds only.
FROM rust:1.97.1 AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release --features flight --bin mizuchi

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/mizuchi /usr/local/bin/mizuchi
ENTRYPOINT ["mizuchi"]
