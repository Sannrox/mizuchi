FROM rust:1.97.1
WORKDIR /src
COPY . .
RUN cargo build --locked --features flight --bin mizuchi
ENV PATH="/src/target/debug:${PATH}"
ENTRYPOINT ["mizuchi"]
