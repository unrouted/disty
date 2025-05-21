FROM rust:1-bookworm AS build

WORKDIR /usr/src/disty

RUN apt-get update && apt-get install -y --no-install-recommends musl-tools ca-certificates

RUN rustup target add x86_64-unknown-linux-musl

RUN USER=root cargo init --bin /usr/src/disty
COPY Cargo.toml Cargo.lock .
RUN cargo build --target x86_64-unknown-linux-musl --release

# Copy the source and build the application.
COPY src src
RUN touch src/main.rs
RUN cargo build --locked --frozen --offline --target x86_64-unknown-linux-musl --release

# Copy the statically-linked binary into a scratch container.
FROM scratch
COPY --from=build /usr/src/disty/target/x86_64-unknown-linux-musl/release/disty .
USER 1000
ENTRYPOINT ["./disty"]

