FROM rust:1.78-bookworm as builder
WORKDIR /src

RUN apt-get update && apt-get install -y protobuf-compiler git cmake make g++ gcc libclang-16-dev
RUN USER=root cargo init --name distribd /src
COPY Cargo.toml Cargo.lock /src/
RUN mkdir src/bin && mv src/main.rs src/bin/main.rs
RUN cargo build --release

COPY src /src/src
RUN touch src/bin/main.rs
RUN cargo build --release
RUN ls /src/target/

FROM rust:1-bookworm
STOPSIGNAL SIGINT
COPY --from=builder /src/target/release/distribd /distribd
CMD ["/distribd"]
