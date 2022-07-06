FROM rust:alpine3.16 as builder
WORKDIR /src

RUN apk --no-cache add musl-dev
RUN USER=root cargo init --name distribd /src
COPY Cargo.toml Cargo.lock /src/
RUN cargo build --target x86_64-unknown-linux-musl --release

COPY src /src/src
RUN cargo build --target x86_64-unknown-linux-musl --release

FROM scratch
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/distribd /app
CMD ["/app/bin/distribd"]
