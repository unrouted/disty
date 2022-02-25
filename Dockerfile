FROM rust:alpine3.15 as builder
ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN apk add --no-cache python3-dev gcc libffi-dev musl-dev openssl-dev make g++ curl
RUN python3 -m venv /app && /app/bin/python -m pip install -U pip setuptools wheel

WORKDIR /src

RUN USER=root cargo init --lib --name distribd /src
COPY Cargo.toml Cargo.lock /src/
RUN RUSTFLAGS="-C target-feature=-crt-static" cargo build

COPY requirements.txt /src/requirements.txt
RUN /app/bin/python -m pip install -r requirements.txt

COPY . /src/
RUN /app/bin/python -m pip install /src

FROM alpine:3.15
ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN apk add --no-cache libffi libstdc++ openssl python3
COPY --from=builder /app /app

COPY Rocket.toml /app/Rocket.toml

CMD ["/app/bin/python", "-m", "distribd", "8080"]
