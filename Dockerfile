FROM rust:alpine3.15 as base
ENV PYTHONUNBUFFERED=1
WORKDIR /app

FROM base as builder

RUN apk add --no-cache python3-dev gcc libffi-dev musl-dev openssl-dev make g++ curl
RUN python3 -m venv /app && /app/bin/python -m pip install -U pip setuptools wheel

COPY . /src
RUN /app/bin/python -m pip install /src

FROM base as final

RUN apk add --no-cache libffi libstdc++ openssl python3
COPY --from=builder /app /app

CMD ["/app/bin/python", "-m", "distribd", "8080"]
