FROM alpine:3.11 as base
ENV PYTHONUNBUFFERED=1
WORKDIR /app


FROM base as builder

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apk add --no-cache python3-dev gcc libffi-dev musl-dev openssl-dev make g++
RUN python3 -m pip install poetry

RUN python3 -m venv /app


COPY pyproject.toml poetry.lock /src/
RUN ln -s /app /src/.venv
RUN cd /src && poetry install --no-dev

COPY . /src
RUN cd /src && poetry build && /app/bin/pip install dist/*.whl

FROM base as final

RUN apk add --no-cache libffi openssl python3
COPY --from=builder /app /app

CMD ["/app/bin/python", "-m", "distribd", "8080"]
