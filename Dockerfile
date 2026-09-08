FROM ghcr.io/astral-sh/uv:0.8.20 AS uv

FROM python:3.12-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

COPY --from=uv /uv /uvx /bin/

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
