FROM ghcr.io/astral-sh/uv:0.8.20 AS uv

FROM python:3.12-slim

WORKDIR /opt/dbt

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    DBT_PROFILES_DIR=/opt/dbt \
    PATH="/opt/dbt/.venv/bin:$PATH"

COPY --from=uv /uv /uvx /bin/

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

COPY dbt_project.yml profiles.yml ./
COPY models ./models
COPY tests ./tests

ENTRYPOINT ["dbt"]
CMD ["--version"]
