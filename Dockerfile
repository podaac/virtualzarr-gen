FROM python:3.12 AS base

WORKDIR /opt/cloud-optimized

# Install system dependencies (aws cli + jq are used by wrapper.sh for SSM)
RUN apt-get update && apt-get install -y jq awscli && rm -rf /var/lib/apt/lists/*

# uv: pinned binary from the official image
COPY --from=ghcr.io/astral-sh/uv:0.12.9 /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/opt/venv/bin:$PATH"

# Install third-party dependencies first (cached layer; only invalidated by lock changes)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Project code. icechunk_pipeline carries the append Lambda handler too.
COPY podaac ./podaac
COPY icechunk_pipeline ./icechunk_pipeline
RUN uv sync --frozen --no-dev

# --- ECS target (default) ---
FROM base AS ecs

COPY wrapper.sh ./
RUN chmod 755 wrapper.sh

ENTRYPOINT ["/opt/cloud-optimized/wrapper.sh"]

# --- Lambda target (append) ---
# NOTE: this is the LAST stage, so an unpinned `docker build` defaults to it.
# All builds must pin their stage: CI ECS image uses `--target ecs`
# (.github/workflows/docker-publish.yml), the append Lambda uses `--target lambda`
# (terraform/append_lambda.tf + terraform-deploy.yml). Do not rely on the default.
FROM base AS lambda

# awslambdaric comes from the `lambda` dependency group; boto3 (used by the
# handler for SSM) is a main dependency since it is not bundled in custom images.
RUN uv sync --frozen --no-dev --group lambda

ENTRYPOINT ["/opt/venv/bin/python", "-m", "awslambdaric"]
CMD ["icechunk_pipeline.append_lambda_handler.handler"]
