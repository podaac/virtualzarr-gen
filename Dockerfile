FROM python:3.12 AS base

WORKDIR /opt/cloud-optimized

# Install system dependencies (aws cli + jq are used by wrapper.sh for SSM)
RUN apt-get update && apt-get install -y jq awscli && rm -rf /var/lib/apt/lists/*

# Poetry installs into a single venv at /opt/venv (no Poetry-managed venvs)
RUN pip install --no-cache-dir "poetry==2.3.2"
ENV POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"
RUN python -m venv /opt/venv

# Install third-party dependencies first (cached layer; only invalidated by lock changes)
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --without dev --no-root

# Project code. icechunk_pipeline carries the append Lambda handler too.
COPY podaac ./podaac
COPY icechunk_pipeline ./icechunk_pipeline
RUN poetry install --no-interaction --no-ansi --without dev

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

# awslambdaric comes from the optional `lambda` group; boto3 (used by the
# handler for SSM) is a main dependency since it is not bundled in custom images.
RUN poetry install --no-interaction --no-ansi --without dev --with lambda

ENTRYPOINT ["/opt/venv/bin/python", "-m", "awslambdaric"]
CMD ["icechunk_pipeline.append_lambda_handler.handler"]
