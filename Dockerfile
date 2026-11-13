# Multi-stage build: Poetry and build tooling stay in the builder; the shipped
# images are python:3.12-slim + the prebuilt venv only (smaller CVE surface).

# --- Builder: resolve and install dependencies into /opt/venv ---
FROM python:3.12 AS builder

WORKDIR /opt/cloud-optimized

RUN pip install --no-cache-dir "poetry==2.3.3"
ENV POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"
RUN python -m venv /opt/venv

# Third-party dependencies first (cached layer; only invalidated by lock changes)
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --without dev --no-root

# Project code. icechunk_pipeline carries the append Lambda handler too.
COPY podaac ./podaac
COPY icechunk_pipeline ./icechunk_pipeline
RUN poetry install --no-interaction --no-ansi --without dev

# Separate venv for the Lambda image, adding the optional `lambda` group.
# Built fresh (not copied from /opt/venv): a copied venv's install records hold
# absolute /opt/venv paths, and reinstalling over them deletes the ECS venv's
# console scripts.
RUN python -m venv /opt/venv-lambda && \
    VIRTUAL_ENV=/opt/venv-lambda PATH="/opt/venv-lambda/bin:$PATH" \
    poetry install --no-interaction --no-ansi --without dev --with lambda

# --- Runtime base: slim, patched, no build tools ---
FROM python:3.12-slim AS runtime

WORKDIR /opt/cloud-optimized

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/venv/bin:$PATH"

# --- ECS target (default) ---
FROM runtime AS ecs

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/cloud-optimized/podaac ./podaac
COPY --from=builder /opt/cloud-optimized/icechunk_pipeline ./icechunk_pipeline
COPY wrapper.sh ./
RUN chmod 755 wrapper.sh

ENTRYPOINT ["/opt/cloud-optimized/wrapper.sh"]

# --- Lambda target (append) ---
# NOTE: this is the LAST stage, so an unpinned `docker build` defaults to it.
# All builds must pin their stage: CI ECS image uses `--target ecs`
# (.github/workflows/docker-publish.yml), the append Lambda uses `--target lambda`
# (terraform/append_lambda.tf + terraform-deploy.yml). Do not rely on the default.
FROM runtime AS lambda

# awslambdaric comes from the optional `lambda` group; boto3 (used by the
# handler for SSM) is a main dependency since it is not bundled in custom images.
COPY --from=builder /opt/venv-lambda /opt/venv
COPY --from=builder /opt/cloud-optimized/podaac ./podaac
COPY --from=builder /opt/cloud-optimized/icechunk_pipeline ./icechunk_pipeline

ENTRYPOINT ["/opt/venv/bin/python", "-m", "awslambdaric"]
CMD ["icechunk_pipeline.append_lambda_handler.handler"]
