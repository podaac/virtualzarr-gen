FROM python:3.12 AS base

WORKDIR /opt/cloud-optimized

# Install system dependencies
RUN apt-get update && apt-get install -y jq awscli

# Install Poetry
RUN pip install poetry

# Kerchunk environment (default)
COPY pyproject.toml poetry.lock* ./
COPY podaac ./podaac
# icechunk_pipeline is a declared package in pyproject; copy it before any
# install so both the kerchunk `poetry install` and the icechunk editable
# `pip install -e .` can find it. It carries the append Lambda handler too.
COPY icechunk_pipeline ./icechunk_pipeline
RUN python -m venv /opt/venv-kerchunk && \
    . /opt/venv-kerchunk/bin/activate && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

# Register Jupyter kernel for papermill
RUN /opt/venv-kerchunk/bin/python -m ipykernel install --name python3 --display-name "Python 3"

# Icechunk environment
COPY requirements_icechunk.txt ./
RUN python -m venv /opt/venv-icechunk && \
    /opt/venv-icechunk/bin/pip install --no-cache-dir -r requirements_icechunk.txt && \
    /opt/venv-icechunk/bin/pip install --no-deps -e .

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

# boto3 is used by the handler (SSM); it is NOT bundled in this custom image the
# way it is in AWS-managed Lambda base images, so install it explicitly.
RUN /opt/venv-icechunk/bin/pip install --no-cache-dir awslambdaric boto3

# The handler and everything it imports (icechunk_append, source_url_coord,
# collection_config) ship in the icechunk_pipeline / podaac packages copied and
# editable-installed in the base stage -- nothing extra to copy here.

ENTRYPOINT ["/opt/venv-icechunk/bin/python", "-m", "awslambdaric"]
CMD ["icechunk_pipeline.append_lambda_handler.handler"]
