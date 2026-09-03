FROM python:3.12

WORKDIR /opt/cloud-optimized

# Install system dependencies
RUN apt-get update && apt-get install -y jq awscli

# Install Poetry
RUN pip install poetry

# Kerchunk environment (default)
COPY pyproject.toml poetry.lock* ./
COPY podaac ./podaac
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

COPY wrapper.sh ./
COPY vds_basic_L2_dummytime_prod.ipynb ./

RUN chmod 755 wrapper.sh

ENTRYPOINT ["/opt/cloud-optimized/wrapper.sh"]
