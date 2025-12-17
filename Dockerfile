FROM python:3.12

WORKDIR /opt/cloud-optimized

# Install Poetry
RUN pip install poetry

# Copy only dependency files first for better caching
COPY pyproject.toml poetry.lock* ./
COPY podaac ./podaac
COPY wrapper.sh ./

COPY vds_basic_L2_dummytime_prod.ipynb ./

# Install dependencies and your project
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Register Jupyter kernel for papermill (ipykernel is installed via poetry above)
RUN python -m ipykernel install --name python3 --display-name "Python 3"

# Install system dependencies
RUN apt-get update && apt-get install -y jq awscli

RUN chmod 755 wrapper.sh

ENTRYPOINT ["/opt/cloud-optimized/wrapper.sh"]
