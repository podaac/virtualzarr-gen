FROM python:3.12

WORKDIR /opt/cloud-optimized

# Install Poetry
RUN pip install poetry

# Copy only dependency files first for better caching
COPY pyproject.toml poetry.lock* ./

# Install dependencies and your project
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Install system dependencies
RUN apt-get update && apt-get install -y jq awscli

# Copy the rest of your project files
COPY . .

RUN chmod 755 wrapper.sh

ENTRYPOINT ["/opt/cloud-optimized/wrapper.sh"]
