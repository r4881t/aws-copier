FROM python:3.12-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy project files
COPY pyproject.toml poetry.lock ./
COPY README.md ./
COPY src/ ./src/

# Configure Poetry to not create a virtual environment
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-dev --no-interaction

# Create a directory for the database
RUN mkdir -p /data

# Set environment variable for database location
ENV DB_NAME=/data/s3_migration_state.db

# Create an entrypoint script
RUN echo '#!/bin/sh\n\
exec python -m src.aws_copier "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (shows help)
CMD ["--help"]

# Volume for persistent database storage
VOLUME ["/data"]