# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.12-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

# Create a non-root user and group
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy the project files to the working directory
COPY . /app

# Change ownership of the working directory
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# Install dependencies
COPY pyproject.toml poetry.lock /app/
RUN pip install poetry && poetry install --no-root

# Set the entry point to keep the container running
CMD ["poetry", "run", "bash"]
