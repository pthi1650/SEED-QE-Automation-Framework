# Use the official Python image from the Docker Hub
FROM python:3.12.4-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    libpq-dev \
    build-essential \
    python3-dev \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    unzip \
    vim \
    sudo \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Install Vault
RUN wget https://releases.hashicorp.com/vault/1.10.3/vault_1.10.3_linux_amd64.zip \
    && unzip vault_1.10.3_linux_amd64.zip \
    && mv vault /usr/local/bin/ \
    && rm vault_1.10.3_linux_amd64.zip

# Copy the current directory contents into the container at /app
COPY . /app

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Install dependencies using Poetry
RUN poetry install --no-root

# Install AWS CLI
RUN pip install awscli

# Expose necessary ports
EXPOSE 8200
EXPOSE 7990

# Start an interactive shell by default
CMD ["/bin/bash"]
