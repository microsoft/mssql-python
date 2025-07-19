# Use Python 3.11 with Debian Bullseye as the base image
FROM mcr.microsoft.com/devcontainers/python:3.10-bullseye

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH=/workspaces/mssql-python
ENV CMAKE_BUILD_TYPE=Debug

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # Build tools
    build-essential \
    cmake \
    pkg-config \
    ninja-build \
    # Additional tools
    curl \
    wget \
    git \
    vim \
    nano \
    htop \
    tree \
    jq \
    # Clean up
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add mssql-tools to PATH
ENV PATH="$PATH:/opt/mssql-tools18/bin"

# Upgrade pip and install common Python development tools
RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install \
    black \
    flake8 \
    mypy \
    isort \
    pre-commit \
    twine \
    build \
    wheel \
    "pybind11[global]" \
    pytest-xdist \
    pytest-mock \
    "coverage[toml]"

# Set the default user to vscode (created by the base image)
USER vscode

# Set the working directory
WORKDIR /workspaces/mssql-python

# Add helpful aliases to .bashrc
RUN echo 'alias ll="ls -alF"' >> ~/.bashrc \
    && echo 'alias la="ls -A"' >> ~/.bashrc \
    && echo 'alias l="ls -CF"' >> ~/.bashrc 

# Default command
CMD ["/bin/bash"]
