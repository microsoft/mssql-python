# Use Python 3.14 with Debian Bookworm as the base image
FROM mcr.microsoft.com/devcontainers/python:3.14-bookworm

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH=/workspaces/mssql-python
ENV CMAKE_BUILD_TYPE=Debug

# Set timezone (configurable via build arg)
ARG TZ=UTC
ENV TZ=$TZ
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone

# Install all system dependencies in one layer
RUN apt-get update && apt-get install -y \
    # Build tools
    build-essential \
    cmake \
    pkg-config \
    ninja-build \
    python3-dev \
    pybind11-dev \
    # ODBC prerequisites (unixodbc-dev provides sql.h headers for compilation)
    unixodbc-dev \
    gnupg \
    software-properties-common \
    # Utilities
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
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=arm64,amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add mssql-tools to PATH
ENV PATH="$PATH:/opt/mssql-tools18/bin"

# Set the default user to vscode (created by the base image)
USER vscode

# Set the working directory
WORKDIR /workspaces/mssql-python

# Default command
CMD ["/bin/bash"]
