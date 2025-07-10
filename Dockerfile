FROM registry.access.redhat.com/ubi9/python-312:latest

# Use a writable location (UBI base has access to /opt/app-root)
ENV VENV_PATH=/opt/app-root/venv

# Create virtual environment
RUN python3 -m venv $VENV_PATH

# Activate virtual environment on container start
ENV PATH="$VENV_PATH/bin:$PATH"

CMD ["bash"]