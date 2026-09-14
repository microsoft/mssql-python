#!/bin/sh
set -eu
cd /workspace
export DEBIAN_FRONTEND=noninteractive
export PIP_RETRIES=0
case "$1" in
  ubuntu)
    apt-get update
    apt-get install -y python3 python3-pip python3-venv python3-dev build-essential \
      cmake curl git gnupg unixodbc-dev libgssapi-krb5-2 libssl3
    ;;
  debian)
    apt-get update
    apt-get install -y build-essential cmake curl git gnupg unixodbc-dev \
      libgssapi-krb5-2 libssl3
    ;;
  alpine)
    apk add --no-cache build-base cmake clang git bash curl gnupg unixodbc-dev \
      libffi-dev openssl-dev zlib-dev py3-pip python3-dev patchelf krb5-libs libstdc++
    ;;
  *) exit 2 ;;
esac
python3 -m venv /opt/venv
. /opt/venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
bash eng/scripts/install-mssql-py-core.sh
cd mssql_python/pybind
bash build.sh
