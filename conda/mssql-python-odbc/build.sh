#!/bin/bash
# Repackage the prebuilt, ESRP-signed wheel into a conda package (offline).
# PKG_NAME / PKG_VERSION are exported by conda-build; WHEELS_DIR by the pipeline/harness.
set -euo pipefail
# Cross-arch (emulated) build: when repackaging the aarch64 wheel on an x86_64 host,
# $PYTHON is the target-arch interpreter and runs under qemu-user. Point qemu at the
# aarch64 glibc loader/libs (installed via libc6-arm64-cross) so it can find
# /lib/ld-linux-aarch64.so.1 instead of aborting with "Could not open". The dir only
# exists on the emulated aarch64 leg; setting the var elsewhere is a harmless no-op.
[ -d /usr/aarch64-linux-gnu ] && export QEMU_LD_PREFIX="${QEMU_LD_PREFIX:-/usr/aarch64-linux-gnu}"
$PYTHON -m pip install --no-deps --no-index --find-links "$WHEELS_DIR" "$PKG_NAME==$PKG_VERSION" -vv
