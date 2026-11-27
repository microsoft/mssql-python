#!/bin/sh
set -eu

source_root=$1
fixture=$2

# The mirror adds performance tests and their inputs after syncing the public tree.
if [ -f "$source_root/OneBranchPipelines/ado-only-paths.txt" ]; then
    cp -R "$source_root/performance" "$fixture/"
    cp "$source_root/.gitattributes" "$fixture/"
    cp "$source_root"/eng/requirements-perf.in "$source_root"/eng/requirements-perf.txt \
        "$source_root"/eng/requirements-perf-data.in "$source_root"/eng/requirements-perf-data.txt \
        "$fixture/eng/"
fi
