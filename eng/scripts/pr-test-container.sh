#!/usr/bin/env bash
# Manage only the Linux test container belonging to this hosted validation job.
set -euo pipefail
action=$1
name=$2
label="com.microsoft.mssql-python.ci-owner=${BUILD_BUILDID:?}.${SYSTEM_JOBID:?}"
id=$(docker ps -aq --no-trunc --filter "name=^/${name}$" --filter "label=$label")

case "$action" in
  reset)
    if [[ -n "$id" ]]; then
      # docker run can leave a partially started container with the name reserved.
      docker rm -f "$id"
    fi
    ;;
  cleanup)
    if [[ -n "$id" ]]; then
      docker stop "$id" || true
      docker rm "$id" || true
    fi
    ;;
  *) exit 2 ;;
esac
