"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Regression test for issue #594:
  Passing SQL_COPT_SS_ACCESS_TOKEN (1256) via attrs_before triggered a
  use-after-free during SQLDriverConnect. PR #568 copied the token
  bytes into a stack-local std::string in Connection::setAttribute that
  was freed when setAttribute returned, but SQL_COPT_SS_ACCESS_TOKEN is
  a *deferred* ODBC attribute: the driver stashes the caller's pointer
  at SQLSetConnectAttr time and only dereferences it later, when
  SQLDriverConnect builds the FedAuth Login7 packet.

  Observed symptoms in 1.7.1:
    * macOS arm64:  Fatal Python error: Bus error (SIGBUS)
    * Windows x64:  "Authentication token is missing in the federated
                     authentication message"
    * Azure SQL DB: TCP Provider error 0x2746 (server-side reset)

Strategy
--------
A subprocess driver (tests/tools/_issue_594_helper.py) brings up the
local mock TDS server, performs a few FedAuth connects with a known
sentinel token, and asserts that the bytes the server actually
received in the Login7 FedAuth feature extension match the bytes that
were passed in. Running it as a subprocess means a SIGBUS in a buggy
build surfaces as a nonzero exit code (rc=138 / -10 on macOS) instead
of taking down the pytest worker; on platforms where the symptom is
silent corruption (Linux) or a driver-side error (Windows), the
token-integrity check in the helper still catches it.

Against stock 1.7.1 on macOS arm64 a single iteration deterministically
SIGBUSes. A handful of iterations is used as defense in depth for other
platforms and future allocator variance.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

TOOLS_DIR = Path(__file__).parent / "tools"
sys.path.insert(0, str(TOOLS_DIR))

from mock_tds_server import generate_self_signed_cert  # noqa: E402


@pytest.fixture(scope="module")
def cert_pair(tmp_path_factory):
    d = tmp_path_factory.mktemp("mocktds-cert")
    cert = d / "server.pem"
    key = d / "server.key"
    generate_self_signed_cert(str(cert), str(key))
    return str(cert), str(key)


def test_access_token_round_trips_intact(cert_pair):
    """A FedAuth connect via SQL_COPT_SS_ACCESS_TOKEN (1256) must deliver
    the token bytes unchanged to the server. Pre-fix the stack-local
    buffer was freed before SQLDriverConnect read it, causing SIGBUS on
    macOS, "Authentication token is missing" on Windows, or a TCP
    reset (0x2746) against Azure SQL DB."""
    cert, key = cert_pair
    helper = TOOLS_DIR / "_issue_594_helper.py"
    iters = "3"

    started = time.monotonic()
    p = subprocess.run(
        [sys.executable, str(helper), cert, key, iters],
        capture_output=True,
        text=True,
        timeout=120,
    )
    elapsed = time.monotonic() - started

    # rc 138 (macOS) / -10 (signal) / -7 (SIGBUS on linux) all mean the
    # helper crashed in native code — the classic #594 signature.
    assert p.returncode == 0, (
        f"helper exited with rc={p.returncode} after {elapsed:.1f}s. "
        f"A negative rc or rc==138 indicates a native crash (SIGBUS / "
        f"use-after-free, issue #594). rc=4 indicates the token was "
        f"corrupted in flight; rc=5 indicates the driver aborted before "
        f"Login7.\nstdout={p.stdout!r}\nstderr={p.stderr[-600:]!r}"
    )
    assert p.stdout.startswith("OK "), (
        f"unexpected helper output: stdout={p.stdout!r} stderr={p.stderr!r}"
    )
