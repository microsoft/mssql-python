"""
Subprocess helper for tests/test_021_issue_594_access_token_uaf.py.

Runs N FedAuth connects against the local mock TDS server using
SQL_COPT_SS_ACCESS_TOKEN (1256) and verifies that the access token
bytes arrive at the server byte-identical to what we passed in.

Run in a subprocess so a SIGBUS on a buggy build (issue #594) surfaces
as a nonzero exit code (rc=138 / -10 on macOS) instead of taking down
the pytest worker.

Usage:
    python _issue_594_helper.py <cert_path> <key_path> [iters]

Exit codes:
    0  - all iterations succeeded and the token round-tripped intact
    4  - token mismatch (UAF corrupted the token bytes)
    5  - mock server never received a token (connect aborted before Login7)
    other nonzero / signal - crash (SIGBUS, etc.) on a buggy build

Output: one line "OK <iters>" on success, otherwise a diagnostic line.
"""
from __future__ import annotations

import struct
import sys
import time
from itertools import chain, repeat
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from mock_tds_server import MockTdsServer  # noqa: E402

import mssql_python  # noqa: E402

# Pooling would cache the underlying ODBC connection and skip Login7 on
# subsequent connects, never re-exercising the access-token handoff.
mssql_python.pooling(enabled=False)

SQL_COPT_SS_ACCESS_TOKEN = 1256


class _CapturingMockTdsServer(MockTdsServer):
    """Records every access token seen during Login7."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.received_tokens = []  # list[str]

    def resolve_token_username(self, token):
        self.received_tokens.append(token)
        return super().resolve_token_username(token)


def _pack_access_token(token_str: str) -> bytes:
    token_bytes = bytes(token_str, "utf-8")
    encoded = bytes(chain.from_iterable(zip(token_bytes, repeat(0))))
    return struct.pack("<i", len(encoded)) + encoded


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: _issue_594_helper.py CERT KEY [ITERS]", file=sys.stderr)
        return 2

    cert = sys.argv[1]
    key = sys.argv[2]
    iters = int(sys.argv[3]) if len(sys.argv) > 3 else 3

    srv = _CapturingMockTdsServer(host="127.0.0.1", port=0, cert_file=cert, key_file=key)
    srv.start_background()
    deadline = time.monotonic() + 5.0
    while srv.port == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    if srv.port == 0:
        print("mock server failed to bind", file=sys.stderr)
        return 3

    # ~1500-char sentinel matches the size class of a real Azure AD
    # bearer token, which is what triggered the original UAF.
    sentinel = "MSSQL-PYTHON-ISSUE-594-SENTINEL-" + ("A" * 1500)
    attrs = {SQL_COPT_SS_ACCESS_TOKEN: _pack_access_token(sentinel)}
    cs = (
        f"Server=127.0.0.1,{srv.port};Database=mockdb;"
        "Encrypt=Yes;TrustServerCertificate=Yes"
    )

    try:
        for _ in range(iters):
            c = mssql_python.connect(cs, attrs_before=attrs, autocommit=True, timeout=10)
            cur = c.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            c.close()
    finally:
        srv.stop()

    if not srv.received_tokens:
        print("FAIL: mock server never received a FedAuth token", file=sys.stderr)
        return 5

    for i, recv in enumerate(srv.received_tokens):
        if recv != sentinel:
            div = next(
                (j for j in range(min(len(recv), len(sentinel))) if recv[j] != sentinel[j]),
                min(len(recv), len(sentinel)),
            )
            print(
                f"FAIL: corrupted access token on connect #{i+1}: "
                f"len_sent={len(sentinel)} len_recv={len(recv)} "
                f"first_diff_at_idx={div} recv_head={recv[:64]!r}",
                file=sys.stderr,
            )
            return 4

    print(f"OK {iters}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
