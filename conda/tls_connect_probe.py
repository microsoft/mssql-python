"""Live ``Encrypt=yes`` TLS gate: prove the driver's OpenSSL backend is REACHABLE.

Why this exists (and why ``driver_load_probe.py`` is not enough): the Linux
``libmsodbcsql`` links ``libkrb5``/``libgssapi_krb5`` at load time but resolves
its OpenSSL backend (``libssl``/``libcrypto``) by **dlopen at TLS time** -- there
is no ``libssl``/``libcrypto`` ``DT_NEEDED`` or soname string in the binary, so
the crypto libraries are only touched when an actual encrypted handshake runs.
An ``Encrypt=no`` connect (what ``driver_load_probe.py`` does) NEVER exercises
that path, so it cannot reveal an unreachable OpenSSL -- e.g. a conda env where
the declared ``openssl`` lives in ``<PREFIX>/lib`` that the vendored driver's
RUNPATH does not reach. Only a real ``Encrypt=yes`` handshake forces the dlopen.

FAIL-CLOSED contract:

* ``Encrypt`` is forced to ``yes`` (mandatory encryption), so the pre-login TLS
  handshake MUST complete before any LOGIN7 packet is sent. Therefore ANY outcome
  that reaches the authentication / database stage -- a clean connect, a
  ``Login failed for user`` (18456), or a ``Cannot open database`` -- is POSITIVE
  proof that OpenSSL loaded, negotiated, and established the encrypted channel.
  These are the only PASS outcomes (see ``_TLS_COMPLETED_MARKERS``).
* Every other outcome fails closed (non-zero exit). In particular an OpenSSL that
  could not be loaded surfaces BEFORE login as an ``SSL Provider`` /
  ``libssl``/``libcrypto`` / ``cannot open shared object`` error -- classified
  here as ``OPENSSL BACKEND UNREACHABLE`` (see ``_OPENSSL_UNREACHABLE_MARKERS``),
  which is exactly the conda RUNPATH bug this gate is meant to catch.

IMPORTANT -- masking caveat: this gate is only CONCLUSIVE on a minimal base with
NO system OpenSSL on the default loader path. On a full agent (or any host with a
system ``libssl``) the driver's dlopen can fall through to the system copy and the
handshake succeeds even when the conda ``<PREFIX>/lib`` copy is unreachable --
masking the very bug, just like the hosted CI agents do today. Run it in a
minimal container (no system OpenSSL) against a reachable server to make it
meaningful. The masking-IMMUNE static guard is
``eng/scripts/audit_bundled_binaries.py`` (it reads the RUNPATH bytes and requires
an ``$ORIGIN/..`` climb regardless of what system libs exist); this live gate is
the complementary end-to-end backstop.

Config: set ``CONDA_TLS_PROBE_CONN`` to a reachable SQL Server connection string
(creds may be wrong -- reaching ``Login failed`` still proves TLS). If it is not
set the gate SKIPS loudly (exit 0) -- it never silently passes.

Exit code 0 = TLS handshake completed (OpenSSL reachable) OR skipped; non-zero =
OpenSSL backend unreachable / handshake did not complete (blocks publish).
"""

import os
import re
import sys

# Outcomes that can ONLY occur AFTER a mandatory (Encrypt=yes) TLS handshake has
# completed -- i.e. positive proof the dlopen'd OpenSSL backend loaded and
# negotiated the encrypted channel. Matched case-insensitively.
_TLS_COMPLETED_MARKERS = (
    "login failed for user",  # LOGIN7 rejected -> handshake already done
    "18456",  # SQL Server login-failed error number
    "cannot open database",  # authenticated, database validation stage
    "changed database context",  # connected successfully
    "password did not match",
)

# Markers that mean the crypto backend could NOT be loaded / the handshake never
# ran. Listed for a crisp FAIL message -- classification is allowlist-based, so an
# unrecognized outcome fails closed even if it matches nothing here.
_OPENSSL_UNREACHABLE_MARKERS = (
    "libssl",
    "libcrypto",
    "cannot open shared object",  # linux dlopen failure of the crypto backend
    "image not found",  # macOS dlopen failure
    "openssl",
    "ssl provider",  # an SSL Provider error before login = crypto/handshake fail
    "ssl routines",
    "encryption not supported",
    "unable to load",
    "cannot load",
)


def tls_completed(exc):
    """FAIL-CLOSED classifier: True only when the TLS handshake provably completed.

    ``exc is None`` (clean connect) or a post-handshake authentication/database
    diagnostic returns True; every other outcome -- including an OpenSSL-load
    failure or anything unrecognized -- returns False so the gate exits non-zero.
    """
    if exc is None:
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in _TLS_COMPLETED_MARKERS)


def describe(exc):
    """Short, human-readable reason string for the gate's stdout / exit line."""
    if exc is None:
        return "clean connect (TLS handshake completed)"
    msg = str(exc)
    low = msg.lower()
    for marker in _OPENSSL_UNREACHABLE_MARKERS:
        if marker in low:
            return "OpenSSL backend unreachable -> " + msg[:300]
    return msg[:300]


def force_tls(conn):
    """Force ``Encrypt=yes`` and ``TrustServerCertificate=yes`` on the string.

    Encrypt=yes makes the pre-login TLS handshake mandatory (the whole point of
    the gate). TrustServerCertificate=yes lets it reach the auth stage against a
    local dev server's self-signed cert -- this is a local connectivity gate, NOT
    a security assertion, and must never be copied into a production connection.
    """

    def set_kv(s, key, val):
        pat = re.compile(r"(?i)(^|;)\s*" + re.escape(key) + r"\s*=\s*[^;]*")
        if pat.search(s):
            return pat.sub(lambda m: (m.group(1) or "") + key + "=" + val, s, count=1)
        return s + ";" + key + "=" + val

    conn = conn.strip().rstrip(";")
    conn = set_kv(conn, "Encrypt", "yes")
    conn = set_kv(conn, "TrustServerCertificate", "yes")
    return conn


def main():
    raw = os.environ.get("CONDA_TLS_PROBE_CONN", "").strip()
    if not raw:
        print(
            "TLS_PROBE_SKIPPED: set CONDA_TLS_PROBE_CONN to a reachable SQL Server "
            "connection string (on a minimal base with no system OpenSSL) to run "
            "this Encrypt=yes gate."
        )
        return

    conn_str = force_tls(raw)

    # Deferred so this module can be imported (and the classifier unit-tested)
    # WITHOUT the compiled extension / driver payload.
    import mssql_python

    outcome = None
    try:
        conn = mssql_python.connect(conn_str)
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup only
            pass
    except Exception as exc:  # noqa: BLE001 - deliberately classified below
        outcome = exc

    if tls_completed(outcome):
        print("TLS_OK (OpenSSL backend reachable; " + describe(outcome) + ")")
        return
    sys.exit("TLS/OPENSSL BACKEND UNREACHABLE: " + describe(outcome))


if __name__ == "__main__":
    main()
