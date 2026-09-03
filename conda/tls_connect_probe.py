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
set the gate SKIPS loudly (exit 0) -- it never silently passes. Set
``CONDA_TLS_PROBE_REQUIRED=1`` on the minimal-base leg to make the gate MANDATORY:
a missing OR malformed connection string then FAILS (exit non-zero) instead of
skipping, so a typo (a bare ``yes``) can never silently no-op it.

Exit code 0 = TLS handshake completed (OpenSSL reachable) OR skipped; non-zero =
OpenSSL backend unreachable / handshake did not complete (blocks publish).
"""

import os
import sys

# Outcomes that can ONLY occur AFTER a mandatory (Encrypt=yes) TLS handshake has
# completed -- i.e. positive proof the dlopen'd OpenSSL backend loaded and
# negotiated the encrypted channel. Matched case-insensitively.
_TLS_COMPLETED_MARKERS = (
    "login failed for user",  # LOGIN7 rejected -> handshake already done
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
    if any(marker in msg for marker in _TLS_COMPLETED_MARKERS):
        return True
    # SQL Server login-failed error 18456 is post-handshake proof, but the BARE number can
    # appear in an unrelated pre-TLS error (a port, an IP octet, an id). Only accept it when
    # it co-occurs with a login context (SQLSTATE 28000 or the word 'login'), so a network/
    # timeout error that merely contains '18456' cannot pass as TLS-completed.
    if "18456" in msg and ("login" in msg or "28000" in msg):
        return True
    return False


def describe(exc):
    """Short, human-readable reason string for the gate's stdout / exit line."""
    if exc is None:
        return "clean connect (TLS handshake completed)"
    msg = str(exc)
    low = msg.lower()
    for marker in _OPENSSL_UNREACHABLE_MARKERS:
        if marker in low:
            return "TLS handshake did not complete -> " + msg[:300]
    return msg[:300]


def _split_top_level(conn):
    """Split an ODBC connection string on TOP-LEVEL ``;`` only.

    An ODBC value wrapped in ``{...}`` may itself contain ``;`` (MS-ODBCSTR), so a
    naive ``split(';')`` would shred braced values. Track brace depth and break only
    at depth 0. Inside a braced value ``}}`` is an escaped literal ``}`` (MS-ODBCSTR),
    NOT a close -- consume both and keep the depth so the value is not split early.
    """
    segments = []
    buf = ""
    depth = 0
    s = conn.strip()
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "{":
            depth += 1
            buf += ch
        elif ch == "}":
            if depth > 0 and i + 1 < len(s) and s[i + 1] == "}":
                buf += "}}"  # escaped literal '}' inside a braced value; not a close
                i += 2
                continue
            depth = max(0, depth - 1)
            buf += ch
        elif ch == ";" and depth == 0:
            segments.append(buf)
            buf = ""
        else:
            buf += ch
        i += 1
    segments.append(buf)
    return segments


def force_tls(conn):
    """Force ``Encrypt=yes`` and ``TrustServerCertificate=yes`` on the string.

    Encrypt=yes makes the pre-login TLS handshake mandatory (the whole point of
    the gate). TrustServerCertificate=yes lets it reach the auth stage against a
    local dev server's self-signed cert -- this is a local connectivity gate, NOT
    a security assertion, and must never be copied into a production connection.

    Rebuild-from-tokens (NOT regex substitution): brace-aware split on top-level
    ``;``, DROP any existing Encrypt / TrustServerCertificate segment (case-
    insensitive -- including a valueless ``Encrypt`` or a duplicate), then append the
    canonical pair exactly once. A regex substitution can corrupt the string -- e.g.
    ``Encrypt=;yes`` becomes ``Encrypt=yes;yes``, leaving a bare ``yes`` the parser
    rejects with "keyword 'yes' has no value", and a duplicate ``Encrypt`` slips
    through as a "Duplicate keyword" error; rebuilding from tokens cannot.
    """
    kept = []
    for seg in _split_top_level(conn):
        token = seg.strip()
        if not token:
            continue
        key = token.split("=", 1)[0].strip().lower()
        if key in ("encrypt", "trustservercertificate"):
            continue  # drop any existing (incl. valueless / duplicate); re-added below
        kept.append(token)
    kept.append("Encrypt=yes")
    kept.append("TrustServerCertificate=yes")
    return ";".join(kept)


def _redact(conn):
    """Render the connection string's STRUCTURE with every value masked.

    Safe to log: shows the keys (and their order) so a malformed string is
    diagnosable, but never a secret value. A segment with no ``=`` -- the exact shape
    that trips the parser -- is surfaced verbatim so the failure explains itself.
    """
    shown = []
    for seg in _split_top_level(conn):
        token = seg.strip()
        if not token:
            continue
        if "=" in token:
            shown.append(token.split("=", 1)[0].strip() + "=***")
        else:
            shown.append("<<NO-VALUE:" + token + ">>")
    return ";".join(shown)


def _is_probe_connection_string(raw):
    """True if ``raw`` looks like an ODBC connection string (has a key=value pair).

    Guards the common misconfiguration of treating CONDA_TLS_PROBE_CONN as a yes/no
    toggle: a bare ``yes``/``true``/``1`` has no ``=``, so it cannot be a connection
    string and must not be handed to the parser (which would fail on a bare keyword).
    """
    return "=" in raw


# Recognized boolean spellings for CONDA_TLS_PROBE_REQUIRED (kept in sync with the shell
# truthiness in build-conda-packages.sh so the two never disagree).
_REQUIRED_TRUTHY = ("1", "true", "yes", "on")
_REQUIRED_FALSY = ("", "0", "false", "no", "off")


def _required():
    """True when the Encrypt=yes gate is MANDATORY on this leg.

    Set ``CONDA_TLS_PROBE_REQUIRED`` to a truthy value (1/true/yes/on) on the minimal-base
    leg that MUST exercise the dlopen'd OpenSSL backend. In required mode a MISSING or
    MALFORMED ``CONDA_TLS_PROBE_CONN`` FAILS the leg instead of skipping -- so a typo or an
    unset secret can never silently no-op the one gate that actually tests OpenSSL
    reachability. An UNRECOGNIZED value (e.g. the typo 'tru') FAILS LOUD rather than
    silently disabling the gate.
    """
    raw = os.environ.get("CONDA_TLS_PROBE_REQUIRED", "").strip().lower()
    if raw in _REQUIRED_TRUTHY:
        return True
    if raw in _REQUIRED_FALSY:
        return False
    sys.exit(
        f"CONDA_TLS_PROBE_REQUIRED={os.environ.get('CONDA_TLS_PROBE_REQUIRED')!r} is not a "
        f"recognized boolean (use 1/true/yes/on or 0/false/no/off). Refusing to guess whether "
        f"the mandatory TLS gate is on."
    )


def main():
    raw = os.environ.get("CONDA_TLS_PROBE_CONN", "").strip()
    required = _required()
    if not raw:
        if required:
            sys.exit(
                "TLS_PROBE_REQUIRED_BUT_UNSET: CONDA_TLS_PROBE_REQUIRED is on but "
                "CONDA_TLS_PROBE_CONN is empty. The Encrypt=yes OpenSSL-reachability gate is "
                "MANDATORY on this leg -- provide a reachable (test/staging, least-privilege) "
                "SQL Server connection string."
            )
        print(
            "TLS_PROBE_SKIPPED: set CONDA_TLS_PROBE_CONN to a reachable SQL Server "
            "connection string (on a minimal base with no system OpenSSL) to run "
            "this Encrypt=yes gate."
        )
        return

    if not _is_probe_connection_string(raw):
        # A bare word like "yes"/"true"/"1" is the "I thought it was a yes/no toggle"
        # misconfiguration: it is NOT a connection string (no 'key=value' pair).
        if required:
            sys.exit(
                "TLS_PROBE_MISCONFIGURED: CONDA_TLS_PROBE_CONN is set but is not a connection "
                "string (no 'key=value' pair) -- a bare 'yes'/'true'/'1' is NOT a yes/no toggle. "
                "The gate is MANDATORY on this leg; set it to a reachable SQL Server connection "
                "string (Server, user, password)."
            )
        # Not required: skip LOUDLY -- the static RUNPATH audit still guards OpenSSL layout.
        print(
            "TLS_PROBE_SKIPPED: CONDA_TLS_PROBE_CONN is set but is not a connection string "
            "(no 'key=value' pair). It is NOT a yes/no toggle -- set it to a reachable SQL "
            "Server connection string (with Server, user and password keywords) to run the "
            "Encrypt=yes gate, or leave it empty to skip."
        )
        return

    conn_str = force_tls(raw)
    print("TLS_PROBE using (values redacted): " + _redact(conn_str))

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
    sys.exit("TLS HANDSHAKE DID NOT COMPLETE: " + describe(outcome))


if __name__ == "__main__":
    main()
