"""Fail-closed classification tests for ``conda/tls_connect_probe.py``.

The live ``Encrypt=yes`` conda gate runs ``conda/tls_connect_probe.py`` to prove
the driver's dlopen'd OpenSSL backend (``libssl``/``libcrypto``) is REACHABLE --
something the DB-less ``Encrypt=no`` ``driver_load_probe.py`` cannot show, because
the crypto libraries are only touched by a real TLS handshake. The classifier MUST
fail closed: only an outcome that provably means the mandatory pre-login TLS
handshake completed (a clean connect, a ``Login failed`` / 18456, or a
``Cannot open database``) may PASS; an OpenSSL-load failure or anything
unrecognized MUST fail.

These are pure, no-DB unit tests: the probe's native ``import mssql_python`` is
deferred into ``main()``, so the classifier + ``force_tls`` can be exercised
without the compiled extension or a live SQL Server.
"""

import importlib.util
from pathlib import Path

import pytest

_PROBE_PATH = Path(__file__).resolve().parent.parent / "conda" / "tls_connect_probe.py"

# The conda/ sources are not shipped inside the built wheel, so the installed-wheel
# test leg copies only tests/ into an isolated dir. Skip the whole module (rather than
# erroring at collection/run) when the conda source it exercises is absent.
if not _PROBE_PATH.is_file():
    pytest.skip(
        f"conda source not present ({_PROBE_PATH}); skipping conda TLS-probe tests",
        allow_module_level=True,
    )


def _load_probe():
    """Import ``conda/tls_connect_probe.py`` as a standalone module."""
    spec = importlib.util.spec_from_file_location("tls_connect_probe_under_test", _PROBE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Outcomes that can only occur AFTER a mandatory Encrypt=yes handshake completes;
# every one MUST classify as "TLS completed" (PASS -> OpenSSL was reachable).
_TLS_COMPLETED_MESSAGES = [
    "[Microsoft][ODBC Driver 18 for SQL Server]Login failed for user 'x'.",
    "Login failed for user 'sa'. (18456)",
    '[Microsoft][ODBC Driver 18 for SQL Server]Cannot open database "X" requested by the '
    "login. The login failed.",
    "Changed database context to 'master'.",
]

# Outcomes that mean the crypto backend never loaded / the handshake never
# completed; every one MUST classify as "not completed" (FAIL / non-zero exit).
_TLS_FAILURE_MESSAGES = [
    "[Microsoft][ODBC Driver 18 for SQL Server]SSL Provider: The certificate chain was issued "
    "by an authority that is not trusted.",
    "libssl.so.3: cannot open shared object file: No such file or directory",
    "libcrypto.so.3: cannot open shared object file: No such file or directory",
    "[Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: Error code 0x2726",
    "[Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired",
    "dlopen(libssl.dylib): image not found",
    # Fail-closed default: an unexpected / unrelated error is NOT proof of a
    # completed handshake.
    "some totally unexpected internal error",
]


@pytest.mark.parametrize("msg", _TLS_COMPLETED_MESSAGES)
def test_tls_completed_true_for_post_handshake_outcomes(msg):
    probe = _load_probe()
    assert probe.tls_completed(RuntimeError(msg)) is True


@pytest.mark.parametrize("msg", _TLS_FAILURE_MESSAGES)
def test_tls_completed_false_for_pre_handshake_failures(msg):
    probe = _load_probe()
    assert probe.tls_completed(RuntimeError(msg)) is False


def test_tls_completed_true_for_clean_connect():
    probe = _load_probe()
    assert probe.tls_completed(None) is True


def test_force_tls_appends_when_absent():
    probe = _load_probe()
    out = probe.force_tls("Server=localhost;Database=x;Uid=x;Pwd=x;")  # DevSkim: ignore DS162092
    assert "Encrypt=yes" in out
    assert "TrustServerCertificate=yes" in out


def test_force_tls_overrides_encrypt_no():
    probe = _load_probe()
    out = probe.force_tls("Server=localhost;Encrypt=no;Database=x")  # DevSkim: ignore DS162092
    low = out.lower()
    assert "encrypt=yes" in low
    assert "encrypt=no" not in low


def test_force_tls_is_idempotent():
    probe = _load_probe()
    once = probe.force_tls("Server=localhost;Database=x")  # DevSkim: ignore DS162092
    twice = probe.force_tls(once)
    assert once == twice
    # Exactly one Encrypt= and one TrustServerCertificate= key.
    assert twice.lower().count("encrypt=") == 1
    assert twice.lower().count("trustservercertificate=") == 1
