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


def test_tls_completed_false_for_pre_tls_error_carrying_18456():
    # Regression for the dropped 18456+'login' arm: a PRE-TLS network timeout whose text
    # coincidentally contains both 'login' and '18456' must NOT be read as a completed
    # handshake (it would false-pass this fail-closed gate).
    probe = _load_probe()
    msg = "[Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired. Error 18456."
    assert probe.tls_completed(RuntimeError(msg)) is False


def test_tls_completed_true_for_sqlstate_28000_localized():
    # A localized 'login failed' whose English text is translated still carries the
    # locale-independent SQLSTATE 28000 (post-handshake) -> PASS via the 28000 arm.
    probe = _load_probe()
    msg = "[Microsoft][ODBC Driver 18 for SQL Server]SQLSTATE 28000: connexion refusee."
    assert probe.tls_completed(RuntimeError(msg)) is True


def test_force_tls_appends_when_absent():
    probe = _load_probe()
    out = probe.force_tls("Server=dbserver;Database=x")
    assert "Encrypt=yes" in out
    assert "TrustServerCertificate=yes" in out


def test_force_tls_overrides_encrypt_no():
    probe = _load_probe()
    out = probe.force_tls("Server=dbserver;Encrypt=no;Database=x")
    low = out.lower()
    assert "encrypt=yes" in low
    assert "encrypt=no" not in low


def test_force_tls_is_idempotent():
    probe = _load_probe()
    once = probe.force_tls("Server=dbserver;Database=x")
    twice = probe.force_tls(once)
    assert once == twice
    # Exactly one Encrypt= and one TrustServerCertificate= key.
    assert twice.lower().count("encrypt=") == 1
    assert twice.lower().count("trustservercertificate=") == 1


# --- hardening: force_tls must never hand the parser a malformed string ----------
# The mssql_python parser splits on top-level ';' and rejects any segment without an
# '=' ("keyword '<x>' has no value") or a duplicated keyword. A regex substitution
# could produce exactly those; rebuilding from tokens must not.


def _every_segment_has_value(probe, conn):
    """Mirror the parser's rule: each non-empty top-level ';'-segment needs '='."""
    return all("=" in seg.strip() for seg in probe._split_top_level(conn) if seg.strip())


def _key_count(probe, conn, wanted):
    total = 0
    for seg in probe._split_top_level(conn):
        token = seg.strip()
        if token and token.split("=", 1)[0].strip().lower() == wanted:
            total += 1
    return total


def test_force_tls_dedups_duplicate_encrypt():
    """A duplicate Encrypt (old regex fixed only the first -> parser 'Duplicate keyword')."""
    probe = _load_probe()
    out = probe.force_tls("Server=dbserver;Encrypt=yes;Database=x;Encrypt=no")
    assert _key_count(probe, out, "encrypt") == 1
    assert "encrypt=no" not in out.lower()
    assert _every_segment_has_value(probe, out)


def test_force_tls_handles_valueless_encrypt():
    """A bare 'Encrypt' (no '=value') must not leave a value-less keyword behind."""
    probe = _load_probe()
    out = probe.force_tls("Server=dbserver;Encrypt;Database=x")
    assert _key_count(probe, out, "encrypt") == 1
    assert _every_segment_has_value(probe, out)


def test_force_tls_preserves_braced_value_with_semicolon():
    """An ODBC braced value may contain ';'; it must survive intact (MS-ODBCSTR)."""
    probe = _load_probe()
    out = probe.force_tls("Server=dbserver;Pwd={a;b};Encrypt=no")
    assert "Pwd={a;b}" in out
    assert _key_count(probe, out, "encrypt") == 1
    assert "encrypt=no" not in out.lower()
    assert _every_segment_has_value(probe, out)


@pytest.mark.parametrize(
    "raw",
    [
        "Server=dbserver;Database=master",
        "Server=dbserver",
        "server=dbserver;encrypt=no;trustservercertificate=no",
        "Server = dbserver ; Encrypt = no ; TrustServerCertificate = no",
        "Server=dbserver;Encrypt=Strict",
        "Encrypt=yes;Server=dbserver",
        "Server=tcp:dbserver,1433;Database=master",
    ],
)
def test_force_tls_output_is_parseable(raw):
    """Every realistic input yields a string whose every segment has a value and
    carries exactly one Encrypt=yes / TrustServerCertificate=yes."""
    probe = _load_probe()
    out = probe.force_tls(raw)
    assert _every_segment_has_value(probe, out)
    assert _key_count(probe, out, "encrypt") == 1
    assert _key_count(probe, out, "trustservercertificate") == 1
    assert "encrypt=yes" in out.lower()
    assert "trustservercertificate=yes" in out.lower()


def test_split_top_level_respects_braces():
    probe = _load_probe()
    assert probe._split_top_level("Server=x;Pwd={a;b};Encrypt=no") == [
        "Server=x",
        "Pwd={a;b}",
        "Encrypt=no",
    ]


def test_split_top_level_handles_escaped_braces():
    """MS-ODBCSTR: '}}' inside a braced value is a literal '}', not a close -- so a value
    containing '}}' and an internal ';' must NOT be split (matches the shipped parser)."""
    probe = _load_probe()
    assert probe._split_top_level("Server=x;Pwd={p}}w;d};Encrypt=no") == [
        "Server=x",
        "Pwd={p}}w;d}",
        "Encrypt=no",
    ]


def test_split_top_level_inner_brace_is_literal():
    """A braced value is SINGLE-LEVEL (matches _parse_braced_value): an inner '{' is a literal
    char, so '{a{b}' ends at the first '}' and ';Encrypt=no' is a separate segment -- not
    absorbed into Pwd (which would make force_tls emit a duplicate Encrypt)."""
    probe = _load_probe()
    assert probe._split_top_level("Pwd={a{b};Encrypt=no") == ["Pwd={a{b}", "Encrypt=no"]


def test_split_top_level_mid_value_brace_is_literal():
    """A '{' that is NOT the first char of a value is literal (the value is not braced), so
    'Server=a{b' is a simple value and ';' still splits -- matching the production parser."""
    probe = _load_probe()
    assert probe._split_top_level("Server=a{b;Encrypt=no") == ["Server=a{b", "Encrypt=no"]


def test_force_tls_braced_password_with_inner_brace():
    """End-to-end for the inner-'{' case: the user's Encrypt=no is dropped and Encrypt=yes
    appended exactly ONCE -- no duplicate 'encrypt' that mssql_python.connect would reject."""
    probe = _load_probe()
    out = probe.force_tls("Pwd={a{b};Encrypt=no").lower()
    assert "encrypt=yes" in out
    assert "encrypt=no" not in out
    assert out.count("encrypt=") == 1


def test_redact_masks_values_and_flags_bare_segments():
    """The debug line must never leak a value and must surface a no-value segment."""
    probe = _load_probe()
    red = probe._redact("Server=dbserver;Pwd=REDACTME;Encrypt=yes")
    assert "REDACTME" not in red
    assert "Pwd=***" in red
    assert "Server=***" in red
    # a segment with no '=' (the shape that trips the parser) is surfaced verbatim.
    assert "<<NO-VALUE:" in probe._redact("Server=dbserver;bogus;Encrypt=yes")


# --- misconfiguration guard: a bare 'yes' is NOT a connection string --------------
# CONDA_TLS_PROBE_CONN is a full connection string, not a yes/no toggle. A bare word
# (the toggle mistake) must SKIP loudly, never fail the leg on a parse error.


@pytest.mark.parametrize("bogus", ["yes", "true", "1", "no", "enable", "on", "false"])
def test_bare_word_is_not_a_connection_string(bogus):
    probe = _load_probe()
    assert probe._is_probe_connection_string(bogus) is False


@pytest.mark.parametrize(
    "good",
    [
        "Server=dbserver",
        "Database=master;Encrypt=no",
        "Server=host,1433;Encrypt=yes",
    ],
)
def test_real_connection_string_is_recognized(good):
    probe = _load_probe()
    assert probe._is_probe_connection_string(good) is True


@pytest.mark.parametrize("value", ["", "yes", "true", "1"])
def test_main_skips_loudly_for_missing_or_toggle_value(monkeypatch, capsys, value):
    """Empty OR a bare-word 'toggle' value must SKIP loudly (return before importing the
    native driver), never fail the leg on a connection-string parse error."""
    probe = _load_probe()
    monkeypatch.setenv("CONDA_TLS_PROBE_CONN", value)
    probe.main()  # returns cleanly; must NOT reach `import mssql_python`
    assert "TLS_PROBE_SKIPPED" in capsys.readouterr().out
