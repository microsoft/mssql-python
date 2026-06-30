"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Integration tests for FedAuth (access token) connect attributes against a
*mock TDS server*.

These tests guard the fix delivered in PR #596 / issue #594:

    SQL_COPT_SS_ACCESS_TOKEN (1256) is a *deferred* ODBC connect attribute.
    The MS ODBC driver stashes the caller's pointer at ``SQLSetConnectAttr``
    time and only dereferences it later, during ``SQLDriverConnect``, to build
    the FedAuth login packet. PR #568 briefly copied the value into a
    stack-local buffer, which was freed before the deferred read -> a
    use-after-free that variously produced SIGBUS, a server reset, or the
    error "Authentication token is missing in the federated authentication
    message".

    PR #596 stores the value in Connection-owned member buffers so it stays
    valid for the deferred dereference.

The regression is invisible to ordinary unit tests because it only manifests
once a real driver actually transmits the token to a server. The
``mssql-mock-tds`` package gives us exactly that: an in-process TDS server that
records the access token it received in the Login7 FedAuth feature. If the
deferred buffer is ever corrupted again, the token captured by the server will
not match the one we sent (or no token will arrive at all) and these tests
fail.

Requirements (both optional; tests skip cleanly when missing):
  * ``mssql-mock-tds``  -> the mock server Python bindings. Install from the
                          public feed, e.g.::

      pip install --index-url \\
        https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/pypi/simple/ \\
        mssql-mock-tds

  * ``cryptography``    -> used to generate the throwaway TLS identity the mock
                          server needs (already pulled in transitively by
                          ``azure-identity``).
"""

import datetime
import os
import secrets
import struct

import pytest

from mssql_python.constants import ConstantsDDBC

# ---------------------------------------------------------------------------
# Optional-dependency probing. The whole module is skipped (not failed) when a
# dependency is unavailable so the suite stays green on machines/CI legs that do
# not install the mock server.
# ---------------------------------------------------------------------------
try:
    import mssql_mock_tds

    MOCK_TDS_AVAILABLE = True
except ImportError:
    mssql_mock_tds = None
    MOCK_TDS_AVAILABLE = False

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12, NoEncryption
    from cryptography.x509.oid import NameOID

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False


pytestmark = pytest.mark.skipif(
    not (MOCK_TDS_AVAILABLE and CRYPTOGRAPHY_AVAILABLE),
    reason=(
        "Requires the 'mssql-mock-tds' package and 'cryptography'. "
        "Install mssql-mock-tds from the mssql-rs public feed to run these tests."
    ),
)

# Keep the per-connect wait short: the mock server records the FedAuth token
# while processing Login7 but does not complete the handshake the way the real
# ODBC driver expects, so the driver eventually times out. The token has
# already been captured by then, so a small login timeout keeps the tests fast.
_LOGIN_TIMEOUT_SECONDS = 3


def _write_test_identity(directory):
    """Generate a self-signed TLS identity the mock server can load.

    The mock server looks for a TLS identity at ``<cwd>/tests/test_certificates``
    (among a few relative candidates) and resolves it in this order: it first
    tries ``valid_cert.pem`` + ``key.pem`` via ``create_test_identity`` (OpenSSL,
    non-Windows only), then falls back to ``identity.pfx`` via
    ``load_identity_from_file`` with an *empty* password.

    Those two paths need different files per platform:

    * **Non-Windows (Linux, macOS):** emit the PEM pair. ``create_test_identity``
      re-packs them into a 3DES-encrypted PKCS#12 with a non-empty password,
      which macOS' Security framework accepts. A password-less ``identity.pfx``
      would *not* load on macOS -- ``Identity::from_pkcs12`` there rejects an
      unencrypted PKCS#12 with "The user name or passphrase you entered is not
      correct.", and we cannot influence the empty password the binding uses.
    * **Windows:** ``create_test_identity`` is unavailable (no bundled OpenSSL),
      so emit ``identity.pfx``. Schannel happily loads a password-less PKCS#12.
    """
    cert_dir = os.path.join(directory, "tests", "test_certificates")
    os.makedirs(cert_dir, exist_ok=True)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        ]
    )
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .sign(key, hashes.SHA256())
    )

    if os.name == "nt":
        pfx = pkcs12.serialize_key_and_certificates(
            name=b"mock-tds",
            key=key,
            cert=cert,
            cas=None,
            encryption_algorithm=NoEncryption(),
        )
        with open(os.path.join(cert_dir, "identity.pfx"), "wb") as handle:
            handle.write(pfx)
    else:
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
        with open(os.path.join(cert_dir, "valid_cert.pem"), "wb") as handle:
            handle.write(cert_pem)
        with open(os.path.join(cert_dir, "key.pem"), "wb") as handle:
            handle.write(key_pem)


def _access_token_struct(raw_token):
    """Build the ODBC SQL_COPT_SS_ACCESS_TOKEN value for *raw_token*.

    Mirrors ``mssql_python.auth.AADAuth.get_token_struct``: a little-endian
    4-byte length prefix followed by the UTF-16LE encoded token. The driver
    unwraps this struct and transmits only the bare token in the Login7 FedAuth
    feature, which is what the mock server records.
    """
    token_bytes = raw_token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


@pytest.fixture
def mock_tls_server(tmp_path, monkeypatch):
    """Start a TLS-enabled mock TDS server bound to an OS-assigned port.

    The mock loads its TLS identity from a path relative to the current working
    directory, so we generate a throwaway identity under ``tmp_path`` and chdir
    there for the lifetime of the test (``monkeypatch`` restores cwd afterwards).
    """
    _write_test_identity(str(tmp_path))
    monkeypatch.chdir(tmp_path)

    server = mssql_mock_tds.PyMockTdsServer(port=0, tls=True)
    server.start()
    try:
        yield server
    finally:
        server.stop()


def _connect_with_token(server, raw_token):
    """Best-effort connect to *server* using *raw_token* as the access token.

    Returns the captured connection error (if any). The real ODBC driver times
    out because the mock does not finish the handshake; that is expected and not
    what these tests assert on -- they assert on the token the server received.
    """
    import mssql_python

    conn_str = (
        f"Server={server.sql_address};Database=master;" "Encrypt=yes;TrustServerCertificate=yes;"
    )
    attrs_before = {
        ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value: _access_token_struct(raw_token),
        ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value: _LOGIN_TIMEOUT_SECONDS,
    }
    try:
        conn = mssql_python.connect(conn_str, attrs_before=attrs_before)
    except Exception as exc:  # noqa: BLE001 - handshake completion is not under test
        return exc
    # If a future mock learns to complete the handshake, don't leak the handle.
    try:
        conn.close()
    except Exception:  # noqa: BLE001
        pass
    return None


class TestMockServerFedAuth:
    """FedAuth access-token transmission against the mock TDS server."""

    def test_access_token_round_trips_to_server(self, mock_tls_server):
        """The exact access token we set must reach the server intact.

        This is the direct regression guard for issue #594 / PR #596. With the
        use-after-free, the driver dereferenced a freed buffer and the FedAuth
        token arrived corrupted, empty, or not at all (or the process crashed).
        With the fix the Connection-owned buffer survives the deferred read and
        the server records the byte-for-byte token.
        """
        token = "mock_access_token_for_pyodbc_regression_594"

        self_error = _connect_with_token(mock_tls_server, token)

        # The connection itself may time out (the mock doesn't finish login),
        # but the token must have been transmitted before that point.
        assert mock_tls_server.connection_count() >= 1, (
            "Mock server recorded no connection; the driver never reached "
            f"Login7. Last connect error: {self_error!r}"
        )
        assert mock_tls_server.has_received_token(token), (
            "Mock server did not receive the expected access token. This is the "
            "#594/#596 deferred connect-attribute use-after-free signature "
            f"(last token={mock_tls_server.get_last_access_token()!r})."
        )
        assert mock_tls_server.get_last_access_token() == token

    def test_unique_access_token_transmitted_exactly(self, mock_tls_server):
        """A random, unguessable token rules out any cached/constant fallback."""
        token = f"regression_token_{secrets.token_hex(24)}"

        _connect_with_token(mock_tls_server, token)

        assert mock_tls_server.get_last_access_token() == token, (
            "Unique access token was not transmitted byte-for-byte: sent "
            f"{token!r}, server received "
            f"{mock_tls_server.get_last_access_token()!r}."
        )

    def test_distinct_tokens_on_sequential_connects(self, mock_tls_server):
        """Two connects on owned buffers must not clobber each other's token.

        PR #596 hardened the fix with per-attribute owned buffers. Exercising
        two sequential connects with different tokens ensures the second token
        is recorded without invalidating buffer ownership semantics.
        """
        first = f"first_{secrets.token_hex(16)}"
        second = f"second_{secrets.token_hex(16)}"

        _connect_with_token(mock_tls_server, first)
        _connect_with_token(mock_tls_server, second)

        assert mock_tls_server.has_received_token(first)
        assert mock_tls_server.has_received_token(second)
        assert mock_tls_server.get_last_access_token() == second
