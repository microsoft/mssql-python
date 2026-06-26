"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module handles authentication for the mssql_python package.
"""

import hashlib
import inspect
import os
import platform
import struct
import threading
import time
import warnings
from typing import Tuple, Dict, Optional, Any, Protocol, runtime_checkable

from mssql_python.logging import logger
from mssql_python.constants import (
    AuthType,
    _AuthInternal,
    _KEY_AUTHENTICATION,
    _KEY_UID,
    _KEY_PWD,
    _KEY_TRUSTED_CONNECTION,
)
from mssql_python.exceptions import InterfaceError, OperationalError

# Module-level credential instance cache.
# Reusing credential objects allows the Azure Identity SDK's built-in
# in-memory token cache to work, avoiding redundant token acquisitions.
# See: https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/TOKEN_CACHING.md
#
# Cache is keyed on (auth_type, sorted credential_kwargs), which is
# bounded by the distinct credentials a single process ever uses.
_credential_cache: Dict[object, object] = {}
_credential_cache_lock = threading.Lock()

# Canonical keys to strip when handing an Entra-token connection to ODBC.
_SENSITIVE_KEYS = frozenset({_KEY_UID, _KEY_PWD, _KEY_TRUSTED_CONNECTION, _KEY_AUTHENTICATION})

# Azure SQL Database OAuth scope for the Azure **commercial** cloud. Shared by
# the built-in AADAuth path and the custom token_provider path. Sovereign
# clouds (Azure US Gov, Azure China, Azure Germany) are out of scope — a token
# for a different audience is rejected by SQL Server at login.
_DATABASE_SCOPE = "https://database.windows.net/.default"

# Absolute, case-normalized directory of this package, used to attribute
# warnings to the first caller *outside* the package (see _stacklevel_to_caller).
_PACKAGE_DIR = os.path.normcase(os.path.dirname(os.path.abspath(__file__)))


def _stacklevel_to_caller() -> int:
    """Return a ``warnings.warn`` stacklevel pointing at the first frame outside
    this package.

    The token-expiry warning is reached through two different internal call
    chains — the connect path (via ``acquire_token_from_credential``) and the
    bulk-copy path (via ``acquire_raw_token_from_credential``) — which sit at
    different depths. A fixed ``stacklevel`` cannot point at user code for both,
    so we walk outward until we leave the package. Falls back to 2 if no
    external frame is found.
    """
    frame = inspect.currentframe()
    try:
        # Start at the caller of this helper, i.e. the frame that issues warn()
        # (stacklevel == 1 for that warn() call).
        frame = frame.f_back if frame else None
        level = 1
        while frame is not None:
            if not os.path.normcase(frame.f_code.co_filename).startswith(_PACKAGE_DIR):
                return level
            frame = frame.f_back
            level += 1
        return 2
    finally:
        # Break the reference cycle created by holding a frame object.
        del frame


@runtime_checkable
class TokenProvider(Protocol):
    """Structural type accepted by the ``token_provider=`` connect parameter.

    Any object exposing a ``get_token(scope, ...)`` method qualifies — notably
    every ``azure-identity`` credential class (``DefaultAzureCredential``,
    ``AzureCliCredential``, ``ManagedIdentityCredential``, etc.). The returned
    object must expose a non-empty ``.token`` (str); an optional
    ``.expires_on`` (int POSIX timestamp) is captured for diagnostics.
    """

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:  # pragma: no cover - protocol
        ...


# Map Authentication connection-string values to internal short names.
_AUTH_TYPE_MAP: Dict[str, str] = {
    AuthType.INTERACTIVE.value: _AuthInternal.INTERACTIVE,
    AuthType.DEVICE_CODE.value: _AuthInternal.DEVICE_CODE,
    AuthType.DEFAULT.value: _AuthInternal.DEFAULT,
    AuthType.MSI.value: _AuthInternal.MSI,
    AuthType.SERVICE_PRINCIPAL.value: _AuthInternal.SERVICE_PRINCIPAL,
}


def _credential_cache_key(auth_type: str, credential_kwargs: Optional[Dict[str, str]]):
    """Build a hashable cache key from auth_type and optional credential kwargs.

    Returns the plain auth_type string when no kwargs are provided so that
    callers caching by string (the original behavior) keep working. When
    kwargs are present (e.g. user-assigned MSI client_id), the key is a
    tuple of ``(auth_type, sorted_kwargs_items)`` so different kwargs map
    to different cached credentials.
    """
    if not credential_kwargs:
        return auth_type
    return (auth_type, tuple(sorted(credential_kwargs.items())))


class AADAuth:
    """Handles Azure Active Directory authentication"""

    @staticmethod
    def get_token_struct(token: str) -> bytes:
        """Convert token to SQL Server compatible format"""
        logger.debug(
            "get_token_struct: Converting token to SQL Server format - token_length=%d chars",
            len(token),
        )
        token_bytes = token.encode("UTF-16-LE")
        logger.debug(
            "get_token_struct: Token encoded to UTF-16-LE - byte_length=%d", len(token_bytes)
        )
        return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    @staticmethod
    def get_token(auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None) -> bytes:
        """Get DDBC token struct for the specified authentication type."""
        token_struct, _ = AADAuth._acquire_token(auth_type, credential_kwargs)
        return token_struct

    @staticmethod
    def get_raw_token(auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None) -> str:
        """Acquire a raw JWT for the mssql-py-core connection (bulk copy).

        Uses the cached credential instance so the Azure Identity SDK's
        built-in token cache can serve a valid token without a round-trip
        when the previous token has not yet expired.
        """
        _, raw_token = AADAuth._acquire_token(auth_type, credential_kwargs)
        return raw_token

    @staticmethod
    def _acquire_token(
        auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None
    ) -> Tuple[bytes, str]:
        """Internal: acquire token and return (ddbc_struct, raw_jwt)."""
        # Import Azure libraries inside method to support test mocking
        # pylint: disable=import-outside-toplevel
        try:
            from azure.identity import (
                DefaultAzureCredential,
                DeviceCodeCredential,
                InteractiveBrowserCredential,
                ManagedIdentityCredential,
            )
            from azure.core.exceptions import ClientAuthenticationError
        except ImportError as e:
            raise RuntimeError(
                "Azure authentication libraries are not installed. "
                "Please install with: pip install azure-identity azure-core"
            ) from e

        # Mapping of auth types to credential classes
        credential_map = {
            _AuthInternal.DEFAULT: DefaultAzureCredential,
            _AuthInternal.DEVICE_CODE: DeviceCodeCredential,
            _AuthInternal.INTERACTIVE: InteractiveBrowserCredential,
            _AuthInternal.MSI: ManagedIdentityCredential,
        }

        credential_class = credential_map.get(auth_type)
        if not credential_class:
            raise ValueError(
                f"Unsupported auth_type '{auth_type}'. " f"Supported: {', '.join(credential_map)}"
            )
        logger.info(
            "get_token: Starting Azure AD authentication - auth_type=%s, credential_class=%s",
            auth_type,
            credential_class.__name__,
        )

        kwargs = credential_kwargs or {}
        cache_key = _credential_cache_key(auth_type, kwargs)
        try:
            with _credential_cache_lock:
                if cache_key not in _credential_cache:
                    logger.debug(
                        "get_token: Creating new credential instance for auth_type=%s",
                        auth_type,
                    )
                    _credential_cache[cache_key] = credential_class(**kwargs)
                else:
                    logger.debug(
                        "get_token: Reusing cached credential instance for auth_type=%s",
                        auth_type,
                    )
                credential = _credential_cache[cache_key]
            raw_token = credential.get_token(_DATABASE_SCOPE).token
            logger.info(
                "get_token: Azure AD token acquired successfully - token_length=%d chars",
                len(raw_token),
            )
            token_struct = AADAuth.get_token_struct(raw_token)
            return token_struct, raw_token
        except ClientAuthenticationError as e:
            logger.error(
                "get_token: Azure AD authentication failed - credential_class=%s, error=%s",
                credential_class.__name__,
                str(e),
            )
            raise RuntimeError(
                f"Azure AD authentication failed for {credential_class.__name__}: {e}. "
                f"This could be due to invalid credentials, missing environment variables, "
                f"user cancellation, network issues, or unsupported configuration."
            ) from e
        except Exception as e:
            logger.error(
                "get_token: Unexpected error during credential creation - credential_class=%s, error=%s",
                credential_class.__name__,
                str(e),
            )
            raise RuntimeError(f"Failed to create {credential_class.__name__}: {e}") from e


_RESERVED_TENANTS = frozenset({"common", "organizations", "consumers"})


def _parse_tenant_id(sts_url: str) -> Optional[str]:
    """Extract tenant ID (GUID or domain) from a FedAuthInfo STS URL.

    Expected formats:
      https://login.microsoftonline.com/<tenant>/
      https://login.microsoftonline.com/<tenant>/?...
      https://login.microsoftonline.com/<tenant>
    where <tenant> is either a GUID (e.g. ``aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee``)
    or a verified domain (e.g. ``contoso.onmicrosoft.com``). Both forms are
    accepted by ``azure.identity.ClientSecretCredential``.

    Returns ``None`` for the multi-tenant aliases ``common`` / ``organizations``
    / ``consumers``: confidential clients (SP) cannot authenticate against
    them and AAD responds with a cryptic ``AADSTS50194`` ("application is not
    configured as multi-tenant"). Failing fast in the factory surfaces a
    clearer error than the AAD round-trip would.
    """
    # pylint: disable=import-outside-toplevel
    from urllib.parse import urlparse

    try:
        parsed = urlparse(sts_url)
    except (ValueError, AttributeError):
        return None
    # Reject anything that isn't an https URL with a netloc. ``urlparse`` will
    # happily put a bare string like ``"tenant-guid"`` into ``path``, which
    # would then look like a valid tenant. Azure AD STS URLs are always https.
    if parsed.scheme != "https" or not parsed.netloc:
        return None
    path = (parsed.path or "").strip("/")
    if not path:
        return None
    first_segment = path.split("/", 1)[0]
    if not first_segment:
        return None
    if first_segment.lower() in _RESERVED_TENANTS:
        return None
    return first_segment


class ServicePrincipalAuth:
    """Builds an ``entra_id_token_factory`` callable for ActiveDirectoryServicePrincipal.

    The bulkcopy path through mssql-py-core uses callback-based token
    acquisition (FedAuth workflow ``0x02``) because tenant_id is only known
    from the STS URL that the server returns during the TDS handshake.
    """

    @staticmethod
    def make_token_factory(client_id: str, client_secret: str):
        """Return a callable suitable for ``entra_id_token_factory``.

        Signature: ``(spn: str, sts_url: str, auth_method: str) -> bytes``.
        Returns the JWT encoded as UTF-16LE bytes (the TDS FedAuth wire format).

        ``ClientSecretCredential`` instances are reused across calls via the
        module-level ``_credential_cache``, keyed by
        ``("serviceprincipal", tenant_id, client_id)`` so that azure-identity's
        in-memory token cache (which is per-credential-instance) actually
        works across handshake retries, reconnects, and separate bulkcopy
        invocations using the same identity.
        """
        if not client_id:
            raise ValueError("ServicePrincipal auth requires a non-empty client_id (UID)")
        if not client_secret:
            raise ValueError("ServicePrincipal auth requires a non-empty client_secret (PWD)")

        # Hash once at factory-creation time; client_secret is fixed for the
        # lifetime of the closure so there is no need to recompute per call.
        secret_hash = hashlib.sha256(client_secret.encode("utf-8")).hexdigest()

        def _factory(spn: str, sts_url: str, _auth_method: str) -> bytes:
            # pylint: disable=import-outside-toplevel
            try:
                from azure.identity import ClientSecretCredential
                from azure.core.exceptions import ClientAuthenticationError
                from azure.core.pipeline.transport import RequestsTransport
            except ImportError as e:
                raise RuntimeError(
                    "Azure authentication libraries are not installed. "
                    "Please install with: pip install azure-identity azure-core"
                ) from e

            if not spn:
                raise RuntimeError(
                    "ServicePrincipal token factory: empty SPN from server "
                    "(cannot construct token scope)"
                )
            tenant_id = _parse_tenant_id(sts_url)
            if not tenant_id:
                raise RuntimeError(f"Could not extract tenant_id from STS URL: {sts_url!r}")

            try:
                # Reuse the shared credential cache (introduced for MSI in PR #573)
                # so SP credentials get the same per-instance token reuse semantics
                # as the other AD methods. secret_hash is computed once in the
                # outer scope (make_token_factory) so rotation of client_secret
                # produces a distinct cache key without rehashing per call.
                cache_key = _credential_cache_key(
                    _AuthInternal.SERVICE_PRINCIPAL,
                    {
                        "tenant_id": tenant_id,
                        "client_id": client_id,
                        "secret_hash": secret_hash,
                    },
                )
                with _credential_cache_lock:
                    credential = _credential_cache.get(cache_key)
                    if credential is None:
                        # Evict any stale entry for the same identity but a
                        # different secret_hash (secret was rotated). Prevents
                        # unbounded growth and removes the old secret from
                        # process memory sooner.
                        stale = [
                            k
                            for k in _credential_cache
                            if isinstance(k, tuple)
                            and len(k) == 2
                            and k[0] == _AuthInternal.SERVICE_PRINCIPAL
                            and dict(k[1]).get("tenant_id") == tenant_id
                            and dict(k[1]).get("client_id") == client_id
                            and dict(k[1]).get("secret_hash") != secret_hash
                        ]
                        for k in stale:
                            del _credential_cache[k]
                        # Bound the AAD network round-trip. Without explicit
                        # timeouts, azure-identity's defaults can let an
                        # unreachable / slow STS endpoint block the calling
                        # thread for tens of seconds. The factory runs on a
                        # mssql-py-core blocking-pool worker (tokio
                        # spawn_blocking), so a stuck callback ties that
                        # worker up for the duration. SP is non-interactive
                        # and token issuance is typically <1s; 10s/15s is
                        # generous and still bounded.
                        transport = RequestsTransport(
                            connection_timeout=10,
                            read_timeout=15,
                        )
                        # KNOWN LIMITATION: ``authority=`` is not passed,
                        # so this defaults to the public-cloud authority
                        # (login.microsoftonline.com). Sovereign clouds
                        # (Azure US Gov, Azure China) are not supported on
                        # this code path today: AAD will fail with
                        # "tenant not found" because the tenant lives in
                        # the sovereign cloud's AAD, not the public one.
                        # Tracked as a follow-up; the fix is to derive
                        # ``authority`` from ``urlparse(sts_url).netloc``.
                        # Out of scope for the initial #534 work.
                        credential = ClientSecretCredential(
                            tenant_id=tenant_id,
                            client_id=client_id,
                            client_secret=client_secret,
                            transport=transport,
                        )
                        _credential_cache[cache_key] = credential
                # mssql-tds passes the resource SPN; azure-identity wants a scope.
                scope = spn if spn.endswith("/.default") else spn.rstrip("/") + "/.default"
                token = credential.get_token(scope).token
                logger.info(
                    "ServicePrincipal token factory: token acquired, length=%d chars",
                    len(token),
                )
                # Return bare UTF-16LE JWT bytes. Do NOT length-prefix like
                # AADAuth.get_token_struct does for the access_token path;
                # py-core handles the FedAuth length-prefix wrapping itself.
                return token.encode("utf-16-le")
            except ClientAuthenticationError as e:
                # Keep the detailed provider error in debug logs only. The
                # surfaced message is intentionally generic so that any
                # secret-bearing provider text never reaches the user-facing
                # exception chain.
                logger.error(
                    "ServicePrincipal authentication failed: tenant=%s, error=%s",
                    tenant_id,
                    str(e),
                )
                raise RuntimeError(
                    "ServicePrincipal authentication failed; " "see error logs for provider details"
                ) from None

        return _factory


def process_auth_parameters(parsed_params: Dict[str, str]) -> Optional[str]:
    """
    Extract authentication type from parsed connection parameters.

    Returns the internal auth type string needed for token acquisition,
    or None when the driver should handle authentication natively
    (e.g. Windows Interactive).

    Args:
        parsed_params: Dictionary of normalized connection parameters

    Returns:
        Optional[str]: Authentication type string or None
    """
    auth_type = extract_auth_type(parsed_params)
    if not auth_type:
        return None

    # On Windows, Interactive auth is handled natively by the ODBC driver.
    if auth_type == _AuthInternal.INTERACTIVE and platform.system().lower() == "windows":
        logger.debug("process_auth_parameters: Windows platform - using native AADInteractive")
        return None

    # ServicePrincipal: ODBC (msodbcsql 17.3+) handles this natively for
    # regular queries, so return None to let ODBC own the query path. Bulkcopy
    # still needs the auth type (Connection.__init__ falls back to
    # extract_auth_type for that), and the cursor.bulkcopy() callback branch
    # registers an entra_id_token_factory because tenant_id is only known
    # from the STS URL the server returns during the FedAuth handshake.
    if auth_type == _AuthInternal.SERVICE_PRINCIPAL:
        logger.debug("process_auth_parameters: ServicePrincipal - ODBC handles natively")
        return None

    logger.debug("process_auth_parameters: auth_type=%s", auth_type)
    return auth_type


def remove_sensitive_params(parsed_params: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of *parsed_params* without credentials / auth keys."""
    return {k: v for k, v in parsed_params.items() if k not in _SENSITIVE_KEYS}


def get_auth_token(
    auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None
) -> Optional[bytes]:
    """Get DDBC authentication token struct based on auth type."""
    logger.debug("get_auth_token: Starting - auth_type=%s", auth_type)
    if not auth_type:
        logger.debug("get_auth_token: No auth_type specified, returning None")
        return None

    # Handle platform-specific logic for interactive auth
    if auth_type == _AuthInternal.INTERACTIVE and platform.system().lower() == "windows":
        logger.debug("get_auth_token: Windows interactive auth - delegating to native handler")
        return None  # Let Windows handle AADInteractive natively

    try:
        token = AADAuth.get_token(auth_type, credential_kwargs)
        logger.info("get_auth_token: Token acquired successfully - auth_type=%s", auth_type)
        return token
    except (ValueError, RuntimeError) as e:
        logger.warning(
            "get_auth_token: Token acquisition failed - auth_type=%s, error=%s", auth_type, str(e)
        )
        return None


def extract_auth_type(parsed_params: Dict[str, str]) -> Optional[str]:
    """Map the Authentication connection-string value to an internal type name.

    Returns ``"interactive"``, ``"devicecode"``, ``"default"``, ``"msi"``,
    or *None* for unrecognised / absent values.  This is a pure mapping with
    no platform checks — use :func:`process_auth_parameters` when you need
    the Windows-Interactive suppression logic.
    """
    auth_value = parsed_params.get(_KEY_AUTHENTICATION, "").strip().lower()
    return _AUTH_TYPE_MAP.get(auth_value)


def _get_token_from_credential(credential: "TokenProvider") -> Tuple[str, Optional[int]]:
    """Internal: call credential.get_token() and return ``(raw_jwt, expires_on)``.

    Centralises the token-acquisition + error-wrapping logic that both
    :func:`acquire_token_from_credential` and
    :func:`acquire_raw_token_from_credential` need.

    ``expires_on`` is the POSIX timestamp (seconds) at which the token
    expires, taken from the credential's ``AccessToken`` result when present
    (it is ``None`` if the provider does not supply one). It is captured so
    callers can log it and reason about token lifetime; the access token
    itself is a *pre-connect* ODBC attribute and cannot be refreshed on a
    live connection (see the module docs on token lifecycle).

    Note:
        The scope is hard-coded to the Azure **commercial** cloud
        (``https://database.windows.net/.default``). Sovereign clouds
        (Azure US Government, Azure China, Azure Germany) are **out of
        scope** for the ``token_provider`` path — for those, supply a
        pre-acquired token via ``attrs_before[SQL_COPT_SS_ACCESS_TOKEN]``.

    Raises:
        InterfaceError: If the provider returns no valid ``.token`` string.
        OperationalError: If the underlying ``get_token()`` call fails.
    """
    start_time = time.perf_counter()
    try:
        token_result = credential.get_token(_DATABASE_SCOPE)
    except TypeError as e:
        # get_token() is called with exactly one positional scope argument, so
        # a TypeError here almost always means its signature can't accept a
        # scope (e.g. a zero-arg or keyword-only get_token). Surface that as a
        # clear, actionable InterfaceError instead of an opaque failure. This
        # is the call-time source of truth for arity — the connect() path only
        # *warns* on a suspicious signature so it never blocks a credential
        # whose signature is merely hard to introspect (partial/decorated).
        raise InterfaceError(
            driver_error=(
                "token_provider.get_token() must accept a scope positional "
                "argument, e.g. get_token(scope)."
            ),
            ddbc_error=str(e),
        ) from e
    except Exception as e:
        logger.error(
            "_get_token_from_credential: get_token() failed - credential=%s, error=%s",
            type(credential).__name__,
            str(e),
        )
        # Preserve the original credential exception (e.g. azure-identity
        # ClientAuthenticationError) as __cause__ for programmatic handling.
        raise OperationalError(
            driver_error=(f"Failed to acquire token from credential ({type(credential).__name__})"),
            ddbc_error=str(e),
        ) from e

    # azure.identity.aio (async) credentials return a coroutine from a
    # synchronous get_token() call. Detect it and fail with an async-specific
    # message rather than tripping over a missing .token attribute — and close
    # the coroutine so it doesn't emit a "coroutine was never awaited" warning.
    if inspect.iscoroutine(token_result):
        token_result.close()
        raise InterfaceError(
            driver_error=(
                "token_provider.get_token() returned a coroutine, which indicates "
                "an async credential (e.g. from azure.identity.aio). Use a "
                "synchronous credential instead."
            ),
            ddbc_error=f"got coroutine from {type(credential).__name__}.get_token()",
        )

    raw_token = getattr(token_result, "token", None)
    if not isinstance(raw_token, str) or not raw_token:
        raise InterfaceError(
            driver_error=(
                "token_provider.get_token() must return an object with a non-empty "
                "string '.token' attribute."
            ),
            ddbc_error=f"got .token of type {type(raw_token).__name__}",
        )

    expires_on = getattr(token_result, "expires_on", None)
    # Warn (don't fail) if the credential handed back an already-expired token:
    # the server enforces expiry and will reject the login, so surfacing it here
    # points at the real cause instead of an opaque later failure. Only numeric
    # POSIX timestamps are checked; bools are excluded to avoid false positives.
    if (
        isinstance(expires_on, (int, float))
        and not isinstance(expires_on, bool)
        and expires_on < time.time()
    ):
        warnings.warn(
            f"token_provider returned a token that is already expired "
            f"(expires_on={expires_on} is in the past). The server will likely "
            f"reject the connection.",
            UserWarning,
            stacklevel=_stacklevel_to_caller(),
        )
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        "_get_token_from_credential: Token acquired from %s - length=%d chars, "
        "expires_on=%s, duration_ms=%.2f",
        type(credential).__name__,
        len(raw_token),
        expires_on,
        elapsed_ms,
    )
    return raw_token, expires_on


def acquire_token_from_credential(credential: "TokenProvider") -> Tuple[bytes, Optional[int]]:
    """Acquire an ODBC token struct from a user-supplied credential object.

    The credential must follow the Azure ``TokenCredential`` protocol — i.e.
    have a ``.get_token(scope)`` method returning an object with a ``.token``
    attribute (a raw JWT string).

    .. note::
        The scope is fixed to the Azure **commercial** cloud
        (``https://database.windows.net/.default``). Sovereign clouds (Azure
        US Government, Azure China, Azure Germany) are **out of scope** — for
        those, supply a pre-acquired token via
        ``attrs_before[SQL_COPT_SS_ACCESS_TOKEN]`` instead.

    Args:
        credential: Any object with a ``.get_token(scope)`` method.

    Returns:
        Tuple[bytes, Optional[int]]: The ODBC token struct for
        ``SQL_COPT_SS_ACCESS_TOKEN`` and the token's ``expires_on`` POSIX
        timestamp (``None`` if the provider does not supply one).

    Raises:
        InterfaceError: If the provider returns no valid ``.token`` string.
        OperationalError: If the underlying ``get_token()`` call fails.
    """
    raw_token, expires_on = _get_token_from_credential(credential)
    return AADAuth.get_token_struct(raw_token), expires_on


def acquire_raw_token_from_credential(credential: "TokenProvider") -> Tuple[str, Optional[int]]:
    """Acquire a raw JWT string from a user-supplied credential object.

    Used by bulk copy, which needs the raw JWT rather than the ODBC struct.

    .. note::
        The scope is fixed to the Azure **commercial** cloud. Sovereign
        clouds are **out of scope** — see
        :func:`acquire_token_from_credential`.

    Args:
        credential: Any object with a ``.get_token(scope)`` method.

    Returns:
        Tuple[str, Optional[int]]: The raw JWT token string and the token's
        ``expires_on`` POSIX timestamp (``None`` if the provider does not
        supply one).

    Raises:
        InterfaceError: If the provider returns no valid ``.token`` string.
        OperationalError: If the underlying ``get_token()`` call fails.
    """
    return _get_token_from_credential(credential)
