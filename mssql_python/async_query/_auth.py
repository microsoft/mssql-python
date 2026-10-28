"""Entra authentication for individual py-core async connections.

Only credential instances are cached, never connections or raw tokens. Interactive
credentials consequently share a signed-in account within this process; multi-user
applications should supply their own per-user token providers.
"""

import asyncio
import threading
import warnings
from typing import TYPE_CHECKING, Any, Optional

from ..auth import (
    ServicePrincipalAuth,
    acquire_raw_token_from_credential,
    extract_auth_type,
)
from ..constants import _AuthInternal, _KEY_AUTHENTICATION
from ..exceptions import InterfaceError, OperationalError
from ..logging import logger

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

    from ..connection import TokenProvider


_credential_cache: dict[tuple[str, Optional[str]], "TokenCredential"] = {}
_credential_cache_lock = threading.Lock()


def _get_credential(auth_type: str, client_id: Optional[str]) -> "TokenCredential":
    try:
        from azure.identity import (
            DefaultAzureCredential,
            DeviceCodeCredential,
            InteractiveBrowserCredential,
            ManagedIdentityCredential,
        )
    except ImportError as error:
        raise OperationalError(
            "Azure authentication libraries are not installed. Install azure-identity.",
            str(error),
        ) from error

    credential_classes = {
        _AuthInternal.DEFAULT: DefaultAzureCredential,
        _AuthInternal.DEVICE_CODE: DeviceCodeCredential,
        _AuthInternal.INTERACTIVE: InteractiveBrowserCredential,
        _AuthInternal.MSI: ManagedIdentityCredential,
    }
    credential_class = credential_classes[auth_type]
    key = (auth_type, client_id)
    with _credential_cache_lock:
        credential = _credential_cache.get(key)
        if credential is None:
            try:
                # Azure Identity's default interactive behavior is silent-first,
                # prompting only when its cached account cannot supply a token.
                credential = (
                    credential_class(client_id=client_id) if client_id else credential_class()
                )
            except Exception as error:
                logger.error(
                    "Async authentication: credential initialization failed for %s",
                    credential_class.__name__,
                )
                raise OperationalError(
                    f"Failed to create {credential_class.__name__}",
                    str(error),
                ) from error
            _credential_cache[key] = credential
    return credential


def _acquire_builtin_token(auth_type: str, client_id: Optional[str]) -> str:
    credential = _get_credential(auth_type, client_id)
    token, _ = acquire_raw_token_from_credential(credential)
    return token


async def prepare_async_authentication(
    context: dict[str, Any],
    token_provider: Optional["TokenProvider"] = None,
) -> dict[str, Any]:
    """Return a TDS login context without blocking the Python event loop.

    ``token_provider`` accepts synchronous credentials only. Cancellation stops
    connection setup but cannot interrupt an already-running credential call in
    a worker thread. Service Principal callbacks are offloaded by py-core itself.
    """
    authenticated = dict(context)
    auth_type = extract_auth_type({_KEY_AUTHENTICATION: context.get("authentication", "")})

    if token_provider is not None:
        if "authentication" in context:
            raise InterfaceError(
                "Cannot specify both 'token_provider' and 'Authentication'. Use one token source.",
                "",
            )
        if not callable(getattr(token_provider, "get_token", None)):
            raise InterfaceError(
                "token_provider must have a callable .get_token(scope) method.", ""
            )
        dropped = [
            name for name in ("user_name", "password", "trusted_connection") if name in context
        ]
        if dropped:
            warnings.warn(
                "token_provider is set, so the following connection credentials "
                f"are ignored: {', '.join(dropped)}.",
                UserWarning,
                stacklevel=3,
            )
        raw_token, _ = await asyncio.to_thread(acquire_raw_token_from_credential, token_provider)
    elif auth_type == _AuthInternal.SERVICE_PRINCIPAL:
        try:
            authenticated["entra_id_token_factory"] = ServicePrincipalAuth.make_token_factory(
                context.get("user_name", ""), context.get("password", "")
            )
        except ValueError as error:
            raise InterfaceError(
                "ActiveDirectoryServicePrincipal requires UID (client_id) and PWD (client_secret).",
                str(error),
            ) from error
        # py-core resolves the callback's authentication method from these
        # fields, then clears the credentials from its native ClientContext.
        return authenticated
    elif auth_type:
        client_id = None
        if auth_type == _AuthInternal.MSI:
            client_id = (context.get("user_name") or "").strip() or None
        raw_token = await asyncio.to_thread(_acquire_builtin_token, auth_type, client_id)
    else:
        return authenticated

    for key in ("authentication", "user_name", "password", "trusted_connection"):
        authenticated.pop(key, None)
    authenticated["access_token"] = raw_token
    return authenticated
