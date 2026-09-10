"""Build the internal py-core context for asynchronous connections."""

from typing import Any

from ..connection_string_parser import _ConnectionStringParser
from ..exceptions import NotSupportedError
from ..helpers import connstr_to_pycore_params


def build_async_connection_context(connection_str: str, timeout: int) -> dict[str, Any]:
    """Parse and validate a public connection string for py-core."""
    if not isinstance(connection_str, str):
        raise TypeError("connection_str must be a string")
    if "\x00" in connection_str:
        raise ValueError("Connection string must not contain a NUL (\\x00) character")
    if isinstance(timeout, bool) or not isinstance(timeout, int):
        raise TypeError("Login timeout must be an integer")
    if timeout < 0:
        raise ValueError("Login timeout cannot be negative")

    parser = _ConnectionStringParser(validate_keywords=True)
    params = parser._parse(connection_str)
    if not any(params.get(key) for key in ("server", "addr", "address")):
        raise ValueError("SERVER parameter is required in connection string")

    authentication = params.get("authentication", "").strip().lower()
    if authentication and authentication != "sqlpassword":
        raise NotSupportedError(
            "Async Entra authentication is not supported yet",
            "Use SQL authentication or Trusted_Connection for async queries",
        )

    context = connstr_to_pycore_params(params, strict=True)
    if timeout > 0:
        context["connect_timeout"] = timeout
    return context
