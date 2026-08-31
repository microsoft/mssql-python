"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
Selects which ODBC provider (native driver package) mssql-python loads.

Two providers are supported: ``msodbcsql18`` (the Microsoft ODBC Driver 18,
shipped by ``mssql_python_odbc``) and ``mssql-odbc`` (the Rust driver, shipped
by ``mssql_python_rust_odbc``). Selection is process-wide and resolved exactly
once, before the native driver loads, from — in precedence order — the
``MSSQL_PYTHON_NATIVE_PROVIDER`` environment variable, the ``mssql_python.native_provider``
module property, then the release default. An unknown value fails closed rather
than falling back.
"""

import os
import threading
import warnings
import importlib
from typing import Dict, Optional, Tuple

from mssql_python.logging import logger

NATIVE_PROVIDER_ENV_VAR = "MSSQL_PYTHON_NATIVE_PROVIDER"

# Customer-facing provider identifiers.
PROVIDER_MSODBCSQL18 = "msodbcsql18"
PROVIDER_MSSQL_ODBC = "mssql-odbc"

# Phase 1 default. Phase 2 flips this to PROVIDER_MSSQL_ODBC via a documented release.
_DEFAULT_PROVIDER = PROVIDER_MSODBCSQL18

# Provider -> import package that ships its native binaries.
_PACKAGE_BY_PROVIDER: Dict[str, str] = {
    PROVIDER_MSODBCSQL18: "mssql_python_odbc",
    PROVIDER_MSSQL_ODBC: "mssql_python_rust_odbc",
}

# Provider -> the pip distribution that installs its package (for error hints).
_DIST_BY_PROVIDER: Dict[str, str] = {
    PROVIDER_MSODBCSQL18: "mssql-python-odbc",
    PROVIDER_MSSQL_ODBC: "mssql-python-rust-odbc",
}


def _normalize(value: str) -> str:
    """Return the canonical provider id for ``value`` or raise ``ValueError``.

    An unrecognized selection is rejected so a typo fails closed instead of
    silently loading the default provider.
    """
    canonical = value.strip().lower()
    if canonical not in _PACKAGE_BY_PROVIDER:
        valid = ", ".join(sorted(_PACKAGE_BY_PROVIDER))
        raise ValueError(f"Unknown ODBC provider {value!r}. Valid providers are: {valid}.")
    return canonical


class ProviderManager:
    """Process-wide, resolve-once selector for the ODBC provider.

    The selection freezes when :meth:`resolve` first runs (at native driver
    load). A later change to the module property is ignored with a warning,
    mirroring the connection-pool configuration model.
    """

    _lock: threading.Lock = threading.Lock()
    _property_value: Optional[str] = None
    _resolved: Optional[str] = None
    _source: Optional[str] = None

    @classmethod
    def _select(cls) -> Tuple[str, str]:
        """Return the raw (unvalidated) selection and its source (lock-free)."""
        env_value = os.environ.get(NATIVE_PROVIDER_ENV_VAR)
        if env_value and env_value.strip():
            return env_value.strip(), "environment"
        if cls._property_value is not None:
            return cls._property_value, "property"
        return _DEFAULT_PROVIDER, "default"

    @classmethod
    def _compute(cls) -> Tuple[str, str]:
        """Apply precedence and validate; raises ``ValueError`` on an unknown id."""
        raw, source = cls._select()
        return _normalize(raw), source

    @classmethod
    def set_property(cls, value: str) -> None:
        """Set the module-property selection.

        A change after the provider has been resolved is ignored with a warning;
        the env var still takes precedence over this value when both are set.
        """
        with cls._lock:
            canonical = _normalize(value)
            if cls._resolved is not None:
                if canonical != cls._resolved:
                    cls._warn_frozen()
                return
            cls._property_value = canonical

    @classmethod
    def resolve(cls) -> str:
        """Resolve and freeze the provider, returning its canonical id."""
        with cls._lock:
            if cls._resolved is None:
                cls._resolved, cls._source = cls._compute()
                logger.info(
                    "ODBC provider resolved to '%s' (source=%s)",
                    cls._resolved,
                    cls._source,
                )
            return cls._resolved

    @classmethod
    def effective(cls) -> str:
        """Return the provider that would be used, without freezing it.

        Read-only path: an invalid selection is reported as the default rather
        than raised, so plain attribute access (and ``from mssql_python import
        *``) never fails. The hard failure is deferred to :meth:`resolve` /
        :meth:`ensure_available`, where it is actionable.
        """
        with cls._lock:
            if cls._resolved is not None:
                return cls._resolved
            raw, _ = cls._select()
            try:
                return _normalize(raw)
            except ValueError:
                return _DEFAULT_PROVIDER

    @classmethod
    def package_name(cls, provider: Optional[str] = None) -> str:
        """Return the import package that ships ``provider``'s native binaries."""
        provider = provider or cls.effective()
        return _PACKAGE_BY_PROVIDER[provider]

    @classmethod
    def ensure_available(cls) -> str:
        """Resolve and freeze the provider, verifying its package is installed.

        Called before the native driver loads. Fails closed with an actionable
        error if the selected provider's package is missing, rather than
        silently loading a different provider.
        """
        provider = cls.resolve()
        package = _PACKAGE_BY_PROVIDER[provider]
        try:
            importlib.import_module(package)
        except ModuleNotFoundError as exc:
            # Only translate a genuinely-missing provider package. A
            # ModuleNotFoundError naming something else means the package is
            # installed but a transitive import failed, so surface the real error.
            if exc.name != package:
                raise
            dist = _DIST_BY_PROVIDER[provider]
            raise ImportError(
                f"The '{provider}' ODBC provider is selected but its package "
                f"'{package}' is not installed. Install it with: pip install {dist}"
            ) from exc
        return provider

    @classmethod
    def is_frozen(cls) -> bool:
        """Whether the provider has been resolved and can no longer change."""
        return cls._resolved is not None

    @classmethod
    def get_info(cls) -> Dict[str, object]:
        """Report the selected provider for diagnostics.

        Never raises: before the provider freezes it reports the pending
        selection and its ``source``, and an invalid selection is surfaced via
        an ``error`` key instead of an exception.
        """
        with cls._lock:
            if cls._resolved is not None:
                return {
                    "id": cls._resolved,
                    "package": _PACKAGE_BY_PROVIDER[cls._resolved],
                    "source": cls._source,
                    "frozen": True,
                }
            raw, source = cls._select()
            try:
                provider = _normalize(raw)
            except ValueError as exc:
                return {
                    "id": _DEFAULT_PROVIDER,
                    "package": _PACKAGE_BY_PROVIDER[_DEFAULT_PROVIDER],
                    "source": source,
                    "frozen": False,
                    "error": str(exc),
                }
            return {
                "id": provider,
                "package": _PACKAGE_BY_PROVIDER[provider],
                "source": source,
                "frozen": False,
            }

    @classmethod
    def _warn_frozen(cls) -> None:
        message = (
            f"ODBC provider is already loaded as '{cls._resolved}'; ignoring the "
            f"change. Select a provider before the first connection, or set the "
            f"{NATIVE_PROVIDER_ENV_VAR} environment variable."
        )
        logger.warning(message)
        warnings.warn(message, RuntimeWarning, stacklevel=3)

    @classmethod
    def _reset_for_testing(cls) -> None:
        """Reset selection state - for testing purposes only."""
        with cls._lock:
            cls._property_value = None
            cls._resolved = None
            cls._source = None
