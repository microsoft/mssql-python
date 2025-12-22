"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

This module provides functions for managing decimal separator configuration.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mssql_python.helpers import Settings


def setDecimalSeparator(separator: str, settings: "Settings", set_in_cpp_func=None) -> None:
    """
    Sets the decimal separator character used when parsing NUMERIC/DECIMAL values
    from the database, e.g. the "." in "1,234.56".

    The default is to use the current locale's "decimal_point" value when the module
    was first imported, or "." if the locale is not available. This function overrides
    the default.

    Args:
        separator (str): The character to use as decimal separator
        settings (Settings): The settings object to update
        set_in_cpp_func: Optional callable to set the separator in C++ bindings

    Raises:
        ValueError: If the separator is not a single character string
    """
    # Type validation
    if not isinstance(separator, str):
        raise ValueError("Decimal separator must be a string")

    # Length validation
    if len(separator) == 0:
        raise ValueError("Decimal separator cannot be empty")

    if len(separator) > 1:
        raise ValueError("Decimal separator must be a single character")

    # Character validation
    if separator.isspace():
        raise ValueError("Whitespace characters are not allowed as decimal separators")

    # Check for specific disallowed characters
    if separator in ["\t", "\n", "\r", "\v", "\f"]:
        raise ValueError(
            f"Control character '{repr(separator)}' is not allowed as a decimal separator"
        )

    # Set in Python side settings
    settings.decimal_separator = separator

    # Update the C++ side if available
    # Set the initial decimal separator in C++
    try:
        if set_in_cpp_func is not None:
            set_in_cpp_func(separator)
    except ImportError:
        # Handle case where ddbc_bindings is not available
        pass


def getDecimalSeparator(settings: "Settings") -> str:
    """
    Returns the decimal separator character used when parsing NUMERIC/DECIMAL values
    from the database.

    Args:
        settings (Settings): The settings object to read from

    Returns:
        str: The current decimal separator character
    """
    return settings.decimal_separator


def create_decimal_separator_functions(settings: "Settings"):
    """
    Factory function to create decimal separator getter/setter bound to specific settings.

    This function handles importing the C++ binding and initializing the decimal separator.

    Args:
        settings: The Settings instance to bind to

    Returns:
        Tuple of (setDecimalSeparator, getDecimalSeparator) functions
    """
    # Try to import and initialize the C++ binding
    try:
        from mssql_python.ddbc_bindings import DDBCSetDecimalSeparator

        # Set the initial decimal separator in C++
        DDBCSetDecimalSeparator(settings.decimal_separator)
        cpp_binding = DDBCSetDecimalSeparator
    except ImportError:
        # Handle case where ddbc_bindings is not available
        cpp_binding = None

    def setter(separator: str) -> None:
        """
        Sets the decimal separator character used when parsing NUMERIC/DECIMAL values
        from the database, e.g. the "." in "1,234.56".

        Args:
            separator (str): The character to use as decimal separator

        Raises:
            ValueError: If the separator is not a single character string
        """
        setDecimalSeparator(separator, settings, cpp_binding)

    def getter() -> str:
        """
        Returns the decimal separator character used when parsing NUMERIC/DECIMAL values
        from the database.

        Returns:
            str: The current decimal separator character
        """
        return getDecimalSeparator(settings)

    return setter, getter
