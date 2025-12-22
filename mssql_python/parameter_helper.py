"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Parameter style conversion helpers for mssql-python.

Supports both qmark (?) and pyformat (%(name)s) parameter styles.
Simple character scanning approach - does NOT parse SQL contexts.

Reference: https://www.python.org/dev/peps/pep-0249/#paramstyle
"""

from typing import Dict, List, Tuple, Any, Union
from mssql_python.logging import logger


def parse_pyformat_params(sql: str) -> List[str]:
    """
    Extract %(name)s parameter names from SQL string.

    Uses simple character scanning approach - does NOT parse SQL contexts
    (strings, comments, identifiers). This means %(name)s patterns inside SQL
    string literals or comments WILL be detected as parameters.

    Args:
        sql: SQL query string with %(name)s placeholders

    Returns:
        List of parameter names in order of appearance (with duplicates if reused)

    Examples:
        >>> parse_pyformat_params("SELECT * FROM users WHERE id = %(id)s")
        ['id']

        >>> parse_pyformat_params("WHERE name = %(name)s OR email = %(name)s")
        ['name', 'name']

        >>> parse_pyformat_params("SELECT * FROM %(table)s WHERE id = %(id)s")
        ['table', 'id']
    """
    params = []
    i = 0
    length = len(sql)

    while i < length:
        # Look for %(
        if i + 2 < length and sql[i] == "%" and sql[i + 1] == "(":
            # Find the closing )
            j = i + 2
            while j < length and sql[j] != ")":
                j += 1

            # Check if we found ) and it's followed by 's'
            if j < length and sql[j] == ")":
                if j + 1 < length and sql[j + 1] == "s":
                    # Extract parameter name
                    param_name = sql[i + 2 : j]
                    params.append(param_name)
                    i = j + 2
                    continue

        i += 1

    return params


def convert_pyformat_to_qmark(sql: str, param_dict: Dict[str, Any]) -> Tuple[str, Tuple[Any, ...]]:
    """
    Convert pyformat-style query to qmark-style for ODBC execution.

    Validates that all required parameters are present and builds a positional
    parameter tuple. Supports parameter reuse (same parameter appearing multiple times).

    Args:
        sql: SQL query with %(name)s placeholders
        param_dict: Dictionary of parameter values

    Returns:
        Tuple of (rewritten_sql_with_?, positional_params_tuple)

    Raises:
        KeyError: If required parameter is missing from param_dict

    Examples:
        >>> convert_pyformat_to_qmark(
        ...     "SELECT * FROM users WHERE id = %(id)s",
        ...     {"id": 42}
        ... )
        ("SELECT * FROM users WHERE id = ?", (42,))

        >>> convert_pyformat_to_qmark(
        ...     "WHERE name = %(name)s OR email = %(name)s",
        ...     {"name": "alice"}
        ... )
        ("WHERE name = ? OR email = ?", ("alice", "alice"))
    """
    # Support %% escaping - replace %% with a placeholder before parsing
    # This allows users to have literal % in their SQL
    escaped_sql = sql.replace("%%", "\x00ESCAPED_PERCENT\x00")

    # Extract parameter names in order
    param_names = parse_pyformat_params(escaped_sql)

    if not param_names:
        # No parameters found - restore escaped %% and return as-is
        restored_sql = escaped_sql.replace("\x00ESCAPED_PERCENT\x00", "%")
        return restored_sql, ()

    # Validate all required parameters are present
    missing = set(param_names) - set(param_dict.keys())
    if missing:
        # Provide helpful error message
        missing_list = sorted(missing)
        required_list = sorted(set(param_names))
        provided_list = sorted(param_dict.keys())

        error_msg = (
            f"Missing required parameter(s): {', '.join(repr(p) for p in missing_list)}. "
            f"Query requires: {required_list}, provided: {provided_list}"
        )
        raise KeyError(error_msg)

    # Build positional parameter tuple (with duplicates if param reused)
    positional_params = tuple(param_dict[name] for name in param_names)

    # Replace %(name)s with ? using simple string replacement
    # We replace each unique parameter name to avoid issues with overlapping names
    rewritten_sql = escaped_sql
    for param_name in set(param_names):  # Use set to avoid duplicate replacements
        pattern = f"%({param_name})s"
        rewritten_sql = rewritten_sql.replace(pattern, "?")

    # Restore escaped %% back to %
    rewritten_sql = rewritten_sql.replace("\x00ESCAPED_PERCENT\x00", "%")

    logger.debug(
        "Converted pyformat to qmark: params=%s, positional=%s",
        list(param_dict.keys()),
        positional_params,
    )

    return rewritten_sql, positional_params


def detect_and_convert_parameters(
    sql: str, parameters: Union[None, Tuple, List, Dict]
) -> Tuple[str, Union[None, Tuple, List]]:
    """
    Auto-detect parameter style and convert to qmark if needed.

    Detects parameter style based on the type of parameters:
    - None: No parameters
    - Tuple/List: qmark style (?) - pass through unchanged
    - Dict: pyformat style (%(name)s) - convert to qmark

    Args:
        sql: SQL query string
        parameters: Parameters in any supported format

    Returns:
        Tuple of (sql, parameters) where parameters are in qmark format

    Raises:
        TypeError: If parameters type doesn't match placeholders in SQL
        KeyError: If required pyformat parameter is missing

    Examples:
        >>> detect_and_convert_parameters(
        ...     "SELECT * FROM users WHERE id = ?",
        ...     (42,)
        ... )
        ("SELECT * FROM users WHERE id = ?", (42,))

        >>> detect_and_convert_parameters(
        ...     "SELECT * FROM users WHERE id = %(id)s",
        ...     {"id": 42}
        ... )
        ("SELECT * FROM users WHERE id = ?", (42,))
    """
    # No parameters
    if parameters is None:
        return sql, None

    # Qmark style - tuple or list
    if isinstance(parameters, (tuple, list)):
        # Check if SQL appears to have pyformat placeholders
        if "%()" in sql or ")s" in sql:  # Quick heuristic
            param_names = parse_pyformat_params(sql)
            if param_names:
                # SQL has %(name)s but user passed tuple/list
                raise TypeError(
                    f"Parameter style mismatch: query uses named placeholders (%(name)s), "
                    f"but {type(parameters).__name__} was provided. "
                    f"Use dict for named parameters. Example: "
                    f'cursor.execute(sql, {{"param1": value1, "param2": value2}})'
                )

        # Valid qmark style - pass through
        return sql, parameters

    # Pyformat style - dict
    if isinstance(parameters, dict):
        # Check if SQL appears to have qmark placeholders
        if "?" in sql and not parse_pyformat_params(sql):
            # SQL has ? but user passed dict and no %(name)s found
            raise TypeError(
                f"Parameter style mismatch: query uses positional placeholders (?), "
                f"but dict was provided. "
                f"Use tuple/list for positional parameters. Example: "
                f"cursor.execute(sql, (value1, value2))"
            )

        # Convert pyformat to qmark
        converted_sql, qmark_params = convert_pyformat_to_qmark(sql, parameters)
        return converted_sql, qmark_params

    # Unsupported type
    raise TypeError(
        f"Parameters must be tuple, list, dict, or None. " f"Got {type(parameters).__name__}"
    )
