"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Parameter style conversion helpers for mssql-python.

Supports both qmark (?) and pyformat (%(name)s) parameter styles.
Includes context-aware scanning for qmark and pyformat detection,
skipping characters inside bracketed identifiers, string literals,
quoted identifiers, and SQL comments.

Reference: https://www.python.org/dev/peps/pep-0249/#paramstyle
"""

from functools import lru_cache
from typing import Dict, List, Tuple, Any, Union
from mssql_python.logging import logger

# Number of pyformat -> qmark rewrites kept, keyed by SQL text. The cache matters
# most for large statements re-executed in batches (an ORM's multi-row INSERT is
# ~30 KB of SQL with ~2,000 parameters, about 125 KB per entry), so it is kept
# small: worst case a few MB. Small statements are cheap to rescan on a miss.
_PYFORMAT_CACHE_SIZE = 32


def _skip_quoted_context(sql: str, i: int, length: int) -> int:
    """
    If position i starts a SQL quoted context, skip past it and return the new position.
    Returns -1 if no quoted context starts at position i.

    Handles:
    - Single-line comments: -- ... (to end of line)
    - Multi-line comments: /* ... */ (to closing delimiter)
    - Single-quoted string literals: '...' (with '' escape handling)
    - Double-quoted identifiers: "..."
    - Bracketed identifiers: [...]

    Args:
        sql: Full SQL query string
        i: Current scan position
        length: Length of sql (len(sql))

    Returns:
        New position after the quoted context, or -1 if position i
        does not start a quoted context.
    """
    ch = sql[i]

    # Single-line comment: skip to end of line
    if ch == "-" and i + 1 < length and sql[i + 1] == "-":
        i += 2
        while i < length and sql[i] != "\n":
            i += 1
        return i

    # Multi-line comment: skip to closing */
    # SQL Server supports nested block comments, so we track nesting depth.
    if ch == "/" and i + 1 < length and sql[i + 1] == "*":
        i += 2
        depth = 1
        while i < length and depth > 0:
            if i + 1 < length and sql[i] == "/" and sql[i + 1] == "*":
                depth += 1
                i += 2
            elif i + 1 < length and sql[i] == "*" and sql[i + 1] == "/":
                depth -= 1
                i += 2
            else:
                i += 1
        return min(i, length)  # already past final */, or at end if unterminated

    # Single-quoted string literal: skip to closing '
    # Handles escaped quotes ('') inside strings
    if ch == "'":
        i += 1
        while i < length:
            if sql[i] == "'":
                if i + 1 < length and sql[i + 1] == "'":
                    i += 2  # skip escaped quote
                    continue
                break
            i += 1
        return min(i + 1, length)  # skip closing quote

    # Double-quoted identifier: skip to closing "
    # Handles escaped quotes ("") inside identifiers
    if ch == '"':
        i += 1
        while i < length:
            if sql[i] == '"':
                if i + 1 < length and sql[i + 1] == '"':
                    i += 2  # skip escaped quote
                    continue
                break
            i += 1
        return min(i + 1, length)  # skip closing quote

    # Bracketed identifier: skip to closing ]
    # Handles escaped brackets (]]) inside identifiers
    if ch == "[":
        i += 1
        while i < length:
            if sql[i] == "]":
                if i + 1 < length and sql[i + 1] == "]":
                    i += 2  # skip escaped bracket
                    continue
                break
            i += 1
        return min(i + 1, length)  # skip closing bracket

    return -1


def _has_unquoted_question_marks(sql: str) -> bool:
    """
    Check if SQL contains ? characters that are actual qmark parameter placeholders.

    Uses _skip_quoted_context to skip ? characters that appear inside
    bracketed identifiers, string literals, quoted identifiers, and comments.

    Args:
        sql: SQL query string to check

    Returns:
        True if the SQL contains at least one unquoted/unbracketed ? character

    Examples:
        >>> _has_unquoted_question_marks("SELECT * FROM t WHERE id = ?")
        True

        >>> _has_unquoted_question_marks("SELECT [q?marks] FROM t")
        False

        >>> _has_unquoted_question_marks("SELECT 'what?' FROM t")
        False
    """
    i = 0
    length = len(sql)

    while i < length:
        # Skip any quoted context (brackets, strings, comments)
        skipped = _skip_quoted_context(sql, i, length)
        if skipped >= 0:
            i = skipped
            continue

        # Unquoted question mark — this is a real placeholder
        if sql[i] == "?":
            return True

        i += 1

    return False


def parse_pyformat_params(sql: str) -> List[str]:
    """
    Extract %(name)s parameter names from SQL string.

    Uses context-aware scanning to skip %(name)s patterns inside SQL
    string literals, quoted identifiers, bracketed identifiers, and comments.
    Only %(name)s patterns in executable SQL are detected as parameters.

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
    logger.debug(
        "parse_pyformat_params: Starting parse - sql_length=%d, sql_preview=%s",
        len(sql),
        sql[:100] if len(sql) > 100 else sql,
    )
    spans = _scan_pyformat(sql)
    params = [name for name, _, _ in spans]

    if logger.is_debug_enabled:
        for name, start, _ in spans:
            logger.debug(
                "parse_pyformat_params: Found parameter '%s' at position %d",
                name,
                start,
            )
        logger.debug(
            "parse_pyformat_params: Completed - found %d parameters: %s",
            len(params),
            params,
        )
    return params


def _scan_pyformat(sql: str) -> List[Tuple[str, int, int]]:
    """
    Locate executable %(name)s placeholders in a SQL string.

    Uses the same context-aware scanning as parse_pyformat_params(): patterns inside
    string literals, quoted identifiers, bracketed identifiers, and comments are skipped,
    and "%%" (an escaped literal percent) never starts a placeholder.

    Returns:
        List of (name, start, end) per placeholder in order of appearance, where
        sql[start:end] is the full "%(name)s" text.
    """
    spans = []
    i = 0
    length = len(sql)

    while i < length:
        # Skip any quoted context (brackets, strings, comments)
        skipped = _skip_quoted_context(sql, i, length)
        if skipped >= 0:
            i = skipped
            continue

        # "%%" is an escaped literal percent, never the start of a placeholder
        if sql[i] == "%" and i + 1 < length and sql[i + 1] == "%":
            i += 2
            continue

        # Look for %(
        if i + 2 < length and sql[i] == "%" and sql[i + 1] == "(":
            # Find the closing )
            j = i + 2
            while j < length and sql[j] != ")":
                j += 1

            # Check if we found ) and it's followed by 's'
            if j < length and sql[j] == ")":
                if j + 1 < length and sql[j + 1] == "s":
                    spans.append((sql[i + 2 : j], i, j + 2))
                    i = j + 2
                    continue

        i += 1

    return spans


@lru_cache(maxsize=_PYFORMAT_CACHE_SIZE)
def _compile_pyformat(sql: str) -> Tuple[str, Tuple[str, ...]]:
    """
    Rewrite %(name)s placeholders in executable SQL to ? in a single pass.

    Placeholders inside string literals, quoted/bracketed identifiers, and comments
    are left untouched, and each %% (escaped percent) becomes a literal %, wherever
    it appears. The result depends only on the SQL text, so it is cached: re-executing
    the same statement (for example every batch of a multi-row INSERT) skips the scan.

    Returns:
        (qmark_sql, parameter names in placeholder order, with duplicates if reused)
    """
    spans = _scan_pyformat(sql)

    # The scanner never starts a placeholder inside a %% pair, so un-escaping each
    # segment between placeholders pairs the % signs exactly as a whole-string pass would
    parts = []
    last = 0
    for _, start, end in spans:
        parts.append(sql[last:start].replace("%%", "%"))
        parts.append("?")
        last = end
    parts.append(sql[last:].replace("%%", "%"))
    rewritten_sql = "".join(parts)

    return rewritten_sql, tuple(name for name, _, _ in spans)


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
    if logger.is_debug_enabled:
        logger.debug(
            "convert_pyformat_to_qmark: Starting conversion - sql_length=%d, param_count=%d",
            len(sql),
            len(param_dict),
        )
        logger.debug(
            "convert_pyformat_to_qmark: SQL preview: %s",
            sql[:200] if len(sql) > 200 else sql,
        )
        logger.debug(
            "convert_pyformat_to_qmark: Parameters provided: %s",
            list(param_dict.keys()),
        )
        # Support %% escaping - %% is a literal % in the SQL, not a placeholder
        if "%%" in sql:
            logger.debug(
                "convert_pyformat_to_qmark: Detected %d escaped percent sequences (%%%%)",
                sql.count("%%"),
            )

    # Rewrite placeholders to ? and extract parameter names in order (cached per SQL text)
    rewritten_sql, param_names = _compile_pyformat(sql)

    if not param_names:
        logger.debug(
            "convert_pyformat_to_qmark: No pyformat parameters found - returning SQL as-is"
        )
        return rewritten_sql, ()

    if logger.is_debug_enabled:
        logger.debug(
            "convert_pyformat_to_qmark: Extracted %d parameter references (with duplicates): %s",
            len(param_names),
            list(param_names),
        )
        logger.debug(
            "convert_pyformat_to_qmark: Unique parameters needed: %s",
            sorted(set(param_names)),
        )

    # Validate all required parameters are present (membership, not lookup, so a
    # dict subclass with __missing__ such as defaultdict can't fill them in)
    missing = {name for name in param_names if name not in param_dict}
    if missing:
        # Provide helpful error message
        missing_list = sorted(missing)
        required_list = sorted(set(param_names))
        provided_list = sorted(param_dict.keys())

        logger.error(
            "convert_pyformat_to_qmark: Missing parameters - required=%s, provided=%s, missing=%s",
            required_list,
            provided_list,
            missing_list,
        )

        error_msg = (
            f"Missing required parameter(s): {', '.join(repr(p) for p in missing_list)}. "
            f"Query requires: {required_list}, provided: {provided_list}"
        )
        raise KeyError(error_msg)

    # Build positional parameter tuple (with duplicates if param reused)
    positional_params = tuple(param_dict[name] for name in param_names)

    if logger.is_debug_enabled:
        logger.debug(
            "convert_pyformat_to_qmark: Built positional params tuple - length=%d",
            len(positional_params),
        )
        logger.debug(
            "convert_pyformat_to_qmark: Conversion complete - result_sql_length=%d, param_count=%d",
            len(rewritten_sql),
            len(positional_params),
        )
        logger.debug(
            "convert_pyformat_to_qmark: Result SQL preview: %s",
            rewritten_sql[:200] if len(rewritten_sql) > 200 else rewritten_sql,
        )
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
    logger.debug(
        "detect_and_convert_parameters: Starting - sql_length=%d, parameters_type=%s",
        len(sql),
        type(parameters).__name__ if parameters is not None else "None",
    )

    # No parameters
    if parameters is None:
        logger.debug("detect_and_convert_parameters: No parameters provided - returning as-is")
        return sql, None

    # Qmark style - tuple or list
    if isinstance(parameters, (tuple, list)):
        logger.debug(
            "detect_and_convert_parameters: Detected qmark-style parameters (%s) - count=%d",
            type(parameters).__name__,
            len(parameters),
        )

        # Check if SQL has pyformat placeholders (every placeholder contains "%(")
        param_names = parse_pyformat_params(sql) if "%(" in sql else []
        if param_names:
            logger.error(
                "detect_and_convert_parameters: Parameter style mismatch - SQL has pyformat placeholders %s but received %s",
                param_names,
                type(parameters).__name__,
            )
            # SQL has %(name)s but user passed tuple/list
            raise TypeError(
                f"Parameter style mismatch: query uses named placeholders (%(name)s), "
                f"but {type(parameters).__name__} was provided. "
                f"Use dict for named parameters. Example: "
                f'cursor.execute(sql, {{"param1": value1, "param2": value2}})'
            )

        # Valid qmark style - pass through
        logger.debug("detect_and_convert_parameters: Valid qmark style - passing through unchanged")
        return sql, parameters

    # Pyformat style - dict
    if isinstance(parameters, dict):
        logger.debug(
            "detect_and_convert_parameters: Detected pyformat-style parameters (dict) - count=%d, keys=%s",
            len(parameters),
            list(parameters.keys()),
        )

        # Check if SQL appears to have qmark placeholders
        # Fast short-circuit: skip the O(n) context-aware scan if no ? exists at all
        # Then use context-aware check that ignores ? inside brackets, quotes, and comments
        if "?" in sql and _has_unquoted_question_marks(sql) and not parse_pyformat_params(sql):
            logger.error(
                "detect_and_convert_parameters: Parameter style mismatch - SQL has ? placeholders but received dict"
            )
            # SQL has ? but user passed dict and no %(name)s found
            raise TypeError(
                f"Parameter style mismatch: query uses positional placeholders (?), "
                f"but dict was provided. "
                f"Use tuple/list for positional parameters. Example: "
                f"cursor.execute(sql, (value1, value2))"
            )

        logger.debug("detect_and_convert_parameters: Valid pyformat style - converting to qmark")
        # Convert pyformat to qmark
        converted_sql, qmark_params = convert_pyformat_to_qmark(sql, parameters)
        logger.debug(
            "detect_and_convert_parameters: Conversion complete - qmark_param_count=%d",
            len(qmark_params) if qmark_params else 0,
        )
        return converted_sql, qmark_params

    # Unsupported type
    logger.error(
        "detect_and_convert_parameters: Unsupported parameter type - %s",
        type(parameters).__name__,
    )
    raise TypeError(
        f"Parameters must be tuple, list, dict, or None. " f"Got {type(parameters).__name__}"
    )
