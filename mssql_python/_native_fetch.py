# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""Cold-path policy for the experimental native cursor fetch entry points.

The native plan is invalidated by public CPython watchers, not private dict
versions. Eligibility checks run after invalidation, never for each warm row.
Unsupported interpreters keep the ordinary Python methods.

Runtime schema 2 keeps the original six tuple fields and appends the GC-visible
dispatch context. Cursor-owned state is lazy and visible at _native_fetch_state;
it is not a public API or a change to constructor initialization.
"""

import sys
from types import FunctionType, GetSetDescriptorType

from mssql_python import ddbc_bindings, perf_timer


class _Policy:
    def __init__(self, cursor_type, row_type, namespace, fallbacks):
        self.cursor_type = cursor_type
        self.row_type = row_type
        self.namespace = namespace
        self.observer = ddbc_bindings._fetch_observer
        self._conversion_ineligible = ddbc_bindings._fetch_conversion_ineligible
        self.logger = namespace["logger"]
        # Normal binding and warning initialization must not invalidate this snapshot.
        cursor_type._get_sql_to_c_type_map()
        namespace.setdefault("__warningregistry__", {})
        self.classes = tuple(
            dict.fromkeys((*cursor_type.__mro__, *row_type.__mro__, *type(self.logger).__mro__))
        )
        self.class_state = [(cls, dict(vars(cls))) for cls in self.classes]
        self.functions = []
        functions = [*fallbacks, namespace["check_error"]]
        for _, attributes in self.class_state:
            for value in attributes.values():
                if isinstance(value, (staticmethod, classmethod)):
                    functions.append(value.__func__)
                elif isinstance(value, property):
                    functions.extend((value.fget, value.fset, value.fdel))
                else:
                    functions.append(value)
        for function in functions:
            if isinstance(function, FunctionType):
                self.functions.append(
                    (
                        function,
                        function.__code__,
                        function.__defaults__,
                        function.__kwdefaults__,
                        dict(function.__kwdefaults__ or {}),
                    )
                )
        self.globals = [
            (namespace, name, namespace[name])
            for name in (
                "Row",
                "check_error",
                "perf_phase",
                "ddbc_bindings",
                "get_settings",
                "_SQL_HANDLE_STMT",
                "_SQL_NO_DATA",
            )
        ]
        self.globals.extend(
            (vars(ddbc_bindings), name, getattr(ddbc_bindings, name))
            for name in ("DDBCSQLFetchOne", "DDBCSQLFetchMany", "construct_rows")
        )
        self.globals.append((row_type._fast_create.__globals__, "Row", row_type))
        self.namespaces = [
            (scope, {key: value for key, value in scope.items() if key != "_NATIVE_FETCH_RUNTIME"})
            for scope in (namespace, row_type._fast_create.__globals__)
        ]
        self.dicts = tuple(
            [
                namespace,
                vars(ddbc_bindings),
                vars(perf_timer),
                vars(self.logger),
                row_type._fast_create.__globals__,
            ]
            + [kwdefaults for _, _, _, kwdefaults, _ in self.functions if kwdefaults is not None]
        )
        ddbc_bindings._watch_fetch_dependencies(
            self.observer, self.dicts, self.classes + (type(self.logger),)
        )

    def __call__(self, cursor, data):
        # Watch even an ineligible cursor so restoring its state can re-enable it.
        connection = data.get("_connection")
        if type(cursor) is not self.cursor_type or connection is None:
            return None
        from mssql_python.connection import Connection

        if type(connection) is not Connection:
            return None
        descriptor = vars(Connection).get("__dict__")
        if (
            Connection.__getattribute__ is not object.__getattribute__
            or type(descriptor) is not GetSetDescriptorType
            # CPython 3.15 shares the ordinary instance-dict descriptor on object.
            or descriptor.__objclass__ not in (Connection, object)
            or descriptor.__name__ != "__dict__"
        ):
            return None
        connection_data = object.__getattribute__(connection, "__dict__")
        ddbc_bindings._watch_fetch_dependencies(
            self.observer,
            (data, connection_data),
            Connection.__mro__,
        )
        index = data.get("_next_row_index")
        if type(index) is not int or index < 1:
            return None
        if self._conversion_ineligible(data):
            return None
        for cls, original in self.class_state:
            current = vars(cls)
            if len(current) != len(original) or any(
                name not in current or current[name] is not value
                for name, value in original.items()
            ):
                return None
        for function, code, defaults, kwdefaults, keywords in self.functions:
            current_keywords = kwdefaults or {}
            if (
                function.__code__ is not code
                or function.__defaults__ is not defaults
                or function.__kwdefaults__ is not kwdefaults
                or len(current_keywords) != len(keywords)
                or any(
                    name not in current_keywords or current_keywords[name] is not value
                    for name, value in keywords.items()
                )
            ):
                return None
        if any(namespace.get(name) is not value for namespace, name, value in self.globals):
            return None
        for scope, original in self.namespaces:
            current = {key: value for key, value in scope.items() if key != "_NATIVE_FETCH_RUNTIME"}
            if len(current) != len(original) or any(
                name not in current or current[name] is not value
                for name, value in original.items()
            ):
                return None
        if self.logger.is_debug_enabled:
            return None
        if (
            self.row_type.__new__ is not object.__new__
            or self.row_type.__setattr__ is not object.__setattr__
            or any(
                name in vars(base)
                for base in Connection.__mro__
                for name in ("_decoding_generation", "_converters_generation")
            )
        ):
            return None
        if any(name in data for name in vars(self.cursor_type)):
            return None
        for cached, generation in (
            ("_cached_decoding_generation", "_decoding_generation"),
            ("_cached_converters_generation", "_converters_generation"),
        ):
            if (
                type(data.get(cached)) is not int
                or type(connection_data.get(generation)) is not int
            ):
                return None
        if (
            data.get("closed") is not False
            or data.get("_has_result_set") is not True
            or data.get("_skip_increment_for_next_fetch") is not False
            or data.get("_cached_decoding_generation")
            != connection_data.get("_decoding_generation")
            or data.get("_cached_converters_generation")
            != connection_data.get("_converters_generation")
            or type(data.get("_cached_converter_map")) is not tuple
            or data.get("_cached_converter_map") != ()
            or data.get("_uuid_str_indices") is not None
        ):
            return None
        description = data.get("description")
        column_map = data.get("_cached_column_map")
        lower_map = data.get("_cached_column_map_lower")
        names = data.get("_cached_result_columns")
        ddbc_bindings._watch_fetch_dependencies(
            self.observer,
            tuple(mapping for mapping in (column_map, lower_map) if type(mapping) is dict),
            (),
        )
        if (
            type(data) is not dict
            or type(connection_data) is not dict
            or type(description) is not list
            or not description
            or data.get("_result_columns_src") is not description
            or type(column_map) is not dict
            or not column_map
            or (lower_map is not None and type(lower_map) is not dict)
            or type(names) is not tuple
            or type(index) is not int
            or not 0 <= index < sys.maxsize - 1
            or type(data.get("_rownumber")) is not int
            or type(data.get("rowcount")) is not int
            or data["_rownumber"] != index - 1
            or data["rowcount"] != index
            or type(data.get("messages")) is not list
            or type(data.get("_cached_char_encoding")) is not str
            or type(data.get("_cached_wchar_encoding")) is not str
            or type(data.get("_cached_char_ctype")) is not int
            or type(data.get("arraysize")) is not int
            or type(data.get("hstmt")) is not ddbc_bindings.SqlHandle
        ):
            return None
        return (
            data["hstmt"],
            data,
            connection,
            connection_data,
            column_map,
            lower_map,
            names,
            self.row_type,
            description,
            data["messages"],
            index,
            data["_cached_char_encoding"],
            data["_cached_wchar_encoding"],
            data["_cached_char_ctype"],
            data["arraysize"],
            self.row_type._fast_create,
            type(connection),
        )


class _InstallRollbackError(RuntimeError):
    def __init__(self, installation_error, cleanup_errors):
        self.installation_error = installation_error
        self.cleanup_errors = tuple(cleanup_errors)
        super().__init__(
            "Native fetch installation and rollback both failed",
            installation_error,
            self.cleanup_errors,
        )


def install(cursor_type, row_type, namespace):
    observer = ddbc_bindings._fetch_observer
    if observer is None or not isinstance(getattr(row_type, "_fast_create", None), FunctionType):
        return None
    methods = ("fetchone", "fetchmany", "fetchval", "__next__")
    fallbacks = tuple(getattr(cursor_type, name) for name in methods)
    entries = ddbc_bindings._native_fetch_methods
    originals = {name: vars(cursor_type)[name] for name in methods}
    begin = ddbc_bindings._begin_native_fetch_install
    activate = ddbc_bindings._activate_native_fetch_install
    abort = ddbc_bindings._abort_native_fetch_install
    disable = ddbc_bindings._disable_native_fetch_install
    context = ddbc_bindings._native_fetch_context
    begin(cursor_type, fallbacks)
    try:
        for name, entry in zip(methods, entries):
            setattr(cursor_type, name, entry)
        policy = _Policy(cursor_type, row_type, namespace, fallbacks)
        runtime = (
            observer,
            cursor_type,
            policy,
            fallbacks,
            ("_values", "_column_map", "_cursor", "_column_map_lower", "_column_names"),
            ("_next_row_index", "_rownumber", "rowcount"),
            context,
        )
        if any(vars(cursor_type).get(name) is not entry for name, entry in zip(methods, entries)):
            raise RuntimeError("Cursor methods changed during native fetch installation")
        activate(runtime)
        return runtime
    except BaseException as installation_error:
        # Unpublished entries can only forward to originals until activation.
        cleanup_errors = []
        for name, entry in zip(methods, entries):
            try:
                if vars(cursor_type).get(name) is entry:
                    setattr(cursor_type, name, originals[name])
            except BaseException as cleanup_error:
                cleanup_errors.append((name, cleanup_error))
        survivors = tuple(
            name for name, entry in zip(methods, entries) if vars(cursor_type).get(name) is entry
        )
        if survivors and not cleanup_errors:
            cleanup_errors.append(
                (
                    "descriptors",
                    RuntimeError("Native fetch descriptors survived rollback", survivors),
                )
            )
        # Retained descriptors must retain their forwarding functions. A partial
        # rollback is terminal, not a context that a later install may reuse.
        terminalize = disable if cleanup_errors else abort
        try:
            terminalize()
        except BaseException as cleanup_error:
            cleanup_errors.append(("context", cleanup_error))
        if cleanup_errors:
            raise _InstallRollbackError(installation_error, cleanup_errors) from installation_error
        raise
