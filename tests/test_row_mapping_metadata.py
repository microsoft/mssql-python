"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Canonical mapping metadata, live-view and native protocol compatibility guards.
"""

import copy
import gc
import os
from pathlib import Path
import pickle
import subprocess
import sys
from collections.abc import KeysView, Mapping

import pytest

import mssql_python
from mssql_python.row import Row, RowMapping, _ColumnNames, _iter_mapping_keys


class _SerializableRow(Row):
    """Keep mapping serialization independent of uninitialized Row reconstruction."""

    def __reduce_ex__(self, protocol):
        return (
            type(self),
            (
                self._values,
                self._column_map,
                None,
                (),
                None,
                self._column_map_lower,
                self._column_names,
            ),
        )


def make_rows(names, values):
    columns = {name: index for index, name in enumerate(names)}
    return [Row(value, columns, column_names=names) for value in values]


def run_mapping_program(program, *args, debug=False):
    command = [sys.executable]
    if debug:
        command += ["-X", "dev"]
    result = subprocess.run(
        [*command, "-c", program, *args],
        cwd=Path(__file__).resolve().parents[1],
        env=dict(os.environ, PYTHONMALLOC="debug") if debug else None,
        capture_output=True,
        text=True,
        timeout=60 if debug else 30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_mapping_deduplicates_once_for_all_rows():
    names = _ColumnNames(("a", "b", "a"))
    rows = make_rows(names, [[1, 2, 3]] * 20)
    assert names._unique is None
    references = [tuple(map(id, gc.get_referents(row))) for row in rows]
    builds = []
    previous = sys.getprofile()

    def record(frame, event, function):
        if event == "c_call" and getattr(function, "__name__", None) == "fromkeys":
            builds.append(function)

    try:
        sys.setprofile(record)
        results = [dict(row._mapping) for row in rows]
        assert names._unique_keys() is names._unique
    finally:
        sys.setprofile(previous)
    assert results == [{"a": 3, "b": 2}] * 20
    assert len(builds) == 1
    for row, before in zip(rows, references):
        after = tuple(map(id, gc.get_referents(row)))
        if after != before:
            # Preparation may materialize the standard dict, but stores no cache on Row.
            attributes = vars(row)
            assert attributes == {}
            assert sorted(after) == sorted((*before, id(attributes)))


def test_mapping_does_not_inspect_names_until_first_use(monkeypatch):
    calls = []

    def iterate(names):
        calls.append(names)
        return tuple.__iter__(names)

    monkeypatch.setattr(_ColumnNames, "__iter__", iterate, raising=False)
    names = _ColumnNames(("a", "b"))
    row = Row([1, 2], {"a": 0, "b": 1}, column_names=names)
    assert tuple(row) == (1, 2)
    assert calls == []
    assert names._unique is None
    assert row._mapping["b"] == 2
    assert calls
    assert names._unique == {"a": None, "b": None}


def test_mapping_native_slots_bypass_default_per_cell_python_hooks():
    program = """
import sys
import pytest
from mssql_python.row import Row, RowMapping, _ColumnNames

names = _ColumnNames("c" + str(index) for index in range(128))
columns = {name: index for index, name in enumerate(names)}
rows = [Row(list(range(128)), columns, column_names=names) for _ in range(10)]
views = [row._mapping for row in rows]
calls = []
codes = {
    method.__code__ for method in (Row._mapping_keys, Row.__getitem__)
    if hasattr(method, "__code__")
}
def record(frame, event, argument):
    if event == "call" and frame.f_code in codes:
        calls.append(frame.f_code.co_name)
sys.setprofile(record)
try:
    results = [dict(view) for view in views]
finally:
    sys.setprofile(None)
assert results == [columns] * 10
assert calls == [], calls
if RowMapping._prepared_supported:
    for index, view in enumerate(views):
        assert view._prepared_counts == (1, int(index == 0), 128, 0, 128), view._prepared_counts
    import copy
    import pickle
    for restored in (copy.deepcopy(names), pickle.loads(pickle.dumps(names))):
        assert tuple(restored) == tuple(names)
        assert restored._unique == names._unique
        assert restored._prepared is None

    empty = Row([], {}, column_names=_ColumnNames(()))._mapping
    assert dict(empty) == {}
    assert empty._prepared_counts == (1, 1, 0, 0, 0), empty._prepared_counts

    pair = Row([10, 20], {"a": 0, "b": 1}, column_names=_ColumnNames(("a", "b")))
    mapping = pair._mapping
    first, second = iter(mapping), iter(mapping)
    assert next(first) == "a"
    assert mapping["b"] == 20
    assert mapping["a"] == 10
    assert mapping["a"] == 10
    assert next(second) == "a"
    assert mapping["b"] == 20
    assert mapping["a"] == 10
    assert next(first) == "b"
    assert mapping["b"] == 20
    assert list(second) == ["b"]
    assert list(first) == []
    assert mapping._prepared_counts == (2, 1, 6, 0, 3), mapping._prepared_counts

    pair._column_map["a"] = -1
    pair._column_names = _ColumnNames(("a",))
    pending = iter(mapping)
    assert next(pending) == "a"
    ordered = mapping._prepared_counts[4]
    pair._values = (30, 40, 50)
    assert mapping["a"] == 50
    assert mapping._prepared_counts[4] == ordered + 1
    assert list(pending) == []

    def same_index_error():
        with pytest.raises(IndexError) as expected:
            pair["a"]
        before = mapping._prepared_counts
        with pytest.raises(IndexError) as actual:
            mapping["a"]
        assert actual.value.args == expected.value.args
        after = mapping._prepared_counts
        assert after[2] == before[2] + 1
        assert after[3:] == before[3:], (before, after)

    pending = iter(mapping)
    assert next(pending) == "a"
    pair._values = []
    same_index_error()
    assert list(pending) == []
    pair._values = [60]
    for index in (2**100, -sys.maxsize - 1):
        pair._column_map["a"] = index
        assert list(iter(mapping)) == ["a"]
        same_index_error()
"""
    run_mapping_program(program)


@pytest.mark.parametrize(
    "change",
    (
        "names",
        "columns",
        "values",
        "getattribute",
        "view_row",
        "deleted_names",
        "deleted_values",
        "equality_descriptor",
        "instance_dictionary",
        "instance_dictionary_replaced",
        "names_dictionary_replaced",
        "canonical_keys",
        "unique_replaced",
        "map_modified",
        "map_deleted",
        "index_callback",
        "key_hook_code",
        "getitem_code",
    ),
)
def test_mapping_field_guards_in_fresh_process(change):
    program = """
import sys
import pytest
from mssql_python.row import Row, RowMapping, _ColumnNames

row = Row([1, 2], {"a": 0, "b": 1}, column_names=_ColumnNames(("a", "b")))
mapping = row._mapping
assert dict(mapping) == {"a": 1, "b": 2}
if RowMapping._prepared_supported:
    assert mapping._prepared_counts == (1, 1, 2, 0, 2), mapping._prepared_counts
change = sys.argv[1]
if change == "names":
    Row._column_names = property(lambda self: ("b",))
    assert dict(mapping) == {"b": 2}
elif change == "columns":
    calls = []
    class Columns(dict):
        def __contains__(self, key):
            calls.append(("contains", key))
            return super().__contains__(key)
        def __getitem__(self, key):
            calls.append(("getitem", key))
            return 1 - super().__getitem__(key)
    row._column_map = Columns({"a": 0, "b": 1})
    before = mapping._prepared_counts
    expected = row["a"]
    expected_calls = calls.copy()
    calls.clear()
    assert mapping["a"] == expected == 2
    assert calls == expected_calls == [("contains", "a"), ("getitem", "a")]
    calls.clear()
    assert dict(mapping) == {"a": 2, "b": 1}
    assert calls == expected_calls + [("contains", "b"), ("getitem", "b")]
    assert mapping._prepared_counts == before[:3] + (before[3] + 3, before[4])
    row._column_map = {"a": 0, "b": 1}
    before = mapping._prepared_counts
    assert dict(mapping) == {"a": 1, "b": 2}
    if RowMapping._prepared_supported:
        after = mapping._prepared_counts
        assert after[0] == before[0] + 1
        assert after[2:] == (before[2] + 2, before[3], before[4] + 2)
    Row._column_map = property(lambda self: {"a": 1, "b": 0})
    assert dict(mapping) == {"a": 2, "b": 1}
elif change == "values":
    class Values(list):
        def __getitem__(self, index):
            return super().__getitem__(index) + 10
    row._values = Values([1, 2])
    before = mapping._prepared_counts
    assert mapping["a"] == row["a"] == 11
    assert dict(mapping) == {"a": 11, "b": 12}
    assert mapping._prepared_counts == before[:3] + (before[3] + 3, before[4])
    row._values = [1, 2]
    before = mapping._prepared_counts
    assert dict(mapping) == {"a": 1, "b": 2}
    if RowMapping._prepared_supported:
        after = mapping._prepared_counts
        assert after[0] == before[0] + 1
        assert after[2:] == (before[2] + 2, before[3], before[4] + 2)
    Row._values = property(lambda self: [3, 4])
    assert dict(mapping) == {"a": 3, "b": 4}
elif change == "getattribute":
    def read(self, name):
        if name == "_values":
            return [5, 6]
        return object.__getattribute__(self, name)
    Row.__getattribute__ = read
    assert dict(mapping) == {"a": 5, "b": 6}
elif change == "view_row":
    import gc
    configuration = RowMapping._row_mapping_defaults
    defaults = next(value for value in gc.get_referents(configuration) if type(value) is tuple)
    try:
        RowMapping._row_mapping_defaults = defaults
        assert dict(RowMapping(row)) == {"a": 1, "b": 2}
        RowMapping._row_mapping_defaults = defaults[:-1]
        with pytest.raises(TypeError, match="^Invalid RowMapping configuration$"):
            RowMapping(row)
    finally:
        RowMapping._row_mapping_defaults = configuration
    iterator_type = type(iter(mapping))
    original_next = iterator_type.__next__
    try:
        iterator_type.__next__ = lambda self: "changed"
        with pytest.raises(TypeError, match="^Invalid RowMapping iterator type$"):
            RowMapping(row)
    finally:
        iterator_type.__next__ = original_next
    other = Row([7, 8], {"a": 0, "b": 1}, column_names=row._column_names)
    RowMapping._row = property(lambda self: other)
    assert dict(mapping) == {"a": 7, "b": 8}
    def fail_row(self):
        raise RuntimeError("row descriptor failed")
    RowMapping._row = property(fail_row)
    with pytest.raises(RuntimeError, match="^row descriptor failed$"):
        mapping["a"]
    iterator = iter(mapping)
    with pytest.raises(RuntimeError, match="^row descriptor failed$"):
        next(iterator)
    with pytest.raises(StopIteration):
        next(iterator)
elif change == "deleted_names":
    row._values = [("b",), 9]
    row._column_map = {"_column_names": 0, "b": 1}
    del row._column_names
    assert dict(mapping) == {"b": 9}
elif change == "deleted_values":
    del row._values
    with pytest.raises(AttributeError, match="^Row has no attribute '_values'$"):
        mapping["a"]
elif change == "equality_descriptor":
    class Name(str):
        __hash__ = str.__hash__
        def __eq__(self, other):
            Row._values = property(lambda self: [10, 11])
            return str.__eq__(self, other)
    row._column_map = {Name("a"): 0, "b": 1}
    assert mapping["a"] == 10
elif change in ("instance_dictionary", "instance_dictionary_replaced"):
    if change == "instance_dictionary":
        vars(row)["_mapping_keys"] = lambda: ("b",)
    else:
        class InstanceDict(dict):
            pass
        row.__dict__ = InstanceDict(note="custom")
        assert type(row.__dict__) is InstanceDict
        before = mapping._prepared_counts
        assert mapping["a"] == 1
        assert dict(mapping) == {"a": 1, "b": 2}
        assert mapping._prepared_counts == before[:3] + (before[3] + 3, before[4])
        row.__dict__ = {}
        before = mapping._prepared_counts
        assert dict(mapping) == {"a": 1, "b": 2}
        if RowMapping._prepared_supported:
            after = mapping._prepared_counts
            assert after[0] == before[0] + 1
            assert after[2:] == (before[2] + 2, before[3], before[4] + 2)
        row.__dict__ = {"_mapping_keys": lambda: ("b",)}
    assert "a" not in mapping
    assert mapping["b"] == 2
elif change == "names_dictionary_replaced":
    row._column_names.__dict__ = {"_unique": {"b": None}, "_prepared": None}
    assert "a" not in mapping
    assert mapping["b"] == 2
elif change in ("canonical_keys", "unique_replaced"):
    if change == "canonical_keys":
        del row._column_names._unique["a"]
    else:
        row._column_names._unique = {"b": None}
    assert "a" not in mapping
    assert mapping["b"] == 2
elif change == "map_modified":
    row._column_map["a"] = 1
    assert mapping["a"] == 2
    assert dict(mapping) == {"a": 2, "b": 2}
    if RowMapping._prepared_supported:
        assert mapping._prepared_counts == (2, 2, 4, 1, 4), mapping._prepared_counts
elif change == "map_deleted":
    del row._column_map["a"]
    with pytest.raises(KeyError) as error:
        mapping["a"]
    assert error.value.args == ("Row has no column 'a'",)
elif change == "index_callback":
    calls = []
    class Index:
        def __index__(self):
            calls.append("index")
            return 1
    row._column_map["a"] = Index()
    assert list(iter(mapping)) == ["a", "b"]
    assert calls == [], "Key preparation must not execute __index__"
    assert mapping["a"] == 2
    assert calls == ["index"]
elif change == "key_hook_code":
    Row._mapping_keys.__code__ = (lambda self: ("b",)).__code__
    assert "a" not in mapping
    assert mapping["b"] == 2
elif change == "getitem_code":
    Row.__getitem__.__code__ = (lambda self, key: 42).__code__
    assert mapping["a"] == 42
else:
    raise AssertionError(change)
"""
    run_mapping_program(program, change)


def test_mapping_does_not_search_canonical_tuple(monkeypatch):
    names = _ColumnNames(f"c{index}" for index in range(128))
    row = make_rows(names, [list(range(128))])[0]

    def forbidden(self, key):
        raise AssertionError("canonical lookup searched the tuple")

    monkeypatch.setattr(_ColumnNames, "__contains__", forbidden)
    assert row._mapping["c127"] == 127
    assert "c126" in row._mapping
    assert "missing" not in row._mapping


def test_mapping_keys_remain_a_live_set_like_view():
    row = make_rows(_ColumnNames(("a", "b")), [[1, 2]])[0]
    mapping = row._mapping
    keys = mapping.keys()
    assert isinstance(keys, KeysView)
    assert keys & {"b", "c"} == {"b"}
    assert keys == {"a", "b"}
    row._column_names = _ColumnNames(("b",))
    assert list(keys) == ["b"]
    assert len(keys) == 1
    assert dict(mapping) == {"b": 2}


def test_mapping_iterator_defers_hook_until_first_next():
    row = make_rows(_ColumnNames(("a", "b")), [[1, 2]])[0]
    mapping = row._mapping
    iterator = iter(mapping)
    assert row._column_names._unique is None
    row._column_names = _ColumnNames(("b",))
    assert next(iterator) == "b"
    with pytest.raises(StopIteration):
        next(iterator)
    with pytest.raises(StopIteration):
        next(iterator)


def test_mapping_iterator_failure_closes_the_iterator():
    row = Row([1], {"a": 0})
    calls = []

    def fail():
        calls.append(1)
        raise RuntimeError("key hook failed")

    row._mapping_keys = fail
    iterator = iter(row._mapping)
    assert calls == []
    with pytest.raises(RuntimeError, match="key hook failed"):
        next(iterator)
    with pytest.raises(StopIteration):
        next(iterator)
    assert calls == [1]


def test_mapping_iterator_reentrancy_matches_generator_behavior():
    row = Row([1], {"a": 0})
    iterator = iter(row._mapping)

    def keys():
        with pytest.raises(ValueError, match="already executing"):
            next(iterator)
        return ("a",)

    row._mapping_keys = keys
    assert list(iterator) == ["a"]


def test_mapping_observes_replaced_column_metadata():
    names = _ColumnNames(("a", "b", "a"))
    row = make_rows(names, [[1, 2, 3]])[0]
    mapping = row._mapping
    assert dict(mapping) == {"a": 3, "b": 2}
    row._column_map = {"a": 1, "b": 0, "friendly": 2}
    assert dict(mapping) == {"a": 2, "b": 1}
    row._column_names = ("friendly", "a", "friendly")
    assert dict(mapping) == {"friendly": 3, "a": 2}
    row._column_names = None
    assert list(mapping) == ["b", "a", "friendly"]
    assert dict(mapping) == {"b": 1, "a": 2, "friendly": 3}


def test_mapping_dictionary_equality_can_replace_the_column_map():
    class Name(str):
        __hash__ = str.__hash__

        def __eq__(self, other):
            row._column_map = {"a": 1}
            return str.__eq__(self, other)

    original = {Name("a"): 0}
    row = Row([1, 2], {"a": 0}, column_names=_ColumnNames(("a",)))
    mapping = row._mapping
    assert dict(mapping) == {"a": 1}
    row._column_map = original
    expected = row["a"]
    row._column_map = original
    assert mapping["a"] == expected == 2


def test_mapping_failed_dictionary_membership_is_not_repeated():
    calls = []

    class Name(str):
        def __hash__(self):
            return hash("a")

        def __eq__(self, other):
            calls.append(other)
            row._column_map_lower = {"a": 1}
            return False

    row = Row([1, 2], {Name("different"): 0}, column_names=_ColumnNames(("a",)))
    # One dict membership check can revisit a colliding slot, depending on the hash.
    assert "a" not in row._column_map
    expected_calls = calls.copy()
    assert expected_calls
    calls.clear()
    row._column_map_lower = None
    assert row._mapping["a"] == 2
    assert calls == expected_calls


def test_mapping_invalid_keys_keep_keyerror_arguments():
    mapping = make_rows(_ColumnNames(("a",)), [[1]])[0]._mapping
    for key in ([], ("a", "b")):
        with pytest.raises(KeyError) as error:
            mapping[key]
        assert error.value.args == (key,)
        assert key not in mapping
        assert mapping.get(key, "absent") == "absent"


def test_mapping_string_subclass_retains_tuple_membership():
    class UnhashableName(str):
        __hash__ = None

    class HookRow(Row):
        def __getitem__(self, key):
            return 99

    row = HookRow([1], {"a": 0}, column_names=_ColumnNames(("a",)))
    assert row._mapping[UnhashableName("a")] == 99
    assert UnhashableName("missing") not in row._mapping


def test_mapping_custom_canonical_string_does_not_introduce_hashing():
    class UnhashableName(str):
        __hash__ = None

    names = _ColumnNames((UnhashableName("a"),))
    assert names._unique is None
    row = Row([1], {"a": 0}, column_names=names)
    assert row._mapping["a"] == 1
    assert names._unique is False
    with pytest.raises(TypeError):
        list(row._mapping)


@pytest.mark.parametrize("placement", ("subclass", "class"))
def test_mapping_observes_key_hooks_after_view_creation(placement, monkeypatch):
    class CustomRow(Row):
        pass

    cls = CustomRow if placement == "subclass" else Row
    row = cls([1, 2], {"a": 0, "b": 1}, column_names=_ColumnNames(("a", "b")))
    mapping = row._mapping
    assert dict(mapping) == {"a": 1, "b": 2}
    monkeypatch.setattr(cls, "_mapping_keys", lambda self: ("b", "b"))
    assert dict(mapping) == {"b": 2}
    assert "a" not in mapping
    assert mapping["b"] == 2


@pytest.mark.parametrize("placement", ("subclass", "class"))
def test_mapping_observes_getitem_hooks_after_view_creation(placement, monkeypatch):
    class CustomRow(Row):
        pass

    cls = CustomRow if placement == "subclass" else Row
    row = cls([1], {"a": 0}, column_names=_ColumnNames(("a",)))
    mapping = row._mapping
    assert mapping["a"] == 1
    monkeypatch.setattr(cls, "__getitem__", lambda self, key: ("hook", key))
    assert mapping["a"] == ("hook", "a")
    assert dict(mapping) == {"a": ("hook", "a")}
    assert "missing" not in mapping


@pytest.mark.parametrize("lazy_values", (False, True))
def test_mapping_configured_row_uses_inherited_member_descriptors(lazy_values):
    calls = []

    class Storage:
        __slots__ = ("_values", "_column_map", "_column_names", "_column_map_lower", "__dict__")

    class InheritedRow(Storage):
        __slots__ = ()
        __getitem__ = Row.__getitem__
        _mapping_keys = Row._mapping_keys

        if lazy_values:

            @property
            def _values(self):
                calls.append("values")
                return self.values

            @_values.setter
            def _values(self, values):
                self.values = values

    class InheritedMapping(
        mssql_python.ddbc_bindings._make_row_mapping_base(
            InheritedRow, _ColumnNames, _iter_mapping_keys
        ),
        Mapping,
    ):
        __slots__ = ()
        __init__ = RowMapping.__init__
        __len__ = RowMapping.__len__
        keys = RowMapping.keys

    row = InheritedRow()
    row._values = [1, 2]
    row._column_map = {"a": 0, "b": 1}
    row._column_map_lower = None
    row._column_names = _ColumnNames(("a", "b"))
    mapping = InheritedMapping(row)
    assert list(iter(mapping)) == ["a", "b"]
    assert calls == []
    if lazy_values:
        assert mapping._prepared_counts[:3] == (0, 0, 0)
    assert dict(mapping) == {"a": 1, "b": 2}
    row._values = [3, 4]
    assert dict(mapping) == {"a": 3, "b": 4}
    row._mapping_keys = lambda: ("b",)
    assert dict(mapping) == {"b": 4}


def test_mapping_key_descriptor_changes_and_errors_remain_live():
    calls = []

    class KeyHook:
        def __get__(self, row, cls):
            if row is None:
                return self
            calls.append(row)
            if row.fail:
                raise RuntimeError("key descriptor failed")
            return lambda: row.names

    class CustomRow(Row):
        _mapping_keys = KeyHook()

    row = CustomRow([1, 2], {"a": 0, "b": 1})
    row.names = ("a",)
    row.fail = False
    mapping = row._mapping
    assert mapping["a"] == 1
    assert len(calls) == 1
    row.names = ("b",)
    assert mapping["b"] == 2
    assert "a" not in mapping
    row.fail = True
    with pytest.raises(RuntimeError, match="key descriptor failed"):
        mapping["b"]


def test_mapping_key_hook_can_replace_the_view_row():
    first, second = make_rows(_ColumnNames(("a",)), [[1], [2]])
    mapping = first._mapping

    def redirect():
        mapping._row = second
        return ("a",)

    first._mapping_keys = redirect
    assert mapping["a"] == 2
    assert mapping._row is second


def test_mapping_isinstance_string_compatibility():
    class StringKey:
        __class__ = str

        def __eq__(self, other):
            return other == "a"

    class CustomRow(Row):
        def __getitem__(self, key):
            return 42

    row = CustomRow([1], {"a": 0}, column_names=_ColumnNames(("a",)))
    assert row._mapping[StringKey()] == 42


def test_mapping_row_descriptor_and_subclass_iteration():
    class CustomMapping(RowMapping):
        def __init__(self, row):
            self._row = row

        @property
        def _row(self):
            return self.current

        @_row.setter
        def _row(self, row):
            self.current = row

        def __iter__(self):
            return iter(("a",))

    row = make_rows(_ColumnNames(("a", "b")), [[1, 2]])[0]
    mapping = CustomMapping(row)
    assert len(mapping) == 1
    assert dict(mapping) == {"a": 1}
    mapping.__init__(make_rows(_ColumnNames(("a",)), [[3]])[0])
    assert mapping["a"] == 3


@pytest.mark.parametrize("operation", (copy.copy, copy.deepcopy, pickle.dumps))
def test_mapping_copy_and_pickle(operation):
    row_type = Row if operation is copy.copy else _SerializableRow
    row = row_type([1, 2, 3], {"a": 2, "b": 1}, column_names=_ColumnNames(("a", "b", "a")))
    mapping = row._mapping
    assert dict(mapping) == {"a": 3, "b": 2}
    result = operation(mapping)
    if operation is pickle.dumps:
        result = pickle.loads(result)
    if operation is not copy.copy:
        assert result._row._column_names._prepared is None
    assert type(result) is RowMapping
    assert dict(result) == {"a": 3, "b": 2}
    if operation is copy.copy:
        assert result._row is row


@pytest.mark.parametrize("operation", (copy.copy, copy.deepcopy))
def test_mapping_copy_preserves_subclass_slots_and_attributes(operation):
    class CustomMapping(RowMapping):
        __slots__ = ("label", "__dict__")

    mapping = CustomMapping(_SerializableRow([1], {"a": 0}, column_names=_ColumnNames(("a",))))
    mapping.label = "slot"
    mapping.note = "attribute"
    result = operation(mapping)
    assert type(result) is CustomMapping
    assert dict(result) == {"a": 1}
    assert result.label == "slot"
    assert result.note == "attribute"


def test_mapping_native_lifetimes_in_isolated_debug_process():
    program = """
import gc
import sys
import weakref
from mssql_python.row import Row, RowMapping, _ColumnNames

names = _ColumnNames(("a",))
row = Row([object()], {"a": 0}, column_names=names)
reference = weakref.ref(row)
mapping = row._mapping
before = sys.getrefcount(row)
for _ in range(100):
    assert row._mapping["a"] is row[0]
assert sys.getrefcount(row) == before
del row
assert reference() is not None
assert list(mapping) == ["a"]
del mapping
assert reference() is None

type_refs = sys.getrefcount(RowMapping)
for cycle in (False, True):
    for _ in range(500):
        row = Row([42], {"a": 0}, column_names=names)
        reference = weakref.ref(row)
        mapping = row._mapping
        iterator = iter(mapping)
        assert dict(mapping) == {"a": 42}
        if cycle:
            row._values[0] = (mapping, iterator)
        else:
            assert list(iterator) == ["a"]
        del row, mapping, iterator
        gc.collect()
        assert reference() is None
assert sys.getrefcount(RowMapping) == type_refs

if RowMapping._prepared_supported:
    callback_results = []

    class ReenterMapping:
        def __init__(self, view):
            self.view = view

        def __del__(self):
            result = {}
            callback_results.append(result)
            try:
                result["value"] = self.view["replacement"]
                result["keys"] = list(iter(self.view))
            except Exception as error:
                result["error"] = (type(error).__name__, str(error))

    old_row = Row([10], {"old": 0}, column_names=_ColumnNames(("old",)))
    old_reference = weakref.ref(old_row)
    reentrant_view = old_row._mapping
    old_row.on_delete = ReenterMapping(reentrant_view)
    assert dict(reentrant_view) == {"old": 10}
    assert reentrant_view._prepared_counts[:2] == (1, 1)
    replacement = Row(
        [20, 30],
        {"replacement": 0, "other": 1},
        column_names=_ColumnNames(("replacement", "other")),
    )
    reentrant_view._row = replacement
    del old_row
    assert old_reference() is not None
    assert callback_results == []
    assert list(iter(reentrant_view)) == ["replacement", "other"]
    assert callback_results == [
        {"value": 20, "keys": ["replacement", "other"]}
    ], callback_results
    assert old_reference() is None
    assert reentrant_view._prepared_counts[0] == 2
    replacement._values[0] = 21
    assert dict(reentrant_view) == {"replacement": 21, "other": 30}
    assert len(callback_results) == 1
    del reentrant_view, replacement

    canonical_key = "".join(("partial", "_first"))
    missing_key = "".join(("partial", "_missing"))
    first_index = int("1000")
    partial_names = _ColumnNames((canonical_key, missing_key))
    partial_columns = {canonical_key: first_index}
    partial_row = Row([None] * (first_index + 1), partial_columns, column_names=partial_names)
    partial_row._values[first_index] = 42
    partial_view = partial_row._mapping
    partial_names._unique_keys()
    assert next(iter(partial_names._unique)) is canonical_key
    assert next(iter(partial_columns)) is canonical_key
    assert partial_columns[canonical_key] is first_index
    try:
        partial_row[missing_key]
    except KeyError as error:
        missing_args = error.args
    else:
        raise AssertionError("Expected ordinary Row lookup to fail")
    entry_refs = (sys.getrefcount(canonical_key), sys.getrefcount(first_index))
    for _ in range(3):
        assert list(iter(partial_view)) == [canonical_key, missing_key]
        assert partial_view._prepared_counts[:2] == (0, 0)
        assert partial_names._prepared is None
        assert partial_view[canonical_key] == 42
        try:
            partial_view[missing_key]
        except KeyError as error:
            assert error.args == missing_args
        else:
            raise AssertionError("Expected mapping lookup to fail")
        assert (sys.getrefcount(canonical_key), sys.getrefcount(first_index)) == entry_refs
    partial_columns[missing_key] = 0
    partial_row._values[0] = 84
    assert dict(partial_view) == {canonical_key: 42, missing_key: 84}
    assert partial_view._prepared_counts[:2] == (1, 1)
    assert partial_names._prepared is not None
    del partial_view, partial_row, partial_columns, partial_names

# Exercise shutdown with a live cycle as well as explicit collection above.
row = Row([None], {"a": 0}, column_names=names)
mapping = row._mapping
assert dict(mapping) == {"a": None}
row._values[0] = mapping
"""
    run_mapping_program(program, debug=True)


@pytest.mark.skipif(
    not RowMapping._prepared_supported, reason="CPython dictionary watchers unavailable"
)
def test_mapping_watcher_exhaustion_warns_and_falls_back():
    program = """
import ctypes
import warnings
callback_type = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p
)
callback = callback_type(lambda *args: 0)
add = ctypes.pythonapi.PyDict_AddWatcher
add.argtypes = [callback_type]
add.restype = ctypes.c_int
clear = ctypes.pythonapi.PyDict_ClearWatcher
clear.argtypes = [ctypes.c_int]
clear.restype = ctypes.c_int
watchers = []
try:
    for _ in range(32):
        try:
            watchers.append(add(callback))
        except RuntimeError:
            break
    else:
        raise AssertionError("Expected the bounded CPython watcher table to fill")
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        from mssql_python.row import Row, RowMapping, _ColumnNames
    assert any("no CPython dict watcher" in str(item.message) for item in recorded), recorded
    assert not RowMapping._prepared_supported
    row = Row([1], {"a": 0}, column_names=_ColumnNames(("a",)))
    assert dict(row._mapping) == {"a": 1}
    row._values[0] = 2
    assert dict(row._mapping) == {"a": 2}
finally:
    for watcher in watchers:
        assert clear(watcher) == 0
"""
    run_mapping_program(program)
