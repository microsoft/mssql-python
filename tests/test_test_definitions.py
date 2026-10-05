"""Guard against tests silently disappearing when a later definition replaces them."""

import ast
from pathlib import Path
from textwrap import indent

import pytest


def _scope_test_definitions(scope):
    """Descend through control flow, but not into a new Python namespace."""
    for node in ast.iter_child_nodes(scope):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("test_"):
                yield node
        elif isinstance(node, ast.ClassDef):
            if node.name.startswith("Test"):
                yield node
        elif not isinstance(node, ast.Lambda):
            yield from _scope_test_definitions(node)


def _duplicate_test_names(tree):
    duplicates = []
    scopes = [tree] + [node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    for scope in scopes:
        definitions = {}
        for node in _scope_test_definitions(scope):
            if node.name in definitions:
                duplicates.append(
                    f"{node.lineno}: {node.name} replaces line "
                    f"{definitions[node.name]} in {getattr(scope, 'name', '<module>')}"
                )
            definitions[node.name] = node.lineno
    return duplicates


def test_test_names_are_unique_within_each_scope():
    duplicates = []
    for path in sorted(Path(__file__).parent.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        duplicates.extend(f"{path.name}:{duplicate}" for duplicate in _duplicate_test_names(tree))

    assert not duplicates, "\n".join(duplicates)


@pytest.mark.parametrize("in_class", [False, True], ids=["module", "class"])
@pytest.mark.parametrize(
    "definition, name",
    [
        ("async def test_example():\n    pass\n", "test_example"),
        ("class TestExample:\n    def test_method(self):\n        pass\n", "TestExample"),
    ],
    ids=["function", "test-class"],
)
@pytest.mark.parametrize(
    "block",
    [
        "if enabled:\n{test}",
        "if enabled:\n    pass\nelse:\n{test}",
        "try:\n{test}\nexcept Exception:\n    pass",
        "try:\n    pass\nexcept Exception:\n{test}",
        "try:\n    pass\nfinally:\n{test}",
        "with context:\n{test}",
        "for item in items:\n{test}",
        "while enabled:\n{test}",
        "if enabled:\n    with context:\n{test}",
    ],
    ids=["if", "else", "try", "except", "finally", "with", "for", "while", "nested"],
)
def test_duplicate_guard_checks_control_flow(block, in_class, definition, name):
    nested = "    " if block.startswith("if enabled:\n    with") else ""
    source = block.format(test=indent(definition, "    " + nested))
    source += "\n" + definition
    scope_name = "TestContainer" if in_class else "<module>"
    if in_class:
        source = "class TestContainer:\n" + indent(source, "    ")

    tree = ast.parse(source)
    definitions = sorted(
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and node.name == name
    )
    assert _duplicate_test_names(tree) == [
        f"{definitions[1]}: {name} replaces line {definitions[0]} in {scope_name}"
    ]


@pytest.mark.parametrize("in_class", [False, True], ids=["module", "class"])
def test_duplicate_guard_checks_redefined_test_classes(in_class):
    source = (
        "class TestExample:\n"
        "    def test_first(self):\n"
        "        pass\n"
        "class TestExample:\n"
        "    def test_second(self):\n"
        "        pass\n"
    )
    if in_class:
        source = "class TestContainer:\n" + indent(source, "    ")
    assert _duplicate_test_names(ast.parse(source)) == [
        (
            "5: TestExample replaces line 2 in TestContainer"
            if in_class
            else "4: TestExample replaces line 1 in <module>"
        )
    ]


def test_duplicate_guard_keeps_namespaces_separate():
    tree = ast.parse("""
def test_example():
    def test_example():
        pass

def helper():
    def test_example():
        pass
    class TestNested:
        pass
    return lambda: None

class TestFirst:
    def test_example(self):
        pass

    class TestNested:
        def test_example(self):
            pass

if enabled:
    class TestSecond:
        async def test_example(self):
            pass
        class TestNested:
            def test_example(self):
                pass
""")
    assert _duplicate_test_names(tree) == []


def test_duplicate_guard_checks_classes_inside_control_flow():
    tree = ast.parse("""
if enabled:
    class TestExample:
        def test_example(self):
            pass
        with context:
            def test_example(self):
                pass
""")
    assert _duplicate_test_names(tree) == ["7: test_example replaces line 4 in TestExample"]
