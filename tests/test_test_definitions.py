"""Guard against tests silently disappearing when a later definition replaces them."""

import ast
from pathlib import Path


def test_test_names_are_unique_within_each_scope():
    duplicates = []
    for path in sorted(Path(__file__).parent.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        scopes = [tree] + [node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
        for scope in scopes:
            definitions = {}
            for node in scope.body:
                if isinstance(
                    node, (ast.FunctionDef, ast.AsyncFunctionDef)
                ) and node.name.startswith("test_"):
                    if node.name in definitions:
                        duplicates.append(
                            f"{path.name}:{node.lineno}: {node.name} replaces line "
                            f"{definitions[node.name]} in {getattr(scope, 'name', '<module>')}"
                        )
                    definitions[node.name] = node.lineno

    assert not duplicates, "\n".join(duplicates)
