# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""Explicit native SQL Server JSON inputs and result decoding."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, final

from .exceptions import DataError, NotSupportedError


def _validate(value: object, active: set[int]) -> None:
    if type(value) in (dict, list):
        identity = id(value)
        if identity in active:
            raise ValueError("Json does not support circular references")
        active.add(identity)
        try:
            if isinstance(value, dict):
                for key, item in value.items():
                    if type(key) is not str:
                        raise TypeError("Json object keys must be strings")
                    _validate(item, active)
            elif isinstance(value, list):
                for item in value:
                    _validate(item, active)
        finally:
            active.remove(identity)
    elif value is not None and type(value) not in (str, int, float, bool):
        raise TypeError("Json values must be objects, arrays, strings, numbers, booleans or None")


@final
@dataclass(frozen=True, slots=True, init=False, repr=False)
class Json:
    """Native JSON input: serialize a dict/list or retain already-serialized str.

    Text is not parsed or normalized in Python; the provider/server validates
    its JSON syntax and object/array root when the statement executes.
    """

    _text: str = field(repr=False)

    def __init__(self, value: dict[str, Any] | list[Any] | str) -> None:
        if type(value) is str:
            text = value
        elif type(value) in (dict, list):
            _validate(value, set())
            text = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        else:
            raise TypeError("Json requires a dict, list or str; use bare None for SQL NULL")
        text.encode("utf-16-le", "strict")
        object.__setattr__(self, "_text", text)

    def __repr__(self) -> str:
        return "Json(<serialized>)"


def _decode_json(value: str) -> dict[str, Any] | list[Any]:
    try:
        result = json.loads(value)
    except ValueError as error:
        raise DataError("Invalid native JSON result", "") from error
    if not isinstance(result, (dict, list)):
        raise DataError("Native JSON result must have an object or array root", "")
    return result


def _reject_json_parameters(values: Any, backend: str) -> None:
    if isinstance(values, Mapping):
        values = values.values()
    elif not isinstance(values, Sequence):
        return
    if any(isinstance(value, Json) for value in values):
        raise NotSupportedError(f"Json parameters are not supported by {backend}", "")
