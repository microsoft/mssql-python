"""Native SQL Server vectors. Half-precision wire conversion belongs to the backend."""

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
import math
import struct
from numbers import Real
from typing import Literal, Optional, Union, overload

VectorBaseType = Literal["float32", "float16"]
_FLOAT32_MAX = 3.4028234663852886e38
_FLOAT16_MAX = 65504.0
_FLOAT32 = struct.Struct("=f")


def _check_shape(dimensions: int, base_type: VectorBaseType) -> None:
    if base_type not in ("float32", "float16"):
        raise ValueError("base_type must be 'float32' or 'float16'")
    limit = 1998 if base_type == "float32" else 3996
    if isinstance(dimensions, bool) or not isinstance(dimensions, int):
        raise TypeError("dimensions must be an integer")
    if not 1 <= dimensions <= limit:
        raise ValueError(f"{base_type} vector dimensions must be between 1 and {limit}")


@dataclass(frozen=True, init=False)
class Vector(Sequence[float]):
    """An immutable vector with explicit SQL storage subtype.

    Values are normalized to float32 and exposed as Python floats. base_type
    selects SQL storage, not a Python scalar type. FLOAT16 range is checked
    conservatively; the backend still performs half-precision conversion.
    """

    values: tuple[float, ...]
    base_type: VectorBaseType

    def __init__(self, values: Iterable[float], base_type: VectorBaseType = "float32") -> None:
        _check_shape(1, base_type)
        if isinstance(values, (str, bytes, bytearray)):
            raise TypeError("vector values must be an iterable of real numbers")
        converted: list[float] = []
        for index, value in enumerate(values):
            if isinstance(value, bool) or not isinstance(value, Real):
                raise TypeError(
                    f"vector element at index {index} must be a real number, not bool or None"
                )
            if abs(value) > _FLOAT32_MAX:
                raise ValueError(f"vector element at index {index} must be a finite float32 value")
            number = float(value)
            if not math.isfinite(number) or abs(number) > _FLOAT32_MAX:
                raise ValueError(f"vector element at index {index} must be a finite float32 value")
            if base_type == "float16" and abs(number) > _FLOAT16_MAX:
                raise ValueError(
                    f"vector element at index {index} exceeds the finite float16 range"
                )
            converted.append(_FLOAT32.unpack(_FLOAT32.pack(number))[0])
            _check_shape(len(converted), base_type)
        _check_shape(len(converted), base_type)
        object.__setattr__(self, "values", tuple(converted))
        object.__setattr__(self, "base_type", base_type)

    @property
    def dimensions(self) -> int:
        return len(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def __iter__(self) -> Iterator[float]:
        return iter(self.values)

    @overload
    def __getitem__(self, index: int) -> float: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[float, ...]: ...

    def __getitem__(self, index: Union[int, slice]) -> Union[float, tuple[float, ...]]:
        return self.values[index]


def _vector_from_hint(value: object, dimensions: int, subtype: int) -> Optional[Vector]:
    if subtype not in (0, 1):
        raise ValueError("Vector subtype must be 0 (FLOAT32) or 1 (FLOAT16)")
    base_type: VectorBaseType = "float16" if subtype else "float32"
    _check_shape(dimensions, base_type)
    if value is None:
        return None
    if isinstance(value, list):
        value = Vector(value, base_type)
    if not isinstance(value, Vector):
        raise TypeError("Vector parameters must be Vector, list, or None")
    if value.dimensions != dimensions or value.base_type != base_type:
        raise ValueError("Vector dimensions and base_type must match the input hint")
    return value


class OutputParameter:
    """A vector OUTPUT parameter for callproc(); value is filled before return."""

    def __init__(self, dimensions: int, base_type: VectorBaseType = "float32") -> None:
        _check_shape(dimensions, base_type)
        self.dimensions = dimensions
        self.base_type = base_type
        self.value: Optional[Vector] = None


class InputOutputParameter(OutputParameter):
    """A vector INPUT_OUTPUT parameter initialized from a Vector."""

    def __init__(self, value: Vector) -> None:
        if not isinstance(value, Vector):
            raise TypeError("InputOutputParameter requires a Vector")
        super().__init__(value.dimensions, value.base_type)
        self.value = value
