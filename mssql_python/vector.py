"""Native SQL Server vectors. Half-precision wire conversion belongs to the backend."""

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
import math
import struct
from numbers import Real
from typing import Literal, Optional, Union, overload

from .constants import ConstantsDDBC
from .exceptions import NotSupportedError

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


def _try_fast_init(vector: object, values: object, base_type: VectorBaseType) -> bool:
    """Validate a list/tuple of plain floats in bulk; return False to use the checked loop."""
    if not isinstance(values, (list, tuple)) or type(values) not in (list, tuple):
        return False
    count = len(values)
    if not 1 <= count <= (1998 if base_type == "float32" else 3996):
        return False
    if not all(type(value) is float for value in values):
        return False
    peak = _FLOAT16_MAX if base_type == "float16" else _FLOAT32_MAX
    if not all(map(math.isfinite, values)) or max(map(abs, values)) > peak:
        return False
    layout = f"={count}f"
    object.__setattr__(vector, "values", struct.unpack(layout, struct.pack(layout, *values)))
    object.__setattr__(vector, "base_type", base_type)
    return True


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
        if _try_fast_init(self, values, base_type):
            return
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

    @classmethod
    def _from_trusted(cls, values: tuple[float, ...], base_type: VectorBaseType) -> "Vector":
        """Wrap float32 values produced by the driver without revalidating them."""
        vector = object.__new__(cls)
        object.__setattr__(vector, "values", values)
        object.__setattr__(vector, "base_type", base_type)
        return vector

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
    """An OUTPUT parameter for callproc(); value is filled before return.

    Takes the same (sql_type, size, scale) triple as setinputsizes(). Only
    SQL_SS_VECTOR is supported today: size is the dimension count and scale is
    0 (FLOAT32) or 1 (FLOAT16).
    """

    def __init__(self, sql_type: int, size: int, scale: int = 0) -> None:
        if sql_type != ConstantsDDBC.SQL_SS_VECTOR.value:
            raise NotSupportedError(
                driver_error="callproc() output parameters currently support SQL_SS_VECTOR only",
                ddbc_error="Unsupported output parameter SQL type",
            )
        if isinstance(scale, bool) or scale not in (0, 1):
            raise ValueError("Vector subtype must be 0 (FLOAT32) or 1 (FLOAT16)")
        base_type: VectorBaseType = "float16" if scale else "float32"
        _check_shape(size, base_type)
        self.sql_type = sql_type
        self.size = size
        self.scale = scale
        self.value: Optional[Vector] = None

    @property
    def dimensions(self) -> int:
        return self.size

    @property
    def base_type(self) -> VectorBaseType:
        return "float16" if self.scale else "float32"


class InputOutputParameter(OutputParameter):
    """An INPUT_OUTPUT parameter whose SQL type is inferred from its initial Vector."""

    def __init__(self, value: Vector) -> None:
        if not isinstance(value, Vector):
            raise TypeError("InputOutputParameter requires a Vector")
        super().__init__(
            ConstantsDDBC.SQL_SS_VECTOR.value,
            value.dimensions,
            int(value.base_type == "float16"),
        )
        self.value = value
