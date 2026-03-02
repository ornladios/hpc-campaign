from enum import IntEnum
from typing import Any, TypeAlias

import numpy as np


class DatasetType(IntEnum):
    """Enum for dataset types in the archive"""

    Unknown = 0
    ADIOS = 1
    HDF5 = 2
    IMAGE = 3
    TEXT = 4


ADIOS_AvailableVariables: TypeAlias = dict[str, dict[str, str]]
DatasetsVariables: TypeAlias = dict[int, ADIOS_AvailableVariables]


class VariableType(IntEnum):
    """Enum for variable/attribute types in datasets as seen by adios2"""

    unknown = 0
    int8_t = 1
    int16_t = 2
    int32_t = 3
    int64_t = 4
    uint8_t = 5
    uint16_t = 6
    uint32_t = 7
    uint64_t = 8
    float = 9
    double = 10
    long_double = 11
    float_complex = 12
    double_complex = 13
    string = 14
    char = 15
    struct = 16


# pylint: disable=too-many-return-statements
# pylint: disable=unidiomatic-typecheck
def python_type_to_variable_type(obj: Any) -> tuple[int, VariableType]:
    """
    Convert a Python or NumPy type to a VariableType enum.
    """
    if isinstance(obj, bool):
        return 1, VariableType.int8_t

    if isinstance(obj, int):
        return 1, VariableType.int64_t

    if isinstance(obj, float):
        return 1, VariableType.double

    if isinstance(obj, complex):
        return 1, VariableType.double_complex

    if isinstance(obj, str):
        return 1, VariableType.string

    if isinstance(obj, bytes):
        return 1, VariableType.uint8_t

    if isinstance(obj, (list, tuple, dict)):
        return len(obj), python_type_to_variable_type(next(iter(obj)))[1]

    if obj is None:
        return 1, VariableType.unknown

    return 1, VariableType.unknown


def variable_type_to_python_type(vt: VariableType):
    """Translation between adios2 types and python/numpy types"""
    return {
        VariableType.unknown: bytes,
        VariableType.int8_t: np.int8,
        VariableType.int16_t: np.int16,
        VariableType.int32_t: np.int32,
        VariableType.int64_t: np.int64,
        VariableType.uint8_t: np.uint8,
        VariableType.uint16_t: np.uint16,
        VariableType.uint32_t: np.uint32,
        VariableType.uint64_t: np.uint64,
        VariableType.float: np.float32,
        VariableType.double: np.float64,
        VariableType.float_complex: np.complex64,
        VariableType.double_complex: np.complex128,
        VariableType.string: str,
        VariableType.char: np.uint8,
        VariableType.struct: bytes,
    }[vt]


def type_variable_to_python(name):
    """Translation between adios2 types and python/numpy types"""
    return {
        "none": bytes,
        "int8_t": np.int8,
        "int16_t": np.int16,
        "int32_t": np.int32,
        "int64_t": np.int64,
        "uint8_t": np.uint8,
        "uint16_t": np.uint16,
        "uint32_t": np.uint32,
        "uint64_t": np.uint64,
        "float": np.float32,
        "double": np.float64,
        "float complex": np.complex64,
        "double complex": np.complex128,
        "string": str,
        "char": np.uint8,
        "struct": bytes,
    }[name]
