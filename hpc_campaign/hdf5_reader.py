"""Read HDF5 metadata embedded in an ACA without extracting files to disk."""

import sqlite3
import zlib
from contextlib import closing, contextmanager
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator

import h5py
import numpy as np

from .types import VariableType


@contextmanager
def open_embedded_hdf5(archive: str, dsid: int) -> Iterator[h5py.File]:
    """Open the newest live replica's embedded HDF5 image in memory.

    The index API does not accept encryption keys, so encrypted metadata is
    rejected explicitly. The ACA and the HDF5 image are both opened read-only.
    """
    uri = Path(archive).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        row = connection.execute(
            """
            SELECT r.keyid, f.compression, f.data
            FROM dataset AS d
            JOIN replica AS r ON r.datasetid = d.rowid
            JOIN repfiles AS rf ON rf.replicaid = r.rowid
            JOIN file AS f ON f.fileid = rf.fileid
            WHERE d.rowid = ? AND d.fileformat = 'HDF5'
              AND d.deltime = 0 AND r.deltime = 0
            ORDER BY r.rowid DESC, f.fileid DESC
            LIMIT 1
            """,
            (dsid,),
        ).fetchone()
    if row is None or row[2] is None:
        raise ValueError(f"No embedded HDF5 metadata for dataset {dsid} in {archive}")
    keyid, compression, data = row
    if keyid and int(keyid) > 0:
        raise ValueError(f"Cannot index encrypted HDF5 metadata for dataset {dsid} in {archive}")
    image = zlib.decompress(data) if compression else bytes(data)
    # BytesIO is supported throughout the project's h5py >= 3.10 range.
    # Close HDF5 before closing the Python file object it wraps.
    with BytesIO(image) as buffer, h5py.File(buffer, "r") as reader:
        yield reader


# pylint: disable=too-many-return-statements
def variable_type(dtype: np.dtype) -> VariableType:
    """Map HDF5/NumPy storage types onto the existing ACX type vocabulary."""
    if h5py.check_string_dtype(dtype) is not None or dtype.kind in "SU":
        return VariableType.string
    if dtype.fields is not None:
        return VariableType.struct
    if dtype.kind == "b":
        return VariableType.int8_t
    if dtype.kind in "iu":
        prefix = "uint" if dtype.kind == "u" else "int"
        return VariableType.__members__.get(f"{prefix}{dtype.itemsize * 8}_t", VariableType.unknown)
    if dtype.kind == "f":
        return {4: VariableType.float, 8: VariableType.double}.get(dtype.itemsize, VariableType.long_double)
    if dtype.kind == "c":
        return {8: VariableType.float_complex, 16: VariableType.double_complex}.get(
            dtype.itemsize, VariableType.unknown
        )
    return VariableType.unknown


def json_value(value: Any) -> Any:
    """Convert HDF5 values to JSON, retaining nested values and field names.

    Complex numbers become [real, imaginary] pairs. Object references have no
    portable value outside their file, and are represented by null.
    """
    if isinstance(value, np.ndarray):
        if value.dtype.names:
            return json_value(value[()]) if value.ndim == 0 else [json_value(item) for item in value]
        return json_value(value.tolist())
    if isinstance(value, np.void):
        if value.dtype.names:
            return {name: json_value(value[name]) for name in value.dtype.names}
        return value.tobytes().hex()
    if isinstance(value, (h5py.Reference, h5py.Empty)):
        return None
    if isinstance(value, (bytes, np.bytes_)):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, (complex, np.complexfloating)):
        return [float(value.real), float(value.imag)]
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (np.datetime64, np.timedelta64)):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, np.generic):
        return str(value)
    return value


def _objects(reader: h5py.File):
    objects = [("", reader)]
    # visititems follows hard links only, avoiding external source files and
    # cycles. HDF5 named datatypes are retained here for their attributes.
    reader.visititems(lambda name, obj: objects.append((name, obj)))
    return objects


def _has_adios_steps(reader: h5py.File) -> bool:
    # ADIOS identifies its layout by /Step0, even when NumSteps is absent.
    # Do not dereference an external or soft link while detecting the layout.
    return isinstance(reader.get("Step0", getlink=True), h5py.HardLink) and isinstance(reader["Step0"], h5py.Group)


def iter_variables(reader: h5py.File) -> Iterator[dict[str, Any]]:
    """Yield ACX variable records without reading arrays or external data.

    ACA creation may discard the contents of large HDF5 arrays, so their fill
    values cannot establish actual bounds. Bounds remain unknown for HDF5.
    """
    adios_steps = _has_adios_steps(reader)
    summaries: dict[str, dict[str, Any]] = {}
    objects = _objects(reader)
    if adios_steps:
        # Lexical traversal would place Step10 before Step2.
        objects.sort(
            key=lambda item: int(item[0].split("/", 1)[0][4:]) if item[0].split("/", 1)[0][4:].isdigit() else -1
        )
    for name, obj in objects:
        if not isinstance(obj, h5py.Dataset):
            continue
        if adios_steps:
            step, separator, name = name.partition("/")
            if not separator or not step.startswith("Step") or not step[4:].isdigit():
                continue
        shape = obj.shape
        value: Any = ""
        if shape == () and not obj.is_virtual and obj.external is None:
            value = json_value(obj[()])
        entry = {
            "vname": name,
            "vtype": variable_type(obj.dtype),
            "vsteps": 1,
            "vmin": None,
            "vmax": None,
            "ndim": len(shape) if shape is not None else 0,
            "vshape": ", ".join(str(size) for size in shape) if shape is not None else "",
            "value": value,
        }
        if not adios_steps:
            yield entry
            continue
        if name not in summaries:
            entry["value"] = [value] if shape == () else ""
            summaries[name] = entry
        else:
            summary = summaries[name]
            if summary["vtype"] != entry["vtype"] or summary["ndim"] != entry["ndim"]:
                raise ValueError(f"HDF5 variable {name} changes type or rank between steps")
            summary["vsteps"] += 1
            if shape == ():
                summary["value"].append(value)
    yield from summaries.values()


def iter_attributes(reader: h5py.File) -> Iterator[tuple[str, Any, VariableType]]:
    """Yield root, group, dataset and named-datatype attributes by full path."""
    adios_steps = _has_adios_steps(reader)
    for path, obj in _objects(reader):
        for name, value in obj.attrs.items():
            if adios_steps and not path and name == "NumSteps":
                continue
            dtype = obj.attrs.get_id(name).dtype
            yield f"{path}/{name}" if path else name, json_value(value), variable_type(dtype)
