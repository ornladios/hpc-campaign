"""Index embedded HDF5 images without opening their original replicas."""

import json
import re
import sqlite3
import zlib
from io import BytesIO
from unittest.mock import MagicMock

import h5py
import numpy as np
import pytest

from hpc_campaign.index import Index
from hpc_campaign.info import ArchiveInfo, DatasetInfo, InfoResult
from hpc_campaign.manager import Manager
from hpc_campaign.types import VariableType


@pytest.fixture
def campaign_index(tmp_path):
    index = Index("embedded", campaign_store=str(tmp_path))
    index.open(create=True)
    try:
        yield index
    finally:
        index.close()


def _hdf5_image():
    with BytesIO() as buffer:
        with h5py.File(buffer, "w") as output:
            output.attrs["title"] = "embedded run"
            group = output.create_group("fields")
            group.attrs["axes"] = np.array([b"x", b"y"])
            temperature = group.create_dataset("temperature", shape=(20, 30), dtype="f8")
            temperature.attrs["units"] = "K"
            output["iteration"] = np.int32(17)
            output["label"] = "sample"
            output["enabled"] = np.bool_(True)
            output["empty"] = h5py.Empty("f8")
        return buffer.getvalue()


def _archive(tmp_path, monkeypatch, image, compressed=False, dataset_name="run [1];data.h5", text=False):
    archive = tmp_path / "input.aca"
    with sqlite3.connect(archive) as connection:
        connection.executescript("""
            CREATE TABLE dataset (name TEXT, fileformat TEXT, deltime INT);
            CREATE TABLE replica (datasetid INT, deltime INT, keyid INT);
            CREATE TABLE repfiles (replicaid INT, fileid INT);
            CREATE TABLE file (fileid INTEGER PRIMARY KEY, compression INT, data BLOB);
        """)
        connection.execute("INSERT INTO dataset VALUES (?, 'HDF5', 0)", (dataset_name,))
        connection.execute("INSERT INTO replica VALUES (1, 0, 0)")
        connection.execute("INSERT INTO repfiles VALUES (1, 1)")
        connection.execute(
            "INSERT INTO file VALUES (1, ?, ?)",
            (int(compressed), zlib.compress(image) if compressed else image),
        )
    datasets = {1: DatasetInfo("hdf5", dataset_name, 0, 0, "HDF5")}
    if text:
        datasets[2] = DatasetInfo("text", "notes.txt", 0, 0, "TEXT")
    info = InfoResult(archive=ArchiveInfo("ACA", "test", "0.7", 0), datasets=datasets)
    manager = MagicMock()
    manager.return_value.info.return_value = info
    monkeypatch.setattr("hpc_campaign.index.Manager", manager)
    return archive


@pytest.mark.parametrize("compressed", [False, True])
def test_index_embedded_hdf5_in_memory(tmp_path, monkeypatch, campaign_index, compressed):
    archive = _archive(tmp_path, monkeypatch, _hdf5_image(), compressed=compressed)
    file_reader = MagicMock(side_effect=AssertionError("HDF5 must not open through ADIOS"))
    monkeypatch.setattr("hpc_campaign.index.adios2.FileReader", file_reader)

    for _ in range(2):
        campaign_index.add([str(archive)])

    file_reader.assert_not_called()
    rows = campaign_index.con.execute(
        "SELECT name, type, steps, min, max, ndim, shape, value FROM variables"
    ).fetchall()
    variables = {row[0]: row[1:] for row in rows}
    assert set(variables) == {"fields/temperature", "iteration", "label", "enabled", "empty"}
    assert variables["fields/temperature"] == (VariableType.double, 1, None, None, 2, "20, 30", '""')
    assert variables["iteration"][:6] == (VariableType.int32_t, 1, None, None, 0, "")
    assert json.loads(variables["iteration"][-1]) == 17
    assert json.loads(variables["label"][-1]) == "sample"
    assert json.loads(variables["enabled"][-1]) is True
    attributes = {
        name: json.loads(value) for name, value in campaign_index.con.execute("SELECT name, value FROM attributes")
    }
    assert attributes == {"title": "embedded run", "fields/axes": ["x", "y"], "fields/temperature/units": "K"}


def test_mixed_archive_excludes_hdf5_from_campaign_reader(tmp_path, monkeypatch, campaign_index):
    dataset_name = "run [1];data.h5"
    archive = _archive(tmp_path, monkeypatch, _hdf5_image(), dataset_name=dataset_name, text=True)
    adios = MagicMock()
    monkeypatch.setattr("hpc_campaign.index.adios2.Adios", adios)
    reader = MagicMock()
    reader.available_variables.return_value = {}
    reader.available_attributes.return_value = {"notes.txt/source": {"Type": "string"}}
    reader.read_attribute.return_value = "operator"
    file_reader = MagicMock()
    file_reader.return_value.__enter__.return_value = reader
    monkeypatch.setattr("hpc_campaign.index.adios2.FileReader", file_reader)

    campaign_index.add([str(archive)])

    io = adios.return_value.declare_io.return_value
    io.set_engine.assert_called_once_with("Campaign")
    parameters = io.set_parameters.call_args.args[0]
    assert parameters["exclude-dataset"] == "^" + re.escape(dataset_name).replace(";", r"\x3b") + "$"
    assert "CachePath" in parameters
    file_reader.assert_called_once_with(io, str(archive))
    assert campaign_index.con.execute("SELECT COUNT(*) FROM variables").fetchone()[0] == 5
    assert campaign_index.con.execute("SELECT value FROM attributes WHERE name = 'source'").fetchone() == (
        '"operator"',
    )


def test_real_archive_surrogate_needs_no_original_hdf5(tmp_path, monkeypatch, campaign_index):
    monkeypatch.chdir(tmp_path)
    original = tmp_path / "original.h5"
    with h5py.File(original, "w") as output:
        output["field"] = np.arange(600, dtype=np.float64).reshape(20, 30) + 10
        output["iteration"] = np.int64(42)
        output["field"].attrs["units"] = "m"
    archive = tmp_path / "real.aca"
    manager = Manager(str(archive), campaign_store=str(tmp_path))
    try:
        manager.open(create=True)
        manager.data(str(original))
    finally:
        manager.close()
    original.unlink()
    monkeypatch.setattr(
        "hpc_campaign.index.adios2.FileReader",
        MagicMock(side_effect=AssertionError("HDF5 must not open through ADIOS")),
    )

    campaign_index.add([str(archive)])

    assert campaign_index.con.execute(
        "SELECT ndim, shape, min, max FROM variables WHERE name = 'field'"
    ).fetchone() == (2, "20, 30", None, None)
    assert campaign_index.con.execute("SELECT value FROM variables WHERE name = 'iteration'").fetchone() == ("42",)
    assert campaign_index.con.execute("SELECT value FROM attributes WHERE name = 'field/units'").fetchone() == ('"m"',)


def test_bad_embedded_image_retains_previous_index(tmp_path, monkeypatch, campaign_index):
    archive = _archive(tmp_path, monkeypatch, _hdf5_image())
    campaign_index.add([str(archive)])
    tables = ("archives", "datasets", "variables", "attributes")
    before = {table: campaign_index.con.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
    with sqlite3.connect(archive) as connection:
        connection.execute("UPDATE file SET data = ?", (b"not HDF5",))

    with pytest.raises(OSError):
        campaign_index.add([str(archive)])

    assert {table: campaign_index.con.execute(f"SELECT * FROM {table}").fetchall() for table in tables} == before


@pytest.mark.parametrize("keyid, expected", [(5, "encrypted"), (0, "No embedded")])
def test_unavailable_embedded_metadata_fails_cleanly(tmp_path, monkeypatch, campaign_index, keyid, expected):
    archive = _archive(tmp_path, monkeypatch, _hdf5_image())
    with sqlite3.connect(archive) as connection:
        if keyid:
            connection.execute("UPDATE replica SET keyid = ?", (keyid,))
        else:
            connection.execute("DELETE FROM repfiles")

    with pytest.raises(ValueError, match=expected):
        campaign_index.add([str(archive)])

    assert campaign_index.con.execute("SELECT COUNT(*) FROM archives").fetchone() == (0,)


def test_hdf5_time_series_combines_internal_steps(tmp_path, monkeypatch, campaign_index):
    monkeypatch.chdir(tmp_path)
    filenames = ["first.h5", "second.h5"]
    for member, filename in enumerate(filenames):
        with h5py.File(filename, "w") as output:
            output.attrs["NumSteps"] = np.uint64(2)
            for step in range(2):
                group = output.create_group(f"Step{step}")
                group["time"] = np.int64(1 + member * 2 + step)
                group["temperature"] = np.full((20, 30), 10 + member + step, dtype=np.float64)
                group["temperature"].attrs["units"] = "K"
    archive = tmp_path / "timeseries.aca"
    manager = Manager(str(archive), campaign_store=str(tmp_path))
    try:
        manager.open(create=True)
        # Register members in reverse order, then give them their series order.
        for filename in reversed(filenames):
            manager.data(filename)
        manager.add_time_series("fields", filenames)
        for filename in filenames:
            (tmp_path / filename).unlink()
    finally:
        manager.close()
    file_reader = MagicMock(side_effect=AssertionError("HDF5 series must not open through ADIOS"))
    monkeypatch.setattr("hpc_campaign.index.adios2.FileReader", file_reader)

    for _ in range(2):
        campaign_index.add([str(archive)])

    file_reader.assert_not_called()
    rows = campaign_index.con.execute(
        "SELECT v.name, v.steps, v.ndim, v.shape, v.min, v.max, v.value "
        "FROM variables AS v JOIN datasets AS d USING (datasetid) WHERE d.name = 'fields'"
    ).fetchall()
    assert {row[0] for row in rows} == {"temperature", "time"}
    variables = {row[0]: row[1:] for row in rows}
    assert variables["temperature"] == (4, 2, "20, 30", None, None, '""')
    assert variables["time"][:5] == (4, 0, "", None, None)
    assert json.loads(variables["time"][-1]) == [1, 2, 3, 4]
    mappings = campaign_index.con.execute(
        "SELECT d.name, v.name, v.step_start, v.steps "
        "FROM timeseries_variable_steps AS v JOIN datasets AS d USING (datasetid) "
        "ORDER BY v.step_start, v.name"
    ).fetchall()
    assert mappings == [
        ("first.h5", "temperature", 0, 2),
        ("first.h5", "time", 0, 2),
        ("second.h5", "temperature", 2, 2),
        ("second.h5", "time", 2, 2),
    ]
    attributes = campaign_index.con.execute(
        "SELECT a.name, a.value FROM attributes AS a JOIN datasets AS d USING (datasetid) "
        "WHERE d.name = 'fields' ORDER BY a.name"
    ).fetchall()
    assert attributes == [("Step0/temperature/units", '"K"'), ("Step1/temperature/units", '"K"')]
