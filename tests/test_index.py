"""Exercise archive indexing with a real index database and a controlled ADIOS reader."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

from hpc_campaign.index import Index
from hpc_campaign.info import ArchiveInfo, DatasetInfo, InfoResult
from hpc_campaign.types import VariableType


@pytest.mark.parametrize("verbose", [0, 1])
def test_remove_commits_all_archives(campaign_index, verbose, capsys):
    campaign_index.args.verbose = verbose
    for archive in ["first.aca", 'second"quoted.aca']:
        archiveid = campaign_index._add_archive(archive, "")
        datasetid = campaign_index._add_dataset(archiveid, 1, "data", 0, "")
        campaign_index.con.execute(
            "INSERT INTO variables (archiveid, datasetid, name) VALUES (?, ?, ?)",
            (archiveid, datasetid, "temperature"),
        )
        campaign_index.con.execute(
            "INSERT INTO attributes (archiveid, datasetid, name) VALUES (?, ?, ?)",
            (archiveid, datasetid, "units"),
        )
    campaign_index.con.commit()

    campaign_index.remove([])

    assert not campaign_index.con.in_transaction
    assert "SQL commit" not in capsys.readouterr().out
    campaign_index.close()
    with sqlite3.connect(campaign_index.args.index_file) as connection:
        for table in ("archives", "datasets", "variables", "attributes"):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


@pytest.fixture
def json_input():
    content = Path(__file__).with_name("example.json").read_bytes()
    return np.frombuffer(content, dtype=np.uint8), json.loads(content)


@pytest.fixture
def campaign_index(tmp_path):
    index = Index("json", campaign_store=str(tmp_path))
    index.open(create=True)
    try:
        yield index
    finally:
        index.close()


def test_open_adds_time_series_tables_to_existing_index(campaign_index):
    archiveid = campaign_index._add_archive("existing.aca", "")
    campaign_index.con.commit()
    for table in ["timeseries_variable_steps", "timeseries_datasets", "timeseries"]:
        campaign_index.con.execute(f"DROP TABLE {table}")
    campaign_index.close()

    campaign_index.open()

    assert campaign_index.con.execute("SELECT archiveid, name FROM archives").fetchall() == [
        (archiveid, "existing.aca")
    ]
    for table in ["timeseries", "timeseries_datasets", "timeseries_variable_steps"]:
        assert campaign_index.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


def mock_json_archive(monkeypatch, dataset_name, file_format):
    """Replace archive I/O while retaining dataset grouping and SQLite writes."""
    info = InfoResult(
        archive=ArchiveInfo("ACA", "JSON test", "0.7", 0),
        datasets={7: DatasetInfo("json-test", dataset_name, 0, 0, file_format)},
    )
    manager = MagicMock()
    manager.return_value.info.return_value = info
    monkeypatch.setattr("hpc_campaign.index.Manager", manager)

    reader = MagicMock()
    reader.available_variables.return_value = {}
    reader.available_attributes.return_value = {}
    file_reader = MagicMock()
    file_reader.return_value.__enter__.return_value = reader
    monkeypatch.setattr("hpc_campaign.index.adios2.FileReader", file_reader)
    return reader


@pytest.mark.parametrize("wildcard", [False, True])
def test_add_absolute_archive_path(monkeypatch, campaign_index, tmp_path, json_input, wildcard):
    store = tmp_path / "store"
    store.mkdir()
    campaign_index.args.campaign_store = str(store)
    # The archive is outside the campaign store and contains regex metacharacters.
    archive = tmp_path / "external [1].aca"
    archive.touch()
    content, expected = json_input
    reader = mock_json_archive(monkeypatch, "example.json", "TEXT")
    reader.available_variables.return_value = {"example.json": {"Type": "uint8_t"}}
    reader.read.return_value = content

    campaign_index.add([str(archive)], wildcard=wildcard)

    assert campaign_index.ls([]) == [str(archive)]
    rows = campaign_index.con.execute("SELECT name, value FROM variables").fetchall()
    assert {name: json.loads(value) for name, value in rows} == {key.title(): value for key, value in expected.items()}


@pytest.mark.parametrize("pattern, wildcard", [(r"external.*\.aca$", False), ("external*.aca", True)])
def test_add_archive_pattern(monkeypatch, campaign_index, tmp_path, pattern, wildcard):
    (tmp_path / "external.aca").touch()
    (tmp_path / "other.aca").touch()
    index_archive = MagicMock()
    monkeypatch.setattr(campaign_index, "_index_archive", index_archive)

    campaign_index.add([pattern], wildcard=wildcard)

    index_archive.assert_called_once_with("external.aca")


def test_index_attributes_with_overlapping_dataset_names(monkeypatch, campaign_index):
    reader = mock_json_archive(monkeypatch, "input", "TEXT")
    info = InfoResult(
        archive=ArchiveInfo("ACA", "Path test", "0.7", 0),
        datasets={
            1: DatasetInfo("input", "input", 0, 0, "TEXT"),
            2: DatasetInfo("data", "input_dir/De0.dat", 0, 0, "TEXT"),
            3: DatasetInfo("nested", "input/nested", 0, 0, "TEXT"),
        },
    )
    manager = MagicMock()
    manager.return_value.info.return_value = info
    monkeypatch.setattr("hpc_campaign.index.Manager", manager)
    values = {
        "input/__dataset_location__": "local",
        "input_dir/De0.dat/__dataset_location__": "remote",
        "input/nested/__dataset_location__": "remote",
    }
    reader.available_attributes.return_value = {name: {"Type": "string"} for name in values}
    # Reading a reconstructed name that does not exist must fail as in ADIOS.
    reader.read_attribute.side_effect = values.__getitem__

    campaign_index._index_archive("overlapping.aca")

    rows = campaign_index.con.execute(
        "SELECT datasets.name, attributes.name, attributes.value FROM attributes JOIN datasets USING (datasetid)"
    ).fetchall()
    assert sorted(rows) == [
        ("input", "__dataset_location__", '"local"'),
        ("input/nested", "__dataset_location__", '"remote"'),
        ("input_dir/De0.dat", "__dataset_location__", '"remote"'),
    ]


def test_dataset_grouping_rejects_unmatched_entry_after_match(campaign_index):
    info = InfoResult(
        archive=ArchiveInfo("ACA", "Path test", "0.7", 0),
        datasets={1: DatasetInfo("input", "input", 0, 0, "TEXT")},
    )
    with pytest.raises(RuntimeError, match="unrelated/attribute has no matching dataset"):
        campaign_index._organize_by_dataset({"input/attribute": {}, "unrelated/attribute": {}}, info)


def test_index_json_as_variable(monkeypatch, campaign_index, json_input):
    content, expected = json_input
    dataset_name = "inputs/example.json"
    reader = mock_json_archive(monkeypatch, dataset_name, "TEXT")
    # TEXT datasets expose their bytes under the dataset name, without a suffix.
    reader.available_variables.return_value = {dataset_name: {"Type": "uint8_t"}}
    reader.read.return_value = content

    campaign_index._index_archive("json-variable.aca")

    rows = campaign_index.con.execute("SELECT name, value FROM variables").fetchall()
    assert len(rows) == len(expected)
    assert {name: json.loads(value) for name, value in rows} == {key.title(): value for key, value in expected.items()}
    assert campaign_index.con.execute("SELECT COUNT(*) FROM attributes").fetchone()[0] == 0
    reader.read.assert_called_once_with(dataset_name)


@pytest.mark.parametrize("dataset_name, variable_type", [("inputs/adios2.xml", "char"), ("notes.txt", "uint8_t")])
def test_index_text_as_variable(monkeypatch, campaign_index, dataset_name, variable_type):
    reader = mock_json_archive(monkeypatch, dataset_name, "TEXT")
    reader.available_variables.return_value = {
        dataset_name: {"Type": variable_type, "Shape": "1216", "SingleValue": "false", "AvailableStepsCount": "1"}
    }
    variable = reader.inquire_variable.return_value
    variable.type.return_value = variable_type
    variable.steps.return_value = 1
    variable.shape.return_value = [1216]
    variable.single_value.return_value = False

    # Re-indexing must also retain a single row with SQL NULL bounds.
    for _ in range(2):
        campaign_index._index_archive("text-variable.aca")

    reader.inquire_variable.assert_called_with(dataset_name)
    rows = campaign_index.con.execute("SELECT name, type, steps, min, max, ndim, shape FROM variables").fetchall()
    assert rows == [("", VariableType[variable_type], 1, None, None, 1, "1216")]
    reader.read.assert_not_called()


def test_index_json_as_attribute(monkeypatch, campaign_index, json_input):
    content, expected = json_input
    dataset_name = "simulation.bp"
    attribute_name = "metadata/example.json"
    reader = mock_json_archive(monkeypatch, dataset_name, "ADIOS")
    reader.available_attributes.return_value = {f"{dataset_name}/{attribute_name}": {"Type": "uint8_t"}}
    reader.read_attribute.return_value = content

    campaign_index._index_archive("json-attribute.aca")

    rows = campaign_index.con.execute("SELECT name, value FROM attributes").fetchall()
    # Each JSON member must survive expansion instead of overwriting the others.
    assert len(rows) == len(expected)
    assert sorted(value for _, value in rows) == sorted(json.dumps(value) for value in expected.values())
    assert campaign_index.con.execute("SELECT COUNT(*) FROM variables").fetchone()[0] == 0
    reader.read_attribute.assert_called_once_with(f"{dataset_name}/{attribute_name}")


@pytest.mark.parametrize(
    ("key", "expected_min", "expected_max"),
    [
        ("L_int", "64", "64"),
        ("optional_parameter", None, None),
        ("steps_array", "1000", "3000"),
        ("float_array", "1.1", "4.4444"),
        ("output_variables", "U", "V"),
        ("enabled_axes", "0", "1"),
        ("empty_array", None, None),
        ("mixed_array", None, None),
        ("solver", None, None),
        ("empty_object", None, None),
    ],
)
def test_index_json_variable_bounds(monkeypatch, campaign_index, json_input, key, expected_min, expected_max):
    _, example = json_input
    value = example[key]
    dataset_name = "example.json"
    reader = mock_json_archive(monkeypatch, dataset_name, "TEXT")
    reader.available_variables.return_value = {dataset_name: {"Type": "uint8_t"}}
    reader.read.return_value = np.frombuffer(json.dumps({key: value}).encode("utf-8"), dtype=np.uint8)

    campaign_index._index_archive("json-bounds.aca")

    row = campaign_index.con.execute("SELECT min, max, value FROM variables WHERE name = ?", (key.title(),)).fetchone()
    assert row is not None
    assert row[:2] == (expected_min, expected_max)
    assert json.loads(row[2]) == value
