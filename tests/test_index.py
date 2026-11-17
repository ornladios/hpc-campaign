"""Exercise JSON expansion with a real index database and a controlled ADIOS reader."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

from hpc_campaign.index import Index
from hpc_campaign.info import ArchiveInfo, DatasetInfo, InfoResult


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
