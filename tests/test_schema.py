import sqlite3
import subprocess
import sys
import zlib
from pathlib import Path

import pytest
import yaml

from hpc_campaign.manager import Manager
from hpc_campaign.manager_args import ArgParser

# These tests cover the first schema-storage layer only. hpc_campaign stores
# __campaign_schema.yaml as embedded TEXT; downstream ingestion code interprets its
# layout semantics. The assertions here check API/CLI storage, update behavior,
# example fixture coverage, and coexistence with the existing timeseries table.
# They do not validate file patterns, append/file-per-timestep semantics, grid
# associations, or nested schema override rules; schema validation is tested
# separately in test_schema_validation.py.
repo_root = Path(__file__).resolve().parents[1]
data_dir = repo_root / "data"
schema_examples_dir = data_dir / "schema_examples"


def read_embedded_text(archive_path: Path, dataset_name: str = "__campaign_schema.yaml") -> tuple[str, dict]:
    # Inspect the ACA tables directly so the tests verify the actual embedded
    # archive payload, not just the public Manager call path.
    con = sqlite3.connect(str(archive_path))
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            """
            select
              d.name as dataset_name,
              d.fileformat as fileformat,
              r.name as replica_name,
              f.name as file_name,
              f.compression as compression,
              f.data as data
            from dataset as d
            join replica as r on r.datasetid = d.rowid
            join repfiles as rf on rf.replicaid = r.rowid
            join file as f on f.fileid = rf.fileid
            where d.name = ? and d.deltime = 0 and r.deltime = 0
            order by r.rowid desc, f.fileid desc
            limit 1
            """,
            (dataset_name,),
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    raw = bytes(row["data"])
    text = zlib.decompress(raw).decode("utf-8") if int(row["compression"]) else raw.decode("utf-8")
    return text, dict(row)


def schema_example_paths() -> list[Path]:
    # Keep the examples discoverable so adding a new code_*.yaml fixture
    # automatically exercises schema storage for that layout.
    return sorted(schema_examples_dir.glob("code_*.yaml"))


def run_manager_command(campaign_store: Path, args: list[str]) -> subprocess.CompletedProcess:
    # Exercise the installed CLI entry point in a subprocess, matching the
    # existing manager CLI tests.
    command = [
        sys.executable,
        "-m",
        "hpc_campaign",
        "manager",
        "--campaign_store",
        str(campaign_store),
    ]
    command.extend([str(entry) for entry in args])
    return subprocess.run(command, check=True, capture_output=True, text=True)


def test_schema_command_is_parsed():
    # The manager command splitter needs to recognize schema as its own command,
    # otherwise later commands can consume its arguments incorrectly.
    parser = ArgParser(args=["demo.aca", "schema", "__campaign_schema.yaml"], prog="hpc_campaign manager")
    assert parser.parse_next_command()
    assert parser.args.command == "schema"
    assert parser.args.schema_file == "__campaign_schema.yaml"


@pytest.mark.parametrize("schema_path", schema_example_paths(), ids=lambda p: p.stem)
def test_set_schema_stores_example_as_embedded_campaign_schema_yaml(tmp_path: Path, schema_path: Path):
    # Every example schema should be stored under the fixed campaign dataset
    # name __campaign_schema.yaml, regardless of where the source file lives on disk.
    archive_name = f"{schema_path.stem}.aca"
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.set_schema(schema_path)
    manager.close()

    text, stored = read_embedded_text(tmp_path / archive_name)
    schema = yaml.safe_load(text)

    assert stored["dataset_name"] == "__campaign_schema.yaml"
    assert stored["fileformat"] == "TEXT"
    assert stored["file_name"] == "__campaign_schema.yaml"
    assert schema["schema_version"] == 1
    assert schema["name"] == schema_path.stem
    assert isinstance(schema["files"], dict)


def test_schema_cli_stores_campaign_schema_yaml(tmp_path: Path):
    # The CLI should behave the same as Manager.set_schema and produce an
    # embedded TEXT dataset named __campaign_schema.yaml.
    archive_name = "schema_cli.aca"
    schema_path = schema_examples_dir / "code_single_append.yaml"

    run_manager_command(tmp_path, [archive_name, "schema", str(schema_path)])

    text, stored = read_embedded_text(tmp_path / archive_name)
    schema = yaml.safe_load(text)

    assert stored["dataset_name"] == "__campaign_schema.yaml"
    assert stored["fileformat"] == "TEXT"
    assert schema["name"] == "code_single_append"


def test_set_schema_updates_existing_embedded_content(tmp_path: Path):
    # Re-registering __campaign_schema.yaml is expected during schema iteration. The
    # existing replica/file path should be updated rather than leaving stale
    # embedded content behind.
    archive_name = "schema_update.aca"
    schema_path = tmp_path / "__campaign_schema.yaml"
    schema_path.write_text(
        "schema_version: 1\n"
        "name: first\n"
        "files:\n"
        "  output:\n"
        "    role: time_series\n"
        "    mode: append\n"
        "    path: output.bp\n",
        encoding="utf-8",
    )

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.set_schema(schema_path)

    schema_path.write_text(
        "schema_version: 1\n"
        "name: second\n"
        "files:\n"
        "  output:\n"
        "    role: time_series\n"
        "    mode: append\n"
        "    path: updated.bp\n",
        encoding="utf-8",
    )
    manager.set_schema(schema_path)
    manager.close()

    text, _stored = read_embedded_text(tmp_path / archive_name)
    schema = yaml.safe_load(text)

    assert schema["name"] == "second"
    assert schema["files"]["output"]["path"] == "updated.bp"


def test_schema_coexists_with_existing_time_series(tmp_path: Path):
    # Multifile layouts still use the existing timeseries table for ordered
    # dataset membership. The schema describes layout/associations and should
    # not interfere with that zero-based ordering.
    archive_name = "schema_timeseries.aca"
    schema_path = schema_examples_dir / "code_append_grid_multifile.yaml"
    fields = ["fields.0000.bp", "fields.0001.bp"]

    # Build a small campaign that has both new schema metadata and the existing
    # ordered timeseries metadata. onearray.h5 is reused as stand-in data; the
    # dataset names are what matter for this storage-level test.
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.set_schema(schema_path)
    manager.data(data_dir / "onearray.h5", name="grid.bp")
    for field_name in fields:
        manager.data(data_dir / "onearray.h5", name=field_name)
    manager.add_time_series("fields", fields, replace=True)
    manager.close()

    text, stored = read_embedded_text(tmp_path / archive_name)
    schema = yaml.safe_load(text)

    # Confirm the schema remains a normal embedded TEXT dataset named
    # __campaign_schema.yaml, even when the same campaign also has a timeseries.
    assert stored["dataset_name"] == "__campaign_schema.yaml"
    assert schema["name"] == "code_append_grid_multifile"

    # Check the existing timeseries tables directly. This verifies that
    # set_schema() did not alter the current tsid/tsorder representation.
    con = sqlite3.connect(str(tmp_path / archive_name))
    try:
        rows = con.execute(
            """
            select t.name, d.name, d.tsorder
            from timeseries as t
            join dataset as d on d.tsid = t.tsid
            where t.name = ?
            order by d.tsorder
            """,
            ("fields",),
        ).fetchall()
    finally:
        con.close()

    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("fields", "fields.0000.bp", 0),
        ("fields", "fields.0001.bp", 1),
    ]
