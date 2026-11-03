"""Atomic image product publication through Manager."""

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from PIL import Image

import hpc_campaign.image_publication as publication
from hpc_campaign import Manager


def png():
    buffer = BytesIO()
    Image.new("RGB", (16, 12), "blue").save(buffer, format="PNG")
    return buffer.getvalue()


def test_standalone_publication_is_embedded_and_idempotent(tmp_path):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    identifier = str(uuid4())
    record = manager.publish_image(png(), publication_id=identifier, title="Comparison")
    assert record["associations"] == []
    assert manager.publish_image(b"", publication_id=identifier, title="Ignored") == record
    assert manager.con.execute("select count(*) from dataset").fetchone()[0] == 2
    assert manager.con.execute("select count(*) from file where data is not null").fetchone()[0] == 2
    assert manager.con.execute("select count(*) from visualization_sequence").fetchone()[0] == 0
    manager.close()


def test_record_failure_rolls_back_image_and_visualization(tmp_path, monkeypatch):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    manager.data(Path(__file__).resolve().parents[1] / "data/onearray.h5", name="output")
    tables = [
        "dataset",
        "replica",
        "repfiles",
        "file",
        "resolution",
        "visualization_sequence",
        "visualization_variable",
        "visualization_item",
    ]
    before = {table: [tuple(row) for row in manager.con.execute(f"select * from {table}")] for table in tables}

    def fail(*_args, **_kwargs):
        raise RuntimeError("record write failed")

    monkeypatch.setattr(publication, "add_file_to_archive", fail)
    with pytest.raises(RuntimeError, match="record write failed"):
        manager.publish_image(
            png(),
            publication_id=str(uuid4()),
            title="Comparison",
            variables=[{"name": "temperature", "source_dataset": "output"}],
        )
    assert not manager.con.in_transaction
    assert {table: [tuple(row) for row in manager.con.execute(f"select * from {table}")] for table in tables} == before
    manager.close()


def test_two_writers_with_same_id_return_one_publication(tmp_path):
    campaign = str(tmp_path / "campaign.aca")
    manager = Manager(campaign)
    manager.open(create=True)
    manager.close()
    barrier = Barrier(2)
    identifier = str(uuid4())
    payload = png()

    def write(title):
        writer = Manager(campaign)
        writer.open()
        try:
            barrier.wait(timeout=5)
            return writer.publish_image(payload, publication_id=identifier, title=title)
        finally:
            writer.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        records = list(pool.map(write, ["First", "Second"]))
    assert records[0] == records[1]
    manager.open()
    assert manager.con.execute("select count(*) from dataset").fetchone()[0] == 2
    manager.close()
