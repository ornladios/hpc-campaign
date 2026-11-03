"""Atomic movie publication through Manager."""

import hashlib
import io
import json
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from PIL import Image

import hpc_campaign.image_publication as publication
from hpc_campaign import Manager


@pytest.fixture
def movie_data(tmp_path):
    if not shutil.which("ffmpeg"):
        pytest.skip("requires FFmpeg")
    path = tmp_path / "movie.mp4"
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=blue:s=16x12:r=2:d=1",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(path),
        ],
        check=True,
    )
    preview = io.BytesIO()
    Image.new("RGB", (16, 12), "blue").save(preview, format="PNG")
    return (
        path.read_bytes(),
        preview.getvalue(),
        {"width": 16, "height": 12, "fps": 2.0, "frame_count": 2, "duration": 1.0, "codec": "h264"},
    )


def publish(manager, media, identifier, **kwargs):
    data, preview, metadata = media
    return manager.publish_video(
        data, preview=preview, publication_id=identifier, title="Comparison movie", video_metadata=metadata, **kwargs
    )


def test_movie_standalone_embeds_separate_video_preview_and_record(tmp_path, movie_data):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    identifier = str(uuid4())
    record = publish(manager, movie_data, identifier)
    assert record["format"] == "hpc-campaign-published-video"
    assert record["video_sha256"] == hashlib.sha256(movie_data[0]).hexdigest()
    assert record["image_sha256"] == hashlib.sha256(movie_data[1]).hexdigest()
    assert sorted(r[0] for r in manager.con.execute("select fileformat from dataset")) == ["IMAGE", "TEXT", "VIDEO"]
    assert manager.con.execute("select count(*) from visualization_sequence").fetchone()[0] == 0
    assert (
        manager.publish_video(b"", preview=b"", publication_id=identifier, title="Ignored retry", video_metadata={})
        == record
    )
    with pytest.raises(ValueError, match="Invalid existing publication"):
        manager.publish_image(movie_data[1], publication_id=identifier, title="Wrong media type")
    assert manager.con.execute("select count(*) from dataset").fetchone()[0] == 3
    manager.close()


def test_movie_associations_use_video_item_and_image_thumbnail(tmp_path, movie_data):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    manager.data(Path(__file__).resolve().parents[1] / "data/onearray.h5", name="output")
    record = publish(manager, movie_data, str(uuid4()), variables=[{"name": "temperature", "source_dataset": "output"}])
    row = manager.con.execute(
        "select i.item_type,i.item_uuid,s.thumbnail_itemuuid,i.metadata "
        "from visualization_sequence s join visualization_item i on i.visid=s.visid"
    ).fetchone()
    assert tuple(row[:3]) == ("VIDEO", record["video_uuid"], record["image_uuid"])
    assert json.loads(row[3])["fps"] == 2
    manager.close()


def test_record_failure_rolls_back_movie_preview_and_associations(tmp_path, movie_data, monkeypatch):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    manager.data(Path(__file__).resolve().parents[1] / "data/onearray.h5", name="output")
    tables = [
        "dataset",
        "host",
        "directory",
        "replica",
        "repfiles",
        "file",
        "resolution",
        "visualization_sequence",
        "visualization_variable",
        "visualization_item",
    ]
    before = {t: [tuple(r) for r in manager.con.execute(f"select * from {t}")] for t in tables}
    original = publication.add_file_to_archive

    def fail_record(args, filename, cur, rep_id, mt=0, filename_as_recorded="", **kwargs):
        if filename_as_recorded == "publication.json":
            raise RuntimeError("movie record write failed")
        return original(args, filename, cur, rep_id, mt, filename_as_recorded, **kwargs)

    monkeypatch.setattr(publication, "add_file_to_archive", fail_record)
    with pytest.raises(RuntimeError, match="movie record write failed"):
        publish(manager, movie_data, str(uuid4()), variables=[{"name": "temperature", "source_dataset": "output"}])
    assert {t: [tuple(r) for r in manager.con.execute(f"select * from {t}")] for t in tables} == before
    assert not manager.con.in_transaction
    manager.close()


def test_concurrent_movie_retries_create_one_product(tmp_path, movie_data):
    campaign = str(tmp_path / "campaign.aca")
    manager = Manager(campaign)
    manager.open(create=True)
    manager.close()
    barrier = Barrier(2)
    identifier = str(uuid4())

    def write(_):
        writer = Manager(campaign)
        writer.open()
        try:
            barrier.wait(timeout=5)
            return publish(writer, movie_data, identifier)
        finally:
            writer.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        records = list(pool.map(write, range(2)))
    assert records[0] == records[1]
    manager.open()
    assert manager.con.execute("select count(*) from dataset").fetchone()[0] == 3
    manager.close()


@pytest.mark.parametrize("field,value", [("duration", -1), ("fps", float("nan")), ("frame_count", 1.5)])
def test_invalid_playback_metadata_does_not_modify_archive(tmp_path, movie_data, field, value):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    data, preview, metadata = movie_data
    metadata = {**metadata, field: value}
    with pytest.raises(ValueError, match="Invalid video metadata"):
        publish(manager, (data, preview, metadata), str(uuid4()))
    assert manager.con.execute("select count(*) from dataset").fetchone()[0] == 0
    manager.close()


@pytest.mark.parametrize("times", [[0.25, 1.0], [1.0, 1.0]])
def test_physical_time_mapping_is_preserved(tmp_path, movie_data, times):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    manager.data(Path(__file__).resolve().parents[1] / "data/onearray.h5", name="output")
    data, preview, metadata = movie_data
    metadata = {
        **metadata,
        "time_mode": "physical_time",
        "time_values": times,
        "time_unit": "s",
        "time_source": {"kind": "explicit"},
    }
    identifier = str(uuid4())
    record = publish(
        manager, (data, preview, metadata), identifier, variables=[{"name": "temperature", "source_dataset": "output"}]
    )
    assert record["video"]["time_values"] == times
    assert record["video"]["time_mode"] == "physical_time"
    item = json.loads(manager.con.execute("select metadata from visualization_item").fetchone()[0])
    assert item["time_values"] == times and item["time_unit"] == "s"
    assert item["time_source"] == {"kind": "explicit"}
    assert (
        manager.publish_video(b"", preview=b"", publication_id=identifier, title="Retry", video_metadata={}) == record
    )
    manager.close()


@pytest.mark.parametrize(
    "timing",
    [
        {"time_values": [0]},
        {"time_values": [1, 0]},
        {"time_values": [0, float("nan")]},
        {"time_values": [0, 1], "time_unit": ""},
        {"time_values": [0, 1], "time_source": []},
        {"time_mode": "timestep", "time_values": [0, 1]},
    ],
)
def test_invalid_physical_time_mapping_does_not_modify_archive(tmp_path, movie_data, timing):
    manager = Manager(str(tmp_path / "campaign.aca"))
    manager.open(create=True)
    data, preview, metadata = movie_data
    with pytest.raises(ValueError):
        publish(manager, (data, preview, {**metadata, **timing}), str(uuid4()))
    assert manager.con.execute("select count(*) from dataset").fetchone()[0] == 0
    assert not manager.con.in_transaction
    manager.close()
