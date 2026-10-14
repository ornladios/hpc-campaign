import json
from pathlib import Path

import h5py
import numpy as np
import pytest
from adios2.stream import Stream

from hpc_campaign.manager import Manager


def _source_file(path: Path) -> Path:
    with h5py.File(path, "w") as output:
        output.create_dataset("time", data=np.array([0.0, 0.25, 0.75, 1.0]))
    return path


def _adios_source_file(path: Path) -> Path:
    with Stream(str(path), "w") as output:
        for value in (0.0, 0.25, 0.75, 1.0):
            output.begin_step()
            time = np.asarray([value], dtype=np.float64)
            output.write("time", time, time.shape, [0], [1])
            output.end_step()
    return path


def test_video_time_range_is_stored_per_frame(tmp_path: Path):
    source_path = _source_file(tmp_path / "source.h5")
    movie_path = tmp_path / "movie.mp4"
    movie_path.write_bytes(b"opaque video bytes")

    manager = Manager("video_range.aca", campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(source_path, name="source")
    manager.video(
        movie_path,
        name="source/visualizations/movie.mp4",
        source_dataset="source",
        variables=["temperature"],
        vis_type="movie",
        sequence_name="source/visualizations/movie",
        time_range=(2.0, 8.0),
        frame_count=4,
        store=True,
    )

    info = manager.info(list_replicas=True, list_files=True)
    video_dataset = next(dataset for dataset in info.datasets.values() if dataset.file_format == "VIDEO")
    sequence = next(iter(info.visualization_sequences.values()))
    item = sequence.items[0]
    metadata = json.loads(item.metadata)

    assert video_dataset.name == "source/visualizations/movie.mp4"
    assert video_dataset.replicas
    assert item.item_type == "VIDEO"
    assert item.file_format == "VIDEO"
    assert metadata["time_mode"] == "physical_time"
    assert metadata["time_values"] == [2.0, 4.0, 6.0, 8.0]
    assert metadata["time_unit"] == "s"
    assert metadata["mime_type"] == "video/mp4"
    manager.close()


def test_video_uses_indices_from_campaign_time_variable(tmp_path: Path):
    source_path = _source_file(tmp_path / "source.h5")
    movie_path = tmp_path / "movie.mp4"
    movie_path.write_bytes(b"opaque video bytes")

    manager = Manager("video_indices.aca", campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(source_path, name="source")
    manager.video(
        movie_path,
        name="source/visualizations/movie.mp4",
        source_dataset="source",
        variables=["temperature"],
        vis_type="movie",
        sequence_name="source/visualizations/movie",
        time_variable=("source", "time"),
        time_indices=[0, 2, 3],
        frame_count=3,
    )

    info = manager.info()
    sequence = next(iter(info.visualization_sequences.values()))
    metadata = json.loads(sequence.items[0].metadata)
    assert metadata["time_values"] == [0.0, 0.75, 1.0]
    assert metadata["time_source"]["indices"] == [0, 2, 3]
    assert metadata["time_source"]["variable"] == {"dataset": "source", "name": "time"}
    manager.close()


def test_video_reads_all_adios_time_steps_from_local_replica(tmp_path: Path):
    source_path = _adios_source_file(tmp_path / "source.bp")
    movie_path = tmp_path / "movie.mp4"
    movie_path.write_bytes(b"opaque video bytes")

    manager = Manager("video_adios_indices.aca", campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(source_path, name="source")
    manager.video(
        movie_path,
        name="source/visualizations/movie.mp4",
        source_dataset="source",
        variables=["temperature"],
        vis_type="movie",
        sequence_name="source/visualizations/movie",
        time_variable=("source", "time"),
        time_indices=[0, 2, 3],
        frame_count=3,
    )

    sequence = next(iter(manager.info().visualization_sequences.values()))
    metadata = json.loads(sequence.items[0].metadata)
    assert metadata["time_values"] == [0.0, 0.75, 1.0]
    manager.close()


def test_video_explicit_frame_times_and_count_validation(tmp_path: Path):
    source_path = _source_file(tmp_path / "source.h5")
    movie_path = tmp_path / "movie.mp4"
    movie_path.write_bytes(b"opaque video bytes")
    manager = Manager("video_explicit.aca", campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(source_path, name="source")

    with pytest.raises(ValueError, match="one time per frame"):
        manager.video(
            movie_path,
            name="movie.mp4",
            variables=[{"name": "pressure", "source_dataset": "source"}],
            vis_type="movie",
            frame_times=[0.0],
            frame_count=2,
        )

    manager.video(
        movie_path,
        name="movie.mp4",
        variables=[{"name": "pressure", "source_dataset": "source"}],
        vis_type="movie",
        frame_times=[0.2, 0.25],
        frame_count=2,
    )
    sequence = next(iter(manager.info().visualization_sequences.values()))
    assert json.loads(sequence.items[0].metadata)["time_values"] == [0.2, 0.25]

    manager.close()
