import json
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from hpc_campaign.info import format_image_associations, format_info
from hpc_campaign.manager import Manager
from hpc_campaign.manager import main as manager_main
from hpc_campaign.manager_args import ArgParser

repo_root = Path(__file__).resolve().parents[1]
data_dir = repo_root / "data"


def test_visualization_sequence_single_source(tmp_path: Path):
    archive_name = "visualization_single.aca"
    image_path = tmp_path / "thumb.png"
    Image.new("RGB", (8, 8), color="blue").save(image_path)
    print(f"tmp_path={tmp_path}")

    # Create a tiny campaign with one source dataset and one image that will
    # act as the sequence thumbnail and only item.
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")
    manager.image(str(image_path), name="thumb")

    # Register one visualization sequence. The source dataset is implicit for
    # variables unless they override it, and the sequence items are explicit
    # image references rather than a frame-pattern string.
    visid = manager.visualization_sequence(
        name="output/temp_heatmap",
        vis_type="heatmap",
        source_dataset="output",
        variables=[{"name": "temp", "role": "primary"}],
        items=[{"type": "IMAGE", "name": "thumb"}],
        thumbnail_name="thumb",
        metadata={"colormap": "viridis"},
    )

    assert visid > 0

    # Read the archive back through info() and verify that the sequence-level
    # metadata, variable association, thumbnail, and explicit item were stored.
    info_data = manager.info()
    assert len(info_data.visualization_sequences) == 1

    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert sequence_info.name == "output/temp_heatmap"
    assert sequence_info.vis_type == "heatmap"
    assert sequence_info.thumbnail_dataset_name == "thumb"
    assert len(sequence_info.variables) == 1
    assert sequence_info.variables[0].name == "temp"
    assert sequence_info.variables[0].role == "primary"
    assert sequence_info.variables[0].source_dataset_name == "output"
    assert sequence_info.metadata == '{"colormap": "viridis"}'
    assert len(sequence_info.items) == 1
    assert sequence_info.items[0].item_type == "IMAGE"
    assert sequence_info.items[0].dataset_name == "thumb"
    assert sequence_info.items[0].item_uuid == sequence_info.thumbnail_item_uuid

    # format_info() should expose the new visualization section in human-
    # readable output, not just in the structured InfoResult object.
    output_text = format_info(info_data)
    assert "Visualization Sequences:" in output_text
    assert "output/temp_heatmap   type=heatmap" in output_text
    assert "primary: temp (dataset output)" in output_text
    assert "items: 1 (IMAGE)" in output_text

    image_assoc_text = format_image_associations(info_data)
    assert "thumb: IMAGE" in image_assoc_text
    assert "sequence: output/temp_heatmap" in image_assoc_text
    assert "vis_type: heatmap" in image_assoc_text
    assert "thumbnail: true" in image_assoc_text
    assert "variables: primary=temp@output" in image_assoc_text
    assert f"item_uuid: {sequence_info.thumbnail_item_uuid}" in image_assoc_text
    assert 'metadata: {"colormap": "viridis"}' in image_assoc_text

    manager.close()


def test_manager_info_images_flag_is_parsed():
    parser = ArgParser(args=["demo.aca", "info", "--images"], prog="hpc_campaign manager")
    assert parser.parse_next_command()
    assert parser.args.command == "info"
    assert parser.args.images is True


def test_scalar_field_visualization_sequence(tmp_path: Path):
    archive_name = "scalar_field_visualization.aca"
    field = np.arange(12, dtype=np.float32).reshape(3, 4)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")
    manager.scalar_field_data(field, name="output/visualizations/temp/scalar.000000.raw")

    visid = manager.visualization_sequence(
        name="output/visualizations/temp",
        vis_type="heatmap",
        source_dataset="output",
        variables=[{"name": "temp", "role": "primary"}],
        items=[{"type": "SCALAR_FIELD", "name": "output/visualizations/temp/scalar.000000.raw"}],
        metadata={"colormap": "viridis"},
    )

    assert visid > 0

    info_data = manager.info(list_replicas=True, list_files=True)
    scalar_dataset = next(dataset for dataset in info_data.datasets.values() if dataset.file_format == "SCALAR_FIELD")
    assert scalar_dataset.name == "output/visualizations/temp/scalar.000000.raw"
    assert scalar_dataset.metadata is not None
    assert scalar_dataset.metadata["kind"] == "scalarField"
    assert scalar_dataset.metadata["shape"] == [3, 4]
    assert scalar_dataset.metadata["dtype"] == "float32"
    assert scalar_dataset.metadata["compression"] == "none"
    assert scalar_dataset.metadata["encoding"] == "raw"
    assert scalar_dataset.metadata["min"] == 0.0
    assert scalar_dataset.metadata["max"] == 11.0

    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert sequence_info.items[0].item_type == "SCALAR_FIELD"
    assert sequence_info.items[0].dataset_name == scalar_dataset.name
    assert sequence_info.items[0].file_format == "SCALAR_FIELD"

    assoc_text = format_image_associations(info_data)
    assert "output/visualizations/temp/scalar.000000.raw: SCALAR_FIELD" in assoc_text
    assert "sequence: output/visualizations/temp" in assoc_text
    assert "variables: primary=temp@output" in assoc_text

    manager.close()


def test_scalar_field_data_preserves_array_dtype_by_default(tmp_path: Path):
    archive_name = "scalar_field_dtype.aca"
    field = np.arange(6, dtype=np.float64).reshape(2, 3)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.scalar_field_data(field, name="output/visualizations/temp/scalar.000000.raw")

    info_data = manager.info(list_replicas=True, list_files=True)
    scalar_dataset = next(dataset for dataset in info_data.datasets.values() if dataset.file_format == "SCALAR_FIELD")
    assert scalar_dataset.metadata is not None
    assert scalar_dataset.metadata["shape"] == [2, 3]
    assert scalar_dataset.metadata["dtype"] == "float64"
    assert scalar_dataset.metadata["min"] == 0.0
    assert scalar_dataset.metadata["max"] == 5.0

    manager.close()


def test_scalar_field_data_shape_validates_array_shape(tmp_path: Path):
    archive_name = "scalar_field_shape.aca"
    field = np.arange(6, dtype=np.float32).reshape(2, 3)
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))

    with pytest.raises(ValueError, match="does not match scalar field array shape"):
        manager.scalar_field_data(field, shape=(3, 2), name="output/visualizations/temp/scalar.000000.raw")


def test_scalar_field_data_bytes_require_shape_and_dtype(tmp_path: Path):
    archive_name = "scalar_field_bytes.aca"
    field = np.arange(6, dtype=np.float32).reshape(2, 3)
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))

    with pytest.raises(ValueError, match="dtype is required"):
        manager.scalar_field_data(
            field.tobytes(),
            shape=field.shape,
            name="output/visualizations/temp/missing_dtype.raw",
        )

    with pytest.raises(ValueError, match="shape=\\[height, width\\] is required"):
        manager.scalar_field_data(field.tobytes(), dtype="float32", name="output/visualizations/temp/missing_shape.raw")

    manager.open(create=True, truncate=True)
    manager.scalar_field_data(
        field.tobytes(),
        shape=field.shape,
        dtype="float32",
        name="output/visualizations/temp/scalar.000000.raw",
    )
    info_data = manager.info(list_replicas=True, list_files=True)
    scalar_dataset = next(dataset for dataset in info_data.datasets.values() if dataset.file_format == "SCALAR_FIELD")
    assert scalar_dataset.metadata is not None
    assert scalar_dataset.metadata["shape"] == [2, 3]
    assert scalar_dataset.metadata["dtype"] == "float32"

    manager.close()


def test_scalar_field_sequence_rejects_mismatched_shape_or_dtype(tmp_path: Path):
    archive_name = "scalar_field_sequence_mismatch.aca"
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    manager.scalar_field_data(
        np.arange(6, dtype=np.float32).reshape(2, 3),
        name="output/visualizations/temp/scalar.000000.raw",
    )
    manager.scalar_field_data(
        np.arange(6, dtype=np.float32).reshape(3, 2),
        name="output/visualizations/temp/scalar.000001.raw",
    )
    with pytest.raises(ValueError, match="compatible metadata"):
        manager.visualization_sequence(
            name="output/visualizations/temp_shape",
            vis_type="heatmap",
            source_dataset="output",
            variables=[{"name": "temp", "role": "primary"}],
            items=[
                {"type": "SCALAR_FIELD", "name": "output/visualizations/temp/scalar.000000.raw"},
                {"type": "SCALAR_FIELD", "name": "output/visualizations/temp/scalar.000001.raw"},
            ],
        )

    manager.scalar_field_data(
        np.arange(6, dtype=np.float64).reshape(2, 3),
        name="output/visualizations/temp/scalar.000002.raw",
    )
    with pytest.raises(ValueError, match="compatible metadata"):
        manager.visualization_sequence(
            name="output/visualizations/temp_dtype",
            vis_type="heatmap",
            source_dataset="output",
            variables=[{"name": "temp", "role": "primary"}],
            items=[
                {"type": "SCALAR_FIELD", "name": "output/visualizations/temp/scalar.000000.raw"},
                {"type": "SCALAR_FIELD", "name": "output/visualizations/temp/scalar.000002.raw"},
            ],
        )

    manager.close()


def test_visualization_sequence_rejects_mixed_item_types(tmp_path: Path):
    archive_name = "visualization_mixed_items.aca"
    image_path = tmp_path / "thumb.png"
    Image.new("RGB", (8, 8), color="green").save(image_path)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")
    manager.image(str(image_path), name="thumb")
    manager.scalar_field_data(
        np.arange(6, dtype=np.float32).reshape(2, 3),
        name="output/visualizations/temp/scalar.000000.raw",
    )

    with pytest.raises(ValueError, match="mixed item types are not supported"):
        manager.visualization_sequence(
            name="output/visualizations/mixed",
            vis_type="heatmap",
            source_dataset="output",
            variables=[{"name": "temp", "role": "primary"}],
            items=[
                {"type": "IMAGE", "name": "thumb"},
                {"type": "SCALAR_FIELD", "name": "output/visualizations/temp/scalar.000000.raw"},
            ],
        )

    manager.close()


def test_visualization_sequence_multi_source_with_thumbnail(tmp_path: Path):
    archive_name = "visualization_multi.aca"
    image_path = tmp_path / "thumbnail.png"
    Image.new("RGB", (8, 8), color="red").save(image_path)
    print(f"tmp_path={tmp_path}")

    # Create two source datasets so the sequence can reference variables from
    # different inputs, plus one image that the sequence will point at.
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output_a")
    manager.data(str(data_dir / "onearray.h5"), name="output_b")
    manager.image(str(image_path), name="thumb")

    # Initial definition: one sequence, two variables from different datasets,
    # and one IMAGE item referenced by dataset name.
    visid = manager.visualization_sequence(
        name="output/overlay",
        vis_type="overlay",
        variables=[
            {"name": "rho", "role": "background", "source_dataset": "output_a"},
            {"name": "current_z", "role": "contour", "source_dataset": "output_b"},
        ],
        items=[{"type": "IMAGE", "name": "thumb"}],
        thumbnail_name="thumb",
        replace=False,
    )

    assert visid > 0

    # Confirm the multi-dataset variable mapping and the explicit item link.
    info_data = manager.info()
    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert sequence_info.thumbnail_dataset_name == "thumb"
    assert [(entry.role, entry.name, entry.source_dataset_name) for entry in sequence_info.variables] == [
        ("background", "rho", "output_a"),
        ("contour", "current_z", "output_b"),
    ]
    assert len(sequence_info.items) == 1
    assert sequence_info.items[0].dataset_name == "thumb"

    # Replace the sequence definition in place. This exercises the update path:
    # same sequence name/visid, changed vis_type and variable list, item
    # referenced this time by UUID instead of dataset name.
    updated_visid = manager.visualization_sequence(
        name="output/overlay",
        vis_type="heatmap_contour",
        variables=[
            {"name": "rho", "role": "background", "source_dataset": "output_a"},
            {"name": "div_b", "role": "contour", "source_dataset": "output_b"},
        ],
        items=[{"type": "IMAGE", "uuid": sequence_info.items[0].item_uuid}],
        thumbnail_uuid=sequence_info.items[0].item_uuid,
        replace=True,
    )

    assert updated_visid == visid

    # The sequence row should be updated in place and the old variable mapping
    # should be replaced by the new one.
    updated_info = manager.info()
    updated_sequence = next(iter(updated_info.visualization_sequences.values()))
    assert updated_sequence.vis_type == "heatmap_contour"
    assert [(entry.role, entry.name, entry.source_dataset_name) for entry in updated_sequence.variables] == [
        ("background", "rho", "output_a"),
        ("contour", "div_b", "output_b"),
    ]
    assert len(updated_sequence.items) == 1
    assert updated_sequence.items[0].dataset_name == "thumb"

    manager.close()


def test_visualization_single_file_convenience_api(tmp_path: Path):
    archive_name = "visualization_convenience_single.aca"
    image_path = tmp_path / "frame.png"
    Image.new("RGB", (12, 10), color="purple").save(image_path)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    visid = manager.visualization(
        images=str(image_path),
        vis_type="heatmap",
        source_dataset="output",
        variables=[{"name": "temp", "role": "primary"}],
        thumbnail=(6, 6),
        metadata={"note": "auto-generated names"},
    )

    assert visid > 0

    info_data = manager.info(list_replicas=True, list_files=True)
    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert sequence_info.name == "output/visualizations/temp"
    assert len(sequence_info.items) == 1
    assert sequence_info.items[0].dataset_name == "output/visualizations/temp/image.000000.png"
    image_dataset = next(
        dataset for dataset in info_data.datasets.values() if dataset.name == sequence_info.items[0].dataset_name
    )
    assert len(image_dataset.replicas) == 2

    manager.close()


def test_visualization_convenience_api_respects_verbose(tmp_path: Path, capsys):
    archive_name = "visualization_verbose.aca"
    image_path_a = tmp_path / "frame_a.png"
    image_path_b = tmp_path / "frame_b.png"
    image_path_c = tmp_path / "frame_c.png"
    Image.new("RGB", (12, 10), color="purple").save(image_path_a)
    Image.new("RGB", (12, 10), color="green").save(image_path_b)
    Image.new("RGB", (12, 10), color="orange").save(image_path_c)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path), verbose=0)
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    capsys.readouterr()
    manager.visualization(
        images=[image_path_a, image_path_b],
        vis_type="heatmap",
        source_dataset="output",
        variables=[{"name": "temp", "role": "primary"}],
    )
    captured = capsys.readouterr()
    assert captured.out == ""

    manager.visualization(
        images=image_path_c,
        vis_type="heatmap",
        source_dataset="output",
        name="temp_verbose",
        variables=[{"name": "temp", "role": "primary"}],
        verbose=1,
    )
    captured = capsys.readouterr()
    assert "Process image" in captured.out

    manager.close()


def test_visualization_sequence_file_list_convenience_api(tmp_path: Path):
    archive_name = "visualization_convenience_sequence.aca"
    first_dir = tmp_path / "a"
    second_dir = tmp_path / "b"
    first_dir.mkdir()
    second_dir.mkdir()
    image_path_a = first_dir / "image.000.png"
    image_path_b = second_dir / "image.000.png"
    Image.new("RGB", (8, 8), color="green").save(image_path_a)
    Image.new("RGB", (8, 8), color="yellow").save(image_path_b)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    visid = manager.visualization(
        images=[image_path_a, image_path_b],
        vis_type="heatmap_contour",
        source_dataset="output",
        variables=[
            {"name": "rho", "role": "background"},
            {"name": "pressure", "role": "contour"},
        ],
    )

    assert visid > 0

    info_data = manager.info()
    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert len(sequence_info.items) == 2
    expected_prefix = "output/visualizations/background-rho__contour-pressure"
    assert sequence_info.items[0].dataset_name == f"{expected_prefix}/image.000000.png"
    assert sequence_info.items[1].dataset_name == f"{expected_prefix}/image.000001.png"

    manager.close()


def test_visualization_in_memory_image_convenience_api(tmp_path: Path):
    archive_name = "visualization_convenience_memory.aca"
    image = Image.new("RGB", (10, 10), color="orange")
    png_path = tmp_path / "memory.png"
    image.save(png_path)
    png_bytes = png_path.read_bytes()

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    visid = manager.visualization(
        images=png_bytes,
        vis_type="heatmap",
        source_dataset="output",
        variables=[{"name": "temp", "role": "primary"}],
        thumbnail=(4, 4),
        store=True,
    )

    assert visid > 0

    info_data = manager.info(list_replicas=True, list_files=True)
    sequence_info = next(iter(info_data.visualization_sequences.values()))
    image_dataset = next(
        dataset for dataset in info_data.datasets.values() if dataset.name == sequence_info.items[0].dataset_name
    )
    assert len(image_dataset.replicas) == 2
    assert any(replica.flags.embedded for replica in image_dataset.replicas.values())

    manager.close()


def test_in_memory_image_inputs_require_store_true(tmp_path: Path):
    archive_name = "visualization_memory_store_false.aca"
    image = Image.new("RGB", (10, 10), color="orange")
    png_path = tmp_path / "memory.png"
    image.save(png_path)
    png_bytes = png_path.read_bytes()

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    with pytest.raises(ValueError, match="image_data requires store=True"):
        manager.image_data(png_bytes, image_format="PNG", name="memory/image.png", store=False)

    with pytest.raises(ValueError, match="image_data requires store=True"):
        manager.visualization(
            images=png_bytes,
            vis_type="heatmap",
            source_dataset="output",
            variables=[{"name": "temp", "role": "primary"}],
            store=False,
        )

    manager.close()


def test_visualization_semantic_arguments_and_steps(tmp_path: Path):
    archive_name = "visualization_semantic.aca"
    image_path_a = tmp_path / "image_a.png"
    image_path_b = tmp_path / "image_b.png"
    image_path_c = tmp_path / "image_c.png"
    Image.new("RGB", (8, 8), color="green").save(image_path_a)
    Image.new("RGB", (8, 8), color="yellow").save(image_path_b)
    Image.new("RGB", (8, 8), color="red").save(image_path_c)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    visid = manager.visualization(
        images=[image_path_a, image_path_b],
        kind="heatmap_contour",
        source_dataset="output",
        name="rho_current_overlay",
        color_by="rho",
        contour_by="current_z",
        steps=[10, 20],
    )

    assert visid > 0

    info_data = manager.info()
    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert sequence_info.name == "output/visualizations/rho_current_overlay"
    assert sequence_info.vis_type == "heatmap_contour"
    assert [(entry.role, entry.name, entry.source_dataset_name) for entry in sequence_info.variables] == [
        ("color-by", "rho", "output"),
        ("contour-by", "current_z", "output"),
    ]
    assert [item.dataset_name for item in sequence_info.items] == [
        "output/visualizations/rho_current_overlay/image.000010.png",
        "output/visualizations/rho_current_overlay/image.000020.png",
    ]

    updated_visid = manager.visualization(
        images=image_path_c,
        kind="heatmap",
        source_dataset="output",
        name="rho_current_overlay",
        color_by="rho",
        steps=[30],
        replace=True,
    )

    assert updated_visid == visid
    updated_info = manager.info()
    updated_sequence = next(iter(updated_info.visualization_sequences.values()))
    assert updated_sequence.vis_type == "heatmap"
    assert [(entry.role, entry.name, entry.source_dataset_name) for entry in updated_sequence.variables] == [
        ("color-by", "rho", "output"),
    ]
    assert [item.dataset_name for item in updated_sequence.items] == [
        "output/visualizations/rho_current_overlay/image.000030.png",
    ]

    manager.close()


def test_visualization_axis_semantics_and_exact_sequence_name(tmp_path: Path):
    archive_name = "visualization_axes.aca"
    image_path = tmp_path / "line.png"
    Image.new("RGB", (8, 8), color="white").save(image_path)

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(data_dir / "onearray.h5"), name="output")

    visid = manager.visualization(
        images=image_path,
        kind="line-plot",
        source_dataset="output",
        sequence_name="custom/sequence/name",
        x_axis="time",
        y_axis="mass",
    )

    assert visid > 0
    info_data = manager.info()
    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert sequence_info.name == "custom/sequence/name"
    assert sequence_info.vis_type == "line-plot"
    assert [(entry.role, entry.name, entry.source_dataset_name) for entry in sequence_info.variables] == [
        ("x-axis", "time", "output"),
        ("y-axis", "mass", "output"),
    ]
    assert sequence_info.items[0].dataset_name == "custom/sequence/name/image.000000.png"

    manager.close()


def test_scalar_field_cli_raw_file_requires_shape_and_dtype(tmp_path: Path):
    archive_name = "scalar_field_cli_raw.aca"
    raw_path = tmp_path / "scalar.raw"
    field = np.arange(6, dtype=np.float32).reshape(2, 3)
    raw_path.write_bytes(field.tobytes())

    with pytest.raises(ValueError, match="dtype is required"):
        manager_main(
            args=[
                "--campaign_store",
                str(tmp_path),
                "--truncate",
                archive_name,
                "scalar-field",
                str(raw_path),
                "--name",
                "output/visualizations/temp/scalar.missing_dtype.raw",
                "--shape",
                "2",
                "3",
            ],
            prog="hpc_campaign manager",
        )

    with pytest.raises(ValueError, match="shape=\\[height, width\\] is required"):
        manager_main(
            args=[
                "--campaign_store",
                str(tmp_path),
                "--truncate",
                archive_name,
                "scalar-field",
                str(raw_path),
                "--name",
                "output/visualizations/temp/scalar.missing_shape.raw",
                "--dtype",
                "float32",
            ],
            prog="hpc_campaign manager",
        )

    metadata_path = tmp_path / "scalar_metadata.json"
    metadata_path.write_text(json.dumps({"source_step": 0}), encoding="utf-8")
    manager_main(
        args=[
            "--campaign_store",
            str(tmp_path),
            "--truncate",
            archive_name,
            "scalar-field",
            str(raw_path),
            "--name",
            "output/visualizations/temp/scalar.000000.raw",
            "--shape",
            "2",
            "3",
            "--dtype",
            "float32",
            "--value-encoding",
            "direct",
            "--metadata-json",
            str(metadata_path),
        ],
        prog="hpc_campaign manager",
    )

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    info_data = manager.info(list_replicas=True, list_files=True)
    scalar_dataset = next(dataset for dataset in info_data.datasets.values() if dataset.file_format == "SCALAR_FIELD")
    assert scalar_dataset.metadata is not None
    assert scalar_dataset.metadata["shape"] == [2, 3]
    assert scalar_dataset.metadata["dtype"] == "float32"
    assert scalar_dataset.metadata["source_step"] == 0

    manager.close()


def test_scalar_field_cli_npy_and_visualization_sequence_manifest(tmp_path: Path):
    archive_name = "scalar_field_cli_sequence.aca"
    npy_path = tmp_path / "scalar.npy"
    field = np.arange(6, dtype=np.float64).reshape(2, 3)
    np.save(npy_path, field)

    manifest_path = tmp_path / "sequence.json"
    manifest_path.write_text(
        json.dumps(
            {
                "name": "output/visualizations/temp_scalar_field",
                "vis_type": "heatmap",
                "source_dataset": "output",
                "variables": [{"name": "temp", "role": "color-by"}],
                "items": [
                    {
                        "type": "SCALAR_FIELD",
                        "name": "output/visualizations/temp_scalar_field/scalar.000000.raw",
                    }
                ],
                "metadata": {"colormap": "viridis"},
                "replace": True,
            }
        ),
        encoding="utf-8",
    )

    manager_main(
        args=[
            "--campaign_store",
            str(tmp_path),
            "--truncate",
            archive_name,
            "data",
            str(data_dir / "onearray.h5"),
            "--name",
            "output",
            "scalar-field",
            str(npy_path),
            "--name",
            "output/visualizations/temp_scalar_field/scalar.000000.raw",
            "visualization-sequence",
            str(manifest_path),
        ],
        prog="hpc_campaign manager",
    )

    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    info_data = manager.info(list_replicas=True, list_files=True)
    scalar_dataset = next(dataset for dataset in info_data.datasets.values() if dataset.file_format == "SCALAR_FIELD")
    assert scalar_dataset.metadata is not None
    assert scalar_dataset.metadata["shape"] == [2, 3]
    assert scalar_dataset.metadata["dtype"] == "float64"

    sequence_info = next(iter(info_data.visualization_sequences.values()))
    assert sequence_info.name == "output/visualizations/temp_scalar_field"
    assert sequence_info.items[0].item_type == "SCALAR_FIELD"
    assert sequence_info.items[0].dataset_name == scalar_dataset.name
    assert [(entry.role, entry.name, entry.source_dataset_name) for entry in sequence_info.variables] == [
        ("color-by", "temp", "output")
    ]

    manager.close()
