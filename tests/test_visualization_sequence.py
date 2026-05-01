from pathlib import Path

from PIL import Image

from hpc_campaign.info import format_info
from hpc_campaign.manager import Manager

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
