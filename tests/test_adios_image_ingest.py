from pathlib import Path

import adios2
import matplotlib
import numpy as np

from hpc_campaign.manager import Manager

matplotlib.use("Agg")

import matplotlib.pyplot as plt


def make_scalar_fields(nx: int, ny: int, step: int) -> dict[str, np.ndarray]:
    x = np.linspace(0.0, 2.0 * np.pi, nx, endpoint=False)
    y = np.linspace(0.0, 2.0 * np.pi, ny, endpoint=False)
    xx, yy = np.meshgrid(x, y, indexing="ij")

    field_a = np.sin(xx + 0.3 * step) * np.cos(yy)
    field_b = xx + 0.5 * yy + float(step)
    return {
        "field_a": field_a.astype(np.float64),
        "field_b": field_b.astype(np.float64),
    }


def write_adios_dataset(output_path: Path, nsteps: int = 3, nx: int = 16, ny: int = 16) -> None:
    with adios2.Stream(str(output_path), "w") as stream:
        for step in range(nsteps):
            stream.begin_step()
            for name, data in make_scalar_fields(nx, ny, step).items():
                stream.write(name, data, data.shape, [0, 0], data.shape)
            stream.end_step()


def render_images(dataset_path: Path, images_root: Path) -> list[tuple[Path, str]]:
    rendered_images: list[tuple[Path, str]] = []
    with adios2.Stream(str(dataset_path), "r") as stream:
        for _ in stream.steps():
            step = stream.current_step()
            for field_name, cmap in (("field_a", "viridis"), ("field_b", "magma")):
                image_dir = images_root / field_name / "heatmap"
                image_dir.mkdir(parents=True, exist_ok=True)
                image_path = image_dir / f"image.{step:06d}.png"
                data = np.squeeze(stream.read(field_name))
                plt.imsave(image_path, data, cmap=cmap, origin="lower")
                print(f"rendered image: {image_path.resolve()}")
                rendered_images.append((image_path, field_name))
    return rendered_images


def add_field_visualization_sequences(manager: Manager, rendered_images: list[tuple[Path, str]]) -> None:
    for field_name in ("field_a", "field_b"):
        logical_names = [
            f"synthetic.bp/{field_name}/images/heatmap/{image_path.name}"
            for image_path, image_field_name in rendered_images
            if image_field_name == field_name
        ]
        manager.visualization_sequence(
            name=f"synthetic.bp/{field_name}/images/heatmap",
            vis_type="heatmap",
            variables=[
                {
                    "name": field_name,
                    "role": "primary",
                    "source_dataset": "synthetic.bp",
                }
            ],
            items=[{"type": "IMAGE", "name": logical_name} for logical_name in logical_names],
            thumbnail_name=logical_names[0],
        )


def find_sequence(visualization_sequences, name: str):
    return next(sequence for sequence in visualization_sequences.values() if sequence.name == name)


def test_adios_images(tmp_path: Path):
    archive_name = "adios_images.aca"
    dataset_path = tmp_path / "synthetic.bp"
    images_root = tmp_path / "rendered"
    archive_path = tmp_path / archive_name

    # Build a small time-varying ADIOS dataset with two scalar fields.
    write_adios_dataset(dataset_path, nsteps=3, nx=64, ny=64)

    # Render one PNG per field per timestep using Python-only tooling so the
    # test remains self-contained and CI-friendly.
    rendered_images = render_images(dataset_path, images_root)
    assert len(rendered_images) == 6

    print(f"campaign file: {archive_path.resolve()}")
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(dataset_path), name="synthetic.bp")

    for image_path, field_name in rendered_images:
        logical_name = f"synthetic.bp/{field_name}/images/heatmap/{image_path.name}"
        manager.image(str(image_path), name=logical_name)

    add_field_visualization_sequences(manager, rendered_images)

    info_data = manager.info()
    datasets = list(info_data.datasets.values())
    adios_datasets = [dataset for dataset in datasets if dataset.file_format == "ADIOS"]
    image_datasets = [dataset for dataset in datasets if dataset.file_format == "IMAGE"]
    visualization_sequences = info_data.visualization_sequences

    assert len(adios_datasets) == 1
    assert adios_datasets[0].name == "synthetic.bp"
    assert len(image_datasets) == 6
    assert len(visualization_sequences) == 2

    expected_image_names = {f"synthetic.bp/field_a/images/heatmap/image.{step:06d}.png" for step in range(3)} | {
        f"synthetic.bp/field_b/images/heatmap/image.{step:06d}.png" for step in range(3)
    }
    assert {dataset.name for dataset in image_datasets} == expected_image_names

    field_a_sequence = find_sequence(visualization_sequences, "synthetic.bp/field_a/images/heatmap")
    assert field_a_sequence.vis_type == "heatmap"
    assert field_a_sequence.thumbnail_dataset_name == "synthetic.bp/field_a/images/heatmap/image.000000.png"
    assert len(field_a_sequence.items) == 3
    assert field_a_sequence.variables[0].name == "field_a"
    assert field_a_sequence.variables[0].source_dataset_name == "synthetic.bp"

    field_b_sequence = find_sequence(visualization_sequences, "synthetic.bp/field_b/images/heatmap")
    assert field_b_sequence.vis_type == "heatmap"
    assert field_b_sequence.thumbnail_dataset_name == "synthetic.bp/field_b/images/heatmap/image.000000.png"
    assert len(field_b_sequence.items) == 3
    assert field_b_sequence.variables[0].name == "field_b"
    assert field_b_sequence.variables[0].source_dataset_name == "synthetic.bp"

    manager.close()


def test_adios_image_bytes(tmp_path: Path):
    archive_name = "adios_image_bytes.aca"
    dataset_path = tmp_path / "synthetic.bp"
    images_root = tmp_path / "rendered"
    archive_path = tmp_path / archive_name

    # Reuse the same synthetic ADIOS dataset and rendered PNGs, but ingest the
    # images from in-memory bytes rather than from filesystem paths.
    write_adios_dataset(dataset_path, nsteps=3, nx=64, ny=64)
    rendered_images = render_images(dataset_path, images_root)
    assert len(rendered_images) == 6

    print(f"campaign file: {archive_path.resolve()}")
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.data(str(dataset_path), name="synthetic.bp")

    for image_path, field_name in rendered_images:
        logical_name = f"synthetic.bp/{field_name}/images/heatmap/{image_path.name}"
        manager.image_data(image_path.read_bytes(), image_format="PNG", name=logical_name)

    add_field_visualization_sequences(manager, rendered_images)

    info_data = manager.info(list_replicas=True)
    datasets = list(info_data.datasets.values())
    adios_datasets = [dataset for dataset in datasets if dataset.file_format == "ADIOS"]
    image_datasets = [dataset for dataset in datasets if dataset.file_format == "IMAGE"]
    visualization_sequences = info_data.visualization_sequences

    assert len(adios_datasets) == 1
    assert adios_datasets[0].name == "synthetic.bp"
    assert len(image_datasets) == 6
    assert len(visualization_sequences) == 2

    expected_image_names = {f"synthetic.bp/field_a/images/heatmap/image.{step:06d}.png" for step in range(3)} | {
        f"synthetic.bp/field_b/images/heatmap/image.{step:06d}.png" for step in range(3)
    }
    assert {dataset.name for dataset in image_datasets} == expected_image_names
    assert all(any(replica.flags.embedded for replica in dataset.replicas.values()) for dataset in image_datasets)

    field_a_sequence = find_sequence(visualization_sequences, "synthetic.bp/field_a/images/heatmap")
    assert field_a_sequence.vis_type == "heatmap"
    assert field_a_sequence.thumbnail_dataset_name == "synthetic.bp/field_a/images/heatmap/image.000000.png"
    assert len(field_a_sequence.items) == 3
    assert field_a_sequence.variables[0].name == "field_a"
    assert field_a_sequence.variables[0].source_dataset_name == "synthetic.bp"

    field_b_sequence = find_sequence(visualization_sequences, "synthetic.bp/field_b/images/heatmap")
    assert field_b_sequence.vis_type == "heatmap"
    assert field_b_sequence.thumbnail_dataset_name == "synthetic.bp/field_b/images/heatmap/image.000000.png"
    assert len(field_b_sequence.items) == 3
    assert field_b_sequence.variables[0].name == "field_b"
    assert field_b_sequence.variables[0].source_dataset_name == "synthetic.bp"

    manager.close()
